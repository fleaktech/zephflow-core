/**
 * Copyright 2025 Fleak Tech Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fleak.zephflow.lib.commands.azureeventhubsink;

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;
import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Publishes records to an Azure Event Hub using the native producer client.
 *
 * <p>Each record is serialized to bytes and wrapped in an {@link EventData}. Events are packed into
 * {@link EventDataBatch}es honoring the service's 1&nbsp;MB batch limit: when {@link
 * EventDataBatch#tryAdd} reports the batch is full, the current batch is sent and a fresh one is
 * started. When a partition-key expression is configured, events are grouped by resolved key and
 * each key is sent in its own batch (a batch carries a single partition key).
 *
 * <p>A record that fails to serialize, or is larger than a whole empty batch, is reported as an
 * {@link ErrorOutput} rather than aborting the flush. A failure from {@link
 * EventHubProducerClient#send} propagates so the caller treats the batch as a complete failure.
 */
@Slf4j
public class AzureEventHubSinkFlusher implements SimpleSinkCommand.Flusher<RecordFleakData> {

  private final EventHubProducerClient producerClient;
  private final FleakSerializer<?> fleakSerializer;
  private final PathExpression partitionKeyExpression; // nullable

  private volatile boolean closed = false;

  public AzureEventHubSinkFlusher(
      @NonNull EventHubProducerClient producerClient,
      @NonNull FleakSerializer<?> fleakSerializer,
      PathExpression partitionKeyExpression) {
    this.producerClient = producerClient;
    this.fleakSerializer = fleakSerializer;
    this.partitionKeyExpression = partitionKeyExpression;
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedInputEvents,
      Map<String, String> metricTags)
      throws Exception {
    if (closed) {
      throw new IllegalStateException("AzureEventHubSinkFlusher is closed");
    }

    List<RecordFleakData> events = preparedInputEvents.preparedList();
    if (events.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    List<ErrorOutput> errorOutputs = new ArrayList<>();

    // Serialize up front, grouping by resolved partition key (null key -> its own group). Grouping
    // preserves order and lets each key be sent as a single-partition-key batch.
    Map<String, List<PreparedEvent>> eventsByPartitionKey = new LinkedHashMap<>();
    for (RecordFleakData event : events) {
      try {
        byte[] value = serializeValue(event);
        if (value == null) {
          continue;
        }
        String partitionKey = resolvePartitionKey(event);
        eventsByPartitionKey
            .computeIfAbsent(partitionKey, k -> new ArrayList<>())
            .add(new PreparedEvent(event, new EventData(value), value.length));
      } catch (Exception e) {
        log.error("Failed to serialize event for Event Hub: {}", toJsonString(event), e);
        errorOutputs.add(new ErrorOutput(event, e.getMessage()));
      }
    }

    long deliveredSize = 0;
    int delivered = 0;
    for (Map.Entry<String, List<PreparedEvent>> group : eventsByPartitionKey.entrySet()) {
      BatchSendResult result = sendGroup(group.getKey(), group.getValue(), errorOutputs);
      delivered += result.count();
      deliveredSize += result.size();
    }

    return new SimpleSinkCommand.FlushResult(delivered, deliveredSize, errorOutputs);
  }

  /** Sends one partition-key group, rolling into a new batch whenever the current one fills up. */
  private BatchSendResult sendGroup(
      String partitionKey, List<PreparedEvent> groupEvents, List<ErrorOutput> errorOutputs) {
    int deliveredCount = 0;
    long deliveredSize = 0;

    EventDataBatch batch = newBatch(partitionKey);
    int batchCount = 0;
    long batchSize = 0;

    for (PreparedEvent prepared : groupEvents) {
      if (batch.tryAdd(prepared.eventData())) {
        batchCount++;
        batchSize += prepared.size();
        continue;
      }
      // tryAdd failed: either the batch is full, or the single event is too large for an empty one.
      if (batch.getCount() == 0) {
        errorOutputs.add(
            new ErrorOutput(
                prepared.raw(), "event exceeds the maximum Event Hub batch size and was dropped"));
        continue;
      }
      producerClient.send(batch);
      deliveredCount += batchCount;
      deliveredSize += batchSize;

      batch = newBatch(partitionKey);
      batchCount = 0;
      batchSize = 0;
      if (batch.tryAdd(prepared.eventData())) {
        batchCount++;
        batchSize += prepared.size();
      } else {
        errorOutputs.add(
            new ErrorOutput(
                prepared.raw(), "event exceeds the maximum Event Hub batch size and was dropped"));
      }
    }

    if (batch.getCount() > 0) {
      producerClient.send(batch);
      deliveredCount += batchCount;
      deliveredSize += batchSize;
    }
    return new BatchSendResult(deliveredCount, deliveredSize);
  }

  private EventDataBatch newBatch(String partitionKey) {
    if (partitionKey == null) {
      return producerClient.createBatch();
    }
    return producerClient.createBatch(new CreateBatchOptions().setPartitionKey(partitionKey));
  }

  private byte[] serializeValue(RecordFleakData event) throws Exception {
    var serialized = fleakSerializer.serialize(List.of(event));
    byte[] value = serialized.value();
    return (value == null || value.length == 0) ? null : value;
  }

  private String resolvePartitionKey(RecordFleakData event) {
    if (partitionKeyExpression == null) {
      return null;
    }
    return partitionKeyExpression.getStringValueFromEventOrDefault(event, null);
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    producerClient.close();
    log.info("AzureEventHubSinkFlusher closed successfully");
  }

  private record PreparedEvent(RecordFleakData raw, EventData eventData, int size) {}

  private record BatchSendResult(int count, long size) {}
}
