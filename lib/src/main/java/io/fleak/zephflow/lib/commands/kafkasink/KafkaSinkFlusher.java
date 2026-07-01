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
package io.fleak.zephflow.lib.commands.kafkasink;

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;

import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.ConnectionFailureClassifier;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;

/**
 * Kafka sink flusher. By default it is fire-and-forget (relies on Kafka's native batching for
 * throughput and reports delivery via async callbacks). When constructed in <b>synchronous</b> mode
 * (used by store-and-forward), it instead waits for acks and <b>throws</b> a connectivity failure
 * when delivery fails, so {@link SimpleSinkCommand} can buffer the batch locally and replay it once
 * the broker is reachable again.
 */
@Slf4j
public class KafkaSinkFlusher implements SimpleSinkCommand.Flusher<RecordFleakData> {

  private final KafkaProducer<byte[], byte[]> producer;
  private final String topic;
  private final FleakSerializer<?> fleakSerializer;
  private final PathExpression partitionKeyExpression;
  private final FleakCounter asyncDeliveredCountCounter;
  private final FleakCounter asyncDeliveredSizeCounter;
  private final FleakCounter asyncErrorCounter;

  // Synchronous delivery: when enabled the flusher waits for acks and throws connectivity failures
  // (classified via this classifier) so store-and-forward can buffer + replay. Null in async mode.
  private final boolean synchronousDelivery;
  private final ConnectionFailureClassifier connectionFailureClassifier;

  private volatile boolean closed = false;

  public KafkaSinkFlusher(
      @NonNull KafkaProducer<byte[], byte[]> producer,
      @NonNull String topic,
      @NonNull FleakSerializer<?> fleakSerializer,
      PathExpression partitionKeyExpression, // This one can stay nullable
      @NonNull FleakCounter asyncDeliveredCountCounter,
      @NonNull FleakCounter asyncDeliveredSizeCounter,
      @NonNull FleakCounter asyncErrorCounter) {
    this(
        producer,
        topic,
        fleakSerializer,
        partitionKeyExpression,
        asyncDeliveredCountCounter,
        asyncDeliveredSizeCounter,
        asyncErrorCounter,
        false,
        null);
  }

  public KafkaSinkFlusher(
      @NonNull KafkaProducer<byte[], byte[]> producer,
      @NonNull String topic,
      @NonNull FleakSerializer<?> fleakSerializer,
      PathExpression partitionKeyExpression,
      @NonNull FleakCounter asyncDeliveredCountCounter,
      @NonNull FleakCounter asyncDeliveredSizeCounter,
      @NonNull FleakCounter asyncErrorCounter,
      boolean synchronousDelivery,
      ConnectionFailureClassifier connectionFailureClassifier) {
    this.producer = producer;
    this.topic = topic;
    this.fleakSerializer = fleakSerializer;
    this.partitionKeyExpression = partitionKeyExpression;
    this.asyncDeliveredCountCounter = asyncDeliveredCountCounter;
    this.asyncDeliveredSizeCounter = asyncDeliveredSizeCounter;
    this.asyncErrorCounter = asyncErrorCounter;
    this.synchronousDelivery = synchronousDelivery;
    this.connectionFailureClassifier = connectionFailureClassifier;
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedInputEvents,
      Map<String, String> metricTags)
      throws Exception {

    if (closed) {
      throw new IllegalStateException("KafkaSinkFlusher is closed");
    }

    List<RecordFleakData> events = preparedInputEvents.preparedList();
    if (events.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    return synchronousDelivery
        ? flushSynchronously(events, metricTags)
        : flushFireAndForget(events, metricTags);
  }

  private SimpleSinkCommand.FlushResult flushFireAndForget(
      List<RecordFleakData> events, Map<String, String> metricTags) {
    List<ErrorOutput> errorOutputs = new ArrayList<>();
    int sentCount = 0;

    for (RecordFleakData event : events) {
      try {
        byte[] eventValue = serializeValue(event);
        if (eventValue == null) {
          continue;
        }
        byte[] keyBytesValue = keyBytes(event);
        producer.send(
            new ProducerRecord<>(topic, keyBytesValue, eventValue),
            (metadata, exception) -> {
              if (exception != null) {
                log.error(
                    "Kafka producer failed to send event: {}", toJsonString(event), exception);
                asyncErrorCounter.increase(metricTags);
                return;
              }
              asyncDeliveredCountCounter.increase(metricTags);
              asyncDeliveredSizeCounter.increase(eventValue.length, metricTags);
            });
        sentCount++;
      } catch (Exception e) {
        log.error("Failed to send Kafka record for event: {}", toJsonString(event), e);
        errorOutputs.add(new ErrorOutput(event, e.getMessage()));
      }
    }
    return new SimpleSinkCommand.FlushResult(sentCount, 0, errorOutputs);
  }

  /**
   * Sends the batch and waits for acks. A connectivity failure is rethrown so the whole batch is
   * buffered by store-and-forward; a permanent per-record failure stays an {@link ErrorOutput}.
   */
  private SimpleSinkCommand.FlushResult flushSynchronously(
      List<RecordFleakData> events, Map<String, String> metricTags) throws Exception {
    // One bounded metadata probe up front. If the broker is unreachable it throws a connectivity
    // failure after a single wait, buffering the whole batch at once. Without it, on a cold start
    // each send() below would separately block max.block.ms waiting for metadata (N x the cost).
    ensureBrokerReachable();

    List<ErrorOutput> errorOutputs = new ArrayList<>();
    List<Inflight> inflight = new ArrayList<>();

    for (RecordFleakData event : events) {
      try {
        byte[] eventValue = serializeValue(event);
        if (eventValue == null) {
          continue;
        }
        Future<RecordMetadata> future =
            producer.send(new ProducerRecord<>(topic, keyBytes(event), eventValue));
        inflight.add(new Inflight(event, eventValue.length, future));
      } catch (Exception e) {
        rethrowIfConnectivity(e); // never reached the broker
        errorOutputs.add(new ErrorOutput(event, e.getMessage()));
      }
    }

    producer.flush(); // ensure every send has been attempted before we wait on the acks

    long deliveredSize = 0;
    int delivered = 0;
    for (Inflight item : inflight) {
      try {
        item.future.get();
        delivered++;
        deliveredSize += item.size;
        asyncDeliveredCountCounter.increase(metricTags);
        asyncDeliveredSizeCounter.increase(item.size, metricTags);
      } catch (ExecutionException e) {
        rethrowIfConnectivity(e.getCause());
        errorOutputs.add(new ErrorOutput(item.event, e.getCause().getMessage()));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw e;
      }
    }
    return new SimpleSinkCommand.FlushResult(delivered, deliveredSize, errorOutputs);
  }

  /**
   * Single bounded check that the broker is reachable (metadata for the topic is obtainable). If it
   * is not, throw the connectivity failure so the caller buffers the whole batch after one wait
   * rather than paying {@code max.block.ms} on every per-record {@code send()}.
   */
  private void ensureBrokerReachable() {
    try {
      List<PartitionInfo> partitions = producer.partitionsFor(topic);
      if (partitions == null || partitions.isEmpty()) {
        throw new org.apache.kafka.common.errors.TimeoutException(
            "No partition metadata available for topic " + topic);
      }
    } catch (Exception e) {
      rethrowIfConnectivity(e);
      // Not a connectivity failure: let the per-record send loop surface it as needed.
    }
  }

  private void rethrowIfConnectivity(Throwable t) {
    if (connectionFailureClassifier != null && connectionFailureClassifier.isConnectionFailure(t)) {
      if (t instanceof RuntimeException re) {
        throw re;
      }
      throw new RuntimeException(t);
    }
  }

  private byte[] serializeValue(RecordFleakData event) throws Exception {
    var serializedEvent = fleakSerializer.serialize(List.of(event));
    byte[] eventValue = serializedEvent.value();
    return (eventValue == null || eventValue.length == 0) ? null : eventValue;
  }

  private byte[] keyBytes(RecordFleakData event) {
    if (partitionKeyExpression == null) {
      return null;
    }
    String keyValue = partitionKeyExpression.getStringValueFromEventOrDefault(event, null);
    return keyValue == null ? null : keyValue.getBytes(StandardCharsets.UTF_8);
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    // Bound shutdown: a wedged producer (e.g. broker unreachable) must not hang terminate()
    // indefinitely while it retries to flush undelivered records.
    producer.close(Duration.ofSeconds(10));
    log.info("KafkaSinkFlusher closed successfully");
  }

  private record Inflight(RecordFleakData event, int size, Future<RecordMetadata> future) {}
}
