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
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Fire-and-forget Kafka sink flusher that sends records directly to Kafka producer. Relies on
 * Kafka's native batching (batch.size, linger.ms) for throughput optimization.
 */
@Slf4j
public class KafkaSinkFlusher implements SimpleSinkCommand.Flusher<RecordFleakData> {

  private final KafkaProducer<byte[], byte[]> producer;
  private final String topic;
  private final FleakSerializer<?> fleakSerializer;
  private final PathExpression partitionKeyExpression;
  private final FleakCounter asyncErrorCounter;

  private volatile boolean closed = false;

  public KafkaSinkFlusher(
      KafkaProducer<byte[], byte[]> producer,
      String topic,
      FleakSerializer<?> fleakSerializer,
      PathExpression partitionKeyExpression,
      FleakCounter asyncErrorCounter) {
    this.producer = producer;
    this.topic = topic;
    this.fleakSerializer = fleakSerializer;
    this.partitionKeyExpression = partitionKeyExpression;
    this.asyncErrorCounter = asyncErrorCounter;
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

    List<ErrorOutput> errorOutputs = new ArrayList<>();
    long flushedDataSize = 0;
    int successCount = 0;

    for (RecordFleakData event : events) {
      try {
        var serializedEvent = fleakSerializer.serialize(List.of(event));
        byte[] keyBytesValue = null;

        if (partitionKeyExpression != null) {
          String keyValue = partitionKeyExpression.getStringValueFromEventOrDefault(event, null);
          if (keyValue != null) {
            keyBytesValue = keyValue.getBytes(StandardCharsets.UTF_8);
          }
        }

        var eventValue = serializedEvent.value();

        if (eventValue != null && eventValue.length > 0) {
          producer.send(
              new ProducerRecord<>(topic, keyBytesValue, eventValue),
              (metadata, exception) -> {
                if (exception != null) {
                  log.error(
                      "Kafka producer failed to send event: {}", toJsonString(event), exception);
                  if (asyncErrorCounter != null) {
                    asyncErrorCounter.increase(metricTags);
                  }
                } else {
                  log.debug(
                      "Sent event to Kafka: topic={}, partition={}, offset={}",
                      metadata.topic(),
                      metadata.partition(),
                      metadata.offset());
                }
              });
          flushedDataSize += eventValue.length;
          successCount++;
        }
      } catch (Exception e) {
        log.error("Failed to send Kafka record for event: {}", toJsonString(event), e);
        errorOutputs.add(new ErrorOutput(event, e.getMessage()));
      }
    }

    log.debug(
        "Flush completed: {} records sent, {} sync errors, {} bytes",
        successCount,
        errorOutputs.size(),
        flushedDataSize);

    return new SimpleSinkCommand.FlushResult(successCount, flushedDataSize, errorOutputs);
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }

    closed = true;

    if (producer != null) {
      producer.close();
    }

    log.info("KafkaSinkFlusher closed successfully");
  }
}
