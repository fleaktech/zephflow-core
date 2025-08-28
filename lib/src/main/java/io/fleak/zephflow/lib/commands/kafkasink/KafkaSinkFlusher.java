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
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public record KafkaSinkFlusher(
    KafkaProducer<byte[], byte[]> producer,
    String topic,
    FleakSerializer<?> fleakSerializer,
    PathExpression partitionKeyExpression)
    implements SimpleSinkCommand.Flusher<RecordFleakData> {

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedInputEvents) throws Exception {

    List<ErrorOutput> errorOutputs = new ArrayList<>();
    long flushedDataSize = 0;
    int totalSends = 0;

    List<RecordFleakData> preparedList = preparedInputEvents.preparedList();
    for (RecordFleakData event : preparedList) {
      try {
        var serializedEvent = fleakSerializer.serialize(List.of(event));
        byte[] keyBytesValue = null;

        if (partitionKeyExpression != null) {
          keyBytesValue =
              partitionKeyExpression
                  .getStringValueFromEventOrDefault(event, null)
                  .getBytes(StandardCharsets.UTF_8);
        }

        var eventValue = serializedEvent.value();

        if (eventValue != null && eventValue.length > 0) {
          // Send to Kafka with callback for async error handling
          producer.send(
              new ProducerRecord<>(topic, keyBytesValue, eventValue),
              (metadata, exception) -> {
                if (exception != null) {
                  log.error(
                      "Kafka producer failed to send event: {}", toJsonString(event), exception);
                  synchronized (errorOutputs) {
                    errorOutputs.add(new ErrorOutput(event, exception.getMessage()));
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
          totalSends++;
        }
      } catch (Exception e) {
        // This catches serialization errors or other immediate issues.
        log.error("Failed to send Kafka record for event: {}", toJsonString(event), e);
        synchronized (errorOutputs) {
          errorOutputs.add(new ErrorOutput(event, e.getMessage()));
        }
      }
    }

    // Flush to ensure all async sends complete before returning
    if (totalSends > 0) {
      producer.flush();
    }

    int successfulSends = totalSends - errorOutputs.size();
    return new SimpleSinkCommand.FlushResult(successfulSends, flushedDataSize, errorOutputs);
  }

  @Override
  public void close() {
    if (producer != null) {
      producer.close();
    }
  }
}
