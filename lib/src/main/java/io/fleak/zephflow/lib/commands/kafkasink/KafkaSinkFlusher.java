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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

@Slf4j
public class KafkaSinkFlusher implements SimpleSinkCommand.Flusher<RecordFleakData> {
  private final KafkaProducer<byte[], byte[]> producer;
  private final String topic;
  private final FleakSerializer<?> fleakSerializer;
  private final PathExpression partitionKeyExpression;

  public KafkaSinkFlusher(
      KafkaProducer<byte[], byte[]> producer,
      String topic,
      FleakSerializer<?> fleakSerializer,
      PathExpression partitionKeyExpression) {
    this.producer = producer;
    this.topic = topic;
    this.fleakSerializer = fleakSerializer;
    this.partitionKeyExpression = partitionKeyExpression;
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedInputEvents) throws Exception {

    // A map to correlate the future with the original event for better error handling.
    Map<Future<?>, RecordFleakData> futureEventMap = new LinkedHashMap<>();
    List<ErrorOutput> errorOutputs = new ArrayList<>();
    long flushedDataSize = 0;

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
          Future<?> future = producer.send(new ProducerRecord<>(topic, keyBytesValue, eventValue));
          futureEventMap.put(future, event); // Associate future with the event
          flushedDataSize += eventValue.length;
        }
      } catch (Exception e) {
        // This catches serialization errors or other immediate issues.
        log.error("Failed to send Kafka record for event: {}", toJsonString(event), e);
        errorOutputs.add(new ErrorOutput(event, e.getMessage()));
      }
    }

    // Now, check the result of each send operation.
    for (Map.Entry<Future<?>, RecordFleakData> entry : futureEventMap.entrySet()) {
      Future<?> future = entry.getKey();
      RecordFleakData event = entry.getValue();
      try {
        // Wait for send to complete.
        future.get(10, TimeUnit.SECONDS);
        log.debug("Sent event to Kafka: {}", toJsonString(event));
      } catch (Exception e) {
        // This will catch exceptions from the Kafka producer, including RecordTooLargeException.
        log.error("Kafka producer failed to send event: {}", toJsonString(event), e);
        errorOutputs.add(new ErrorOutput(event, e.getMessage()));
      }
    }

    return new SimpleSinkCommand.FlushResult(
        preparedList.size() - errorOutputs.size(), flushedDataSize, errorOutputs);
  }

  @Override
  public void close() {
    if (producer != null) {
      producer.close();
    }
  }
}
