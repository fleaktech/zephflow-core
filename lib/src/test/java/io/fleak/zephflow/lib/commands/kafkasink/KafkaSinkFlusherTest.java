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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
class KafkaSinkFlusherTest {

  @Test
  void testFlush_withRecords() throws Exception {
    KafkaProducer<byte[], byte[]> mockProducer = mock();
    String topic = "test-topic";
    PathExpression keyExpression = PathExpression.fromString("$.user_id");

    FleakSerializer<?> serializer =
        SerializerFactory.createSerializerFactory(EncodingType.JSON_OBJECT).createSerializer();

    KafkaSinkFlusher flusher = new KafkaSinkFlusher(mockProducer, topic, serializer, keyExpression);

    RecordFleakData testEvent =
        (RecordFleakData)
            FleakData.wrap(
                Map.of(
                    "user_id", "user123",
                    "message", "test message"));

    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedEvents =
        new SimpleSinkCommand.PreparedInputEvents<>();
    preparedEvents.add(testEvent, testEvent);

    Future<RecordMetadata> mockFuture = mock(Future.class);
    when(mockProducer.send(any(ProducerRecord.class))).thenReturn(mockFuture);

    ArgumentCaptor<ProducerRecord<byte[], byte[]>> recordCaptor =
        ArgumentCaptor.forClass(ProducerRecord.class);

    SimpleSinkCommand.FlushResult result = flusher.flush(preparedEvents);

    verify(mockProducer).send(recordCaptor.capture());
    ProducerRecord<byte[], byte[]> capturedRecord = recordCaptor.getValue();

    assertEquals("test-topic", capturedRecord.topic());
    assertEquals("user123", new String(capturedRecord.key()));
    assertNotNull(capturedRecord.value());

    assertEquals(1, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
  }

  @Test
  void testFlush_withFailure() throws Exception {
    KafkaProducer<byte[], byte[]> mockProducer = mock();
    String topic = "test-topic";

    FleakSerializer<?> serializer =
        SerializerFactory.createSerializerFactory(EncodingType.JSON_OBJECT).createSerializer();

    KafkaSinkFlusher flusher = new KafkaSinkFlusher(mockProducer, topic, serializer, null);

    RecordFleakData testEvent = (RecordFleakData) FleakData.wrap(Map.of("message", "test message"));

    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedEvents =
        new SimpleSinkCommand.PreparedInputEvents<>();
    preparedEvents.add(testEvent, testEvent);

    RuntimeException testException = new RuntimeException("Send failed");
    when(mockProducer.send(any(ProducerRecord.class))).thenThrow(testException);

    SimpleSinkCommand.FlushResult result = flusher.flush(preparedEvents);

    List<ErrorOutput> errorOutputs = result.errorOutputList();

    verify(mockProducer).send(any(ProducerRecord.class));

    assertEquals(0, result.successCount());
    assertEquals(1, errorOutputs.size());
    assertEquals("Send failed", errorOutputs.getFirst().errorMessage());
    assertEquals(testEvent, errorOutputs.getFirst().inputEvent());
  }
}
