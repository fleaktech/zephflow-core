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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/** Test class for KafkaSinkFlusher (fire-and-forget model) */
class KafkaSinkFlusherTest {

  @Mock private KafkaProducer<byte[], byte[]> mockProducer;
  @Mock private FleakSerializer<Object> mockSerializer;
  @Mock private FleakCounter mockAsyncSuccessCounter;
  @Mock private FleakCounter mockAsyncOutputSizeCounter;
  @Mock private FleakCounter mockAsyncErrorCounter;

  private final String topic = "test-topic";
  private static final byte[] TEST_DATA = "test-data".getBytes();

  private KafkaSinkFlusher flusher;
  private List<RecordFleakData> testEvents;

  @BeforeEach
  void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);

    testEvents = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      testEvents.add((RecordFleakData) FleakData.wrap(Map.of("id", i, "message", "test-" + i)));
    }

    when(mockSerializer.serialize(anyList()))
        .thenReturn(new SerializedEvent(null, TEST_DATA, Map.of()));

    doAnswer(
            invocation -> {
              Callback callback = invocation.getArgument(1);
              callback.onCompletion(mock(RecordMetadata.class), null);
              return null;
            })
        .when(mockProducer)
        .send(any(ProducerRecord.class), any(Callback.class));

    flusher =
        new KafkaSinkFlusher(
            mockProducer,
            topic,
            mockSerializer,
            null,
            mockAsyncSuccessCounter,
            mockAsyncOutputSizeCounter,
            mockAsyncErrorCounter);
  }

  private static final Map<String, String> TEST_METRIC_TAGS = Map.of("callingUser", "testUser");

  @Test
  void testFlush_SendsAllRecords() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedEvents =
        createPreparedEvents(testEvents.subList(0, 3));

    SimpleSinkCommand.FlushResult result = flusher.flush(preparedEvents, TEST_METRIC_TAGS);

    // successCount=0 because actual success is tracked async in callback
    assertEquals(0, result.successCount());
    assertEquals(0, result.errorOutputList().size());
    verify(mockProducer, times(3)).send(any(ProducerRecord.class), any(Callback.class));
    // Async counters should be incremented for each successful callback
    verify(mockAsyncSuccessCounter, times(3)).increase(TEST_METRIC_TAGS);
    verify(mockAsyncOutputSizeCounter, times(3)).increase(TEST_DATA.length, TEST_METRIC_TAGS);
    verify(mockAsyncErrorCounter, never()).increase(any());
  }

  @Test
  void testErrorHandling_SerializationFailure() throws Exception {
    FleakSerializer<Object> failingSerializer = mock(FleakSerializer.class);
    when(failingSerializer.serialize(anyList()))
        .thenThrow(new RuntimeException("Serialization error"));

    KafkaSinkFlusher failingFlusher =
        new KafkaSinkFlusher(
            mockProducer,
            topic,
            failingSerializer,
            null,
            mockAsyncSuccessCounter,
            mockAsyncOutputSizeCounter,
            mockAsyncErrorCounter);

    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedEvents =
        createPreparedEvents(testEvents.subList(0, 3));

    SimpleSinkCommand.FlushResult result = failingFlusher.flush(preparedEvents, TEST_METRIC_TAGS);

    assertNotNull(result);
    assertEquals(0, result.successCount());
    assertEquals(3, result.errorOutputList().size());
    verify(mockProducer, never()).send(any(ProducerRecord.class), any(Callback.class));

    failingFlusher.close();
  }

  @Test
  void testErrorHandling_AsyncProducerFailure() throws Exception {
    doAnswer(
            invocation -> {
              Callback callback = invocation.getArgument(1);
              callback.onCompletion(null, new RuntimeException("Producer send failed"));
              return null;
            })
        .when(mockProducer)
        .send(any(ProducerRecord.class), any(Callback.class));

    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedEvents =
        createPreparedEvents(testEvents.subList(0, 3));

    SimpleSinkCommand.FlushResult result = flusher.flush(preparedEvents, TEST_METRIC_TAGS);

    // Fire-and-forget: successCount=0, actual outcomes tracked in callbacks
    assertNotNull(result);
    assertEquals(0, result.successCount());
    assertEquals(0, result.errorOutputList().size());
    verify(mockProducer, times(3)).send(any(ProducerRecord.class), any(Callback.class));
    // Async error counter should be incremented for each failed callback with proper tags
    verify(mockAsyncErrorCounter, times(3)).increase(TEST_METRIC_TAGS);
    verify(mockAsyncSuccessCounter, never()).increase(any());
  }

  @Test
  void testClosedFlusher_ThrowsException() throws Exception {
    flusher.close();

    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedEvents =
        createPreparedEvents(testEvents.subList(0, 1));

    assertThrows(
        IllegalStateException.class, () -> flusher.flush(preparedEvents, TEST_METRIC_TAGS));
  }

  @Test
  void testPartitionKeyExpression() throws Exception {
    PathExpression keyExpression = PathExpression.fromString("$.message");
    KafkaSinkFlusher flusherWithKey =
        new KafkaSinkFlusher(
            mockProducer,
            topic,
            mockSerializer,
            keyExpression,
            mockAsyncSuccessCounter,
            mockAsyncOutputSizeCounter,
            mockAsyncErrorCounter);

    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedEvents =
        createPreparedEvents(testEvents.subList(0, 3));

    SimpleSinkCommand.FlushResult result = flusherWithKey.flush(preparedEvents, TEST_METRIC_TAGS);

    assertNotNull(result);
    assertEquals(0, result.successCount());
    verify(mockProducer, times(3)).send(any(ProducerRecord.class), any(Callback.class));
    verify(mockAsyncSuccessCounter, times(3)).increase(TEST_METRIC_TAGS);

    flusherWithKey.close();
  }

  @Test
  void testEmptyFlush_HandledGracefully() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> emptyEvents =
        new SimpleSinkCommand.PreparedInputEvents<>();

    SimpleSinkCommand.FlushResult result = flusher.flush(emptyEvents, TEST_METRIC_TAGS);

    assertEquals(0, result.successCount());
    assertEquals(0, result.errorOutputList().size());
    verify(mockProducer, never()).send(any(ProducerRecord.class), any(Callback.class));
  }

  @Test
  void testProducerSuccess_CallbackHandling() throws Exception {
    RecordMetadata mockMetadata = mock(RecordMetadata.class);
    when(mockMetadata.topic()).thenReturn(topic);
    when(mockMetadata.partition()).thenReturn(0);
    when(mockMetadata.offset()).thenReturn(123L);
    doAnswer(
            invocation -> {
              Callback callback = invocation.getArgument(1);
              callback.onCompletion(mockMetadata, null);
              return null;
            })
        .when(mockProducer)
        .send(any(ProducerRecord.class), any(Callback.class));

    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedEvents =
        createPreparedEvents(testEvents.subList(0, 3));

    SimpleSinkCommand.FlushResult result = flusher.flush(preparedEvents, TEST_METRIC_TAGS);

    assertEquals(0, result.successCount());
    assertEquals(0, result.errorOutputList().size());
    verify(mockProducer, times(3)).send(any(ProducerRecord.class), any(Callback.class));
    verify(mockAsyncSuccessCounter, times(3)).increase(TEST_METRIC_TAGS);
  }

  @Test
  void testResourceManagement_ProperCleanup() {
    flusher.close();

    verify(mockProducer, times(1)).close();

    // Verify close() is idempotent
    assertDoesNotThrow(
        () -> {
          flusher.close();
          flusher.close();
        });
  }

  @Test
  void testNullValueSerialization() throws Exception {
    FleakSerializer<Object> nullValueSerializer = mock(FleakSerializer.class);
    when(nullValueSerializer.serialize(anyList()))
        .thenReturn(new SerializedEvent(null, null, Map.of()));

    KafkaSinkFlusher nullValueFlusher =
        new KafkaSinkFlusher(
            mockProducer,
            topic,
            nullValueSerializer,
            null,
            mockAsyncSuccessCounter,
            mockAsyncOutputSizeCounter,
            mockAsyncErrorCounter);

    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedEvents =
        createPreparedEvents(testEvents.subList(0, 3));

    SimpleSinkCommand.FlushResult result = nullValueFlusher.flush(preparedEvents, TEST_METRIC_TAGS);

    assertNotNull(result);
    assertEquals(0, result.successCount());
    verify(mockProducer, never()).send(any(ProducerRecord.class), any(Callback.class));

    nullValueFlusher.close();
  }

  @Test
  void testPartitionKey_NullHandling() throws Exception {
    PathExpression nullKeyExpression = mock(PathExpression.class);
    when(nullKeyExpression.getStringValueFromEventOrDefault(any(), any())).thenReturn(null);

    KafkaSinkFlusher nullKeyFlusher =
        new KafkaSinkFlusher(
            mockProducer,
            topic,
            mockSerializer,
            nullKeyExpression,
            mockAsyncSuccessCounter,
            mockAsyncOutputSizeCounter,
            mockAsyncErrorCounter);

    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedEvents =
        createPreparedEvents(testEvents.subList(0, 3));

    SimpleSinkCommand.FlushResult result = nullKeyFlusher.flush(preparedEvents, TEST_METRIC_TAGS);

    assertNotNull(result);
    assertEquals(0, result.successCount());
    verify(mockProducer, times(3)).send(any(ProducerRecord.class), any(Callback.class));
    verify(mockAsyncSuccessCounter, times(3)).increase(TEST_METRIC_TAGS);

    nullKeyFlusher.close();
  }

  @Test
  void testPartitionKey_ExpressionError() throws Exception {
    PathExpression errorKeyExpression = mock(PathExpression.class);
    when(errorKeyExpression.getStringValueFromEventOrDefault(any(), any()))
        .thenThrow(new RuntimeException("Path evaluation failed"));

    KafkaSinkFlusher errorKeyFlusher =
        new KafkaSinkFlusher(
            mockProducer,
            topic,
            mockSerializer,
            errorKeyExpression,
            mockAsyncSuccessCounter,
            mockAsyncOutputSizeCounter,
            mockAsyncErrorCounter);

    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedEvents =
        createPreparedEvents(testEvents.subList(0, 3));

    SimpleSinkCommand.FlushResult result = errorKeyFlusher.flush(preparedEvents, TEST_METRIC_TAGS);

    assertNotNull(result);
    assertEquals(0, result.successCount());
    assertEquals(3, result.errorOutputList().size());

    errorKeyFlusher.close();
  }

  private SimpleSinkCommand.PreparedInputEvents<RecordFleakData> createPreparedEvents(
      List<RecordFleakData> events) {
    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedEvents =
        new SimpleSinkCommand.PreparedInputEvents<>();
    events.forEach(event -> preparedEvents.add(event, event));
    return preparedEvents;
  }
}
