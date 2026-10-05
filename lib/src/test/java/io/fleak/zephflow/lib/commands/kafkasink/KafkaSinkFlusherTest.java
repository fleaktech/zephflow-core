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

class KafkaSinkFlusherTest {

  @Test
  void boundedLocalRejectionsAreDefiniteAndDoNotBecomeRemoteAttempts() throws Exception {
    when(mockSerializer.serialize(anyList()))
        .thenThrow(new IllegalArgumentException("invalid event"));
    var result =
        flusher.flushBounded(createPreparedEvents(testEvents), Map.of(), boundedHooks(() -> {}));
    assertEquals(5, result.errorOutputList().size());
    assertEquals(0L, result.effectOutcome().attemptedCount());
    assertEquals(5L, result.effectOutcome().definiteFailureCount());
    assertEquals(5L, result.effectOutcome().notAttemptedCount());
    assertEquals(0L, result.effectOutcome().unknownCount());
    assertEquals(
        io.fleak.zephflow.api.execution.EffectOutcome.Delivery.FAILED,
        result.effectOutcome().delivery());
    verify(mockProducer, never()).send(any(ProducerRecord.class));
  }

  @Test
  void boundedMixedLocalRejectionPreservesBrokerAcknowledgement() throws Exception {
    when(mockSerializer.serialize(anyList()))
        .thenReturn(new SerializedEvent(null, TEST_DATA, Map.of()))
        .thenReturn(new SerializedEvent(null, null, Map.of()));
    when(mockProducer.send(any(ProducerRecord.class)))
        .thenReturn(
            java.util.concurrent.CompletableFuture.completedFuture(mock(RecordMetadata.class)));
    var result =
        flusher.flushBounded(
            createPreparedEvents(testEvents.subList(0, 2)), Map.of(), boundedHooks(() -> {}));
    assertEquals(1, result.errorOutputList().size());
    assertEquals(1L, result.effectOutcome().attemptedCount());
    assertEquals(1L, result.effectOutcome().acknowledgedCount());
    assertEquals(1L, result.effectOutcome().definiteFailureCount());
    assertEquals(1L, result.effectOutcome().notAttemptedCount());
    assertEquals(0L, result.effectOutcome().unknownCount());
    assertEquals(
        io.fleak.zephflow.api.execution.EffectOutcome.Delivery.PARTIAL,
        result.effectOutcome().delivery());
  }

  @Test
  void boundedStopBeforeFirstSendIsNotAttempted() {
    var failure =
        assertThrows(
            io.fleak.zephflow.lib.commands.sink.BoundedFlushException.class,
            () ->
                flusher.flushBounded(
                    createPreparedEvents(testEvents),
                    Map.of(),
                    boundedHooks(
                        () -> {
                          throw new io.fleak.zephflow.api.execution.ExecutionStoppedException(
                              "stop");
                        })));
    assertEquals(0L, failure.result().effectOutcome().attemptedCount());
    assertEquals(0L, failure.result().effectOutcome().unknownCount());
    assertEquals(5L, failure.result().effectOutcome().notAttemptedCount());
    assertEquals(
        io.fleak.zephflow.api.execution.EffectOutcome.Delivery.NOT_ATTEMPTED,
        failure.result().effectOutcome().delivery());
    verify(mockProducer, never()).send(any(ProducerRecord.class));
  }

  @Test
  void boundedFireAndForgetConfigurationWaitsForBrokerAcknowledgements() throws Exception {
    when(mockProducer.send(any(ProducerRecord.class)))
        .thenReturn(
            java.util.concurrent.CompletableFuture.completedFuture(mock(RecordMetadata.class)))
        .thenReturn(
            java.util.concurrent.CompletableFuture.failedFuture(
                new java.io.IOException("acknowledgement lost")));
    var result =
        flusher.flushBounded(
            createPreparedEvents(testEvents.subList(0, 2)), Map.of(), boundedHooks(() -> {}));
    assertEquals(1, result.successCount());
    assertEquals(1L, result.effectOutcome().acknowledgedCount());
    assertEquals(1L, result.effectOutcome().unknownCount());
    verify(mockProducer, never()).send(any(ProducerRecord.class), any(Callback.class));
    flusher.close();
    verify(mockProducer).close();
  }

  @Test
  void boundedStopPreservesEarlierAcknowledgementAndDoesNotSendNextRecord() {
    java.util.concurrent.atomic.AtomicBoolean stopped =
        new java.util.concurrent.atomic.AtomicBoolean();
    when(mockProducer.send(any(ProducerRecord.class)))
        .thenAnswer(
            invocation -> {
              stopped.set(true);
              return java.util.concurrent.CompletableFuture.completedFuture(
                  mock(RecordMetadata.class));
            });
    var failure =
        assertThrows(
            io.fleak.zephflow.lib.commands.sink.BoundedFlushException.class,
            () ->
                flusher.flushBounded(
                    createPreparedEvents(testEvents.subList(0, 3)),
                    Map.of(),
                    boundedHooks(
                        () -> {
                          if (stopped.get())
                            throw new io.fleak.zephflow.api.execution.ExecutionStoppedException(
                                "stop");
                        })));
    assertEquals(1L, failure.result().effectOutcome().acknowledgedCount());
    assertEquals(2L, failure.result().effectOutcome().notAttemptedCount());
    assertEquals(0L, failure.result().effectOutcome().unknownCount());
    verify(mockProducer, times(1)).send(any(ProducerRecord.class));
  }

  private static io.fleak.zephflow.api.execution.ExecutionHooks boundedHooks(
      io.fleak.zephflow.api.execution.ExecutionControl control) {
    return new io.fleak.zephflow.api.execution.ExecutionHooks(
        control, mock(io.fleak.zephflow.api.execution.ExecutionHooks.Effects.class), Runnable::run);
  }

  @Mock private KafkaProducer<byte[], byte[]> mockProducer;
  @Mock private FleakSerializer<Object> mockSerializer;
  @Mock private FleakCounter mockDeliveredCountCounter;
  @Mock private FleakCounter mockDeliveredSizeCounter;
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
            mockDeliveredCountCounter,
            mockDeliveredSizeCounter,
            mockAsyncErrorCounter);
  }

  private static final Map<String, String> TEST_METRIC_TAGS = Map.of("callingUser", "testUser");

  @Test
  void testFlush_SendsAllRecords() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedEvents =
        createPreparedEvents(testEvents.subList(0, 3));

    SimpleSinkCommand.FlushResult result = flusher.flush(preparedEvents, TEST_METRIC_TAGS);

    assertEquals(3, result.successCount());
    assertEquals(0, result.flushedDataSize()); // size tracked via async counter, not FlushResult
    assertEquals(0, result.errorOutputList().size());
    verify(mockProducer, times(3)).send(any(ProducerRecord.class), any(Callback.class));
    verify(mockDeliveredCountCounter, times(3)).increase(TEST_METRIC_TAGS);
    verify(mockDeliveredSizeCounter, times(3)).increase(TEST_DATA.length, TEST_METRIC_TAGS);
    verify(mockAsyncErrorCounter, never()).increase(any());
  }

  @Test
  void testMixedErrors_SuccessCountExcludesSyncFailures() throws Exception {
    // First and third calls succeed, second fails with serialization error
    when(mockSerializer.serialize(anyList()))
        .thenReturn(new SerializedEvent(null, TEST_DATA, Map.of()))
        .thenThrow(new RuntimeException("Serialization error"))
        .thenReturn(new SerializedEvent(null, TEST_DATA, Map.of()));

    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedEvents =
        createPreparedEvents(testEvents.subList(0, 3));

    SimpleSinkCommand.FlushResult result = flusher.flush(preparedEvents, TEST_METRIC_TAGS);

    assertEquals(2, result.successCount(), "Only successfully-submitted records count");
    assertEquals(0, result.flushedDataSize());
    assertEquals(1, result.errorOutputList().size(), "Sync failures appear in errorOutputList");
    verify(mockProducer, times(2)).send(any(ProducerRecord.class), any(Callback.class));
    verify(mockDeliveredCountCounter, times(2)).increase(TEST_METRIC_TAGS);
    verify(mockDeliveredSizeCounter, times(2)).increase(TEST_DATA.length, TEST_METRIC_TAGS);
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
            mockDeliveredCountCounter,
            mockDeliveredSizeCounter,
            mockAsyncErrorCounter);

    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedEvents =
        createPreparedEvents(testEvents.subList(0, 3));

    SimpleSinkCommand.FlushResult result = failingFlusher.flush(preparedEvents, TEST_METRIC_TAGS);

    assertEquals(0, result.successCount());
    assertEquals(0, result.flushedDataSize());
    assertEquals(3, result.errorOutputList().size());
    verify(mockProducer, never()).send(any(ProducerRecord.class), any(Callback.class));
    verify(mockDeliveredCountCounter, never()).increase(any());
    verify(mockDeliveredSizeCounter, never()).increase(anyLong(), any());
    verify(mockAsyncErrorCounter, never()).increase(any());

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

    // Records were submitted to producer; async delivery failure is tracked via callback counter
    assertEquals(
        3, result.successCount(), "Records submitted count even when async delivery fails");
    assertEquals(0, result.flushedDataSize());
    assertEquals(0, result.errorOutputList().size(), "Async failures don't appear as sync errors");
    verify(mockProducer, times(3)).send(any(ProducerRecord.class), any(Callback.class));
    verify(mockAsyncErrorCounter, times(3)).increase(TEST_METRIC_TAGS);
    verify(mockDeliveredCountCounter, never()).increase(any());
    verify(mockDeliveredSizeCounter, never()).increase(anyLong(), any());
  }

  @Test
  void testErrorHandling_SyncProducerFailure() throws Exception {
    doThrow(new RuntimeException("Buffer full"))
        .when(mockProducer)
        .send(any(ProducerRecord.class), any(Callback.class));

    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedEvents =
        createPreparedEvents(testEvents.subList(0, 3));

    SimpleSinkCommand.FlushResult result = flusher.flush(preparedEvents, TEST_METRIC_TAGS);

    assertEquals(0, result.successCount());
    assertEquals(0, result.flushedDataSize());
    assertEquals(3, result.errorOutputList().size());
    verify(mockDeliveredCountCounter, never()).increase(any());
    verify(mockDeliveredSizeCounter, never()).increase(anyLong(), any());
    verify(mockAsyncErrorCounter, never()).increase(any()); // send threw — no callback fired
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
            mockDeliveredCountCounter,
            mockDeliveredSizeCounter,
            mockAsyncErrorCounter);

    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedEvents =
        createPreparedEvents(testEvents.subList(0, 3));

    SimpleSinkCommand.FlushResult result = flusherWithKey.flush(preparedEvents, TEST_METRIC_TAGS);

    assertEquals(3, result.successCount());
    assertEquals(0, result.flushedDataSize());
    verify(mockProducer, times(3)).send(any(ProducerRecord.class), any(Callback.class));
    verify(mockDeliveredCountCounter, times(3)).increase(TEST_METRIC_TAGS);
    verify(mockDeliveredSizeCounter, times(3)).increase(TEST_DATA.length, TEST_METRIC_TAGS);

    flusherWithKey.close();
  }

  @Test
  void testEmptyFlush_HandledGracefully() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> emptyEvents =
        new SimpleSinkCommand.PreparedInputEvents<>();

    SimpleSinkCommand.FlushResult result = flusher.flush(emptyEvents, TEST_METRIC_TAGS);

    assertEquals(0, result.successCount());
    assertEquals(0, result.flushedDataSize());
    assertEquals(0, result.errorOutputList().size());
    verify(mockProducer, never()).send(any(ProducerRecord.class), any(Callback.class));
    verify(mockDeliveredCountCounter, never()).increase(any());
    verify(mockDeliveredSizeCounter, never()).increase(anyLong(), any());
    verify(mockAsyncErrorCounter, never()).increase(any());
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

    assertEquals(3, result.successCount());
    assertEquals(0, result.flushedDataSize());
    assertEquals(0, result.errorOutputList().size());
    verify(mockProducer, times(3)).send(any(ProducerRecord.class), any(Callback.class));
    verify(mockDeliveredCountCounter, times(3)).increase(TEST_METRIC_TAGS);
    verify(mockDeliveredSizeCounter, times(3)).increase(TEST_DATA.length, TEST_METRIC_TAGS);
  }

  @Test
  void testResourceManagement_ProperCleanup() {
    flusher.close();

    // Shutdown is bounded so a wedged producer can't hang terminate() (see KafkaSinkFlusher.close).
    verify(mockProducer, times(1)).close(any(java.time.Duration.class));

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
            mockDeliveredCountCounter,
            mockDeliveredSizeCounter,
            mockAsyncErrorCounter);

    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedEvents =
        createPreparedEvents(testEvents.subList(0, 3));

    SimpleSinkCommand.FlushResult result = nullValueFlusher.flush(preparedEvents, TEST_METRIC_TAGS);

    assertEquals(0, result.successCount());
    assertEquals(0, result.flushedDataSize());
    verify(mockProducer, never()).send(any(ProducerRecord.class), any(Callback.class));
    verify(mockDeliveredCountCounter, never()).increase(any());
    verify(mockDeliveredSizeCounter, never()).increase(anyLong(), any());
    verify(mockAsyncErrorCounter, never()).increase(any());

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
            mockDeliveredCountCounter,
            mockDeliveredSizeCounter,
            mockAsyncErrorCounter);

    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedEvents =
        createPreparedEvents(testEvents.subList(0, 3));

    SimpleSinkCommand.FlushResult result = nullKeyFlusher.flush(preparedEvents, TEST_METRIC_TAGS);

    assertEquals(3, result.successCount());
    assertEquals(0, result.flushedDataSize());
    verify(mockProducer, times(3)).send(any(ProducerRecord.class), any(Callback.class));
    verify(mockDeliveredCountCounter, times(3)).increase(TEST_METRIC_TAGS);
    verify(mockDeliveredSizeCounter, times(3)).increase(TEST_DATA.length, TEST_METRIC_TAGS);

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
            mockDeliveredCountCounter,
            mockDeliveredSizeCounter,
            mockAsyncErrorCounter);

    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedEvents =
        createPreparedEvents(testEvents.subList(0, 3));

    SimpleSinkCommand.FlushResult result = errorKeyFlusher.flush(preparedEvents, TEST_METRIC_TAGS);

    assertEquals(0, result.successCount());
    assertEquals(0, result.flushedDataSize());
    assertEquals(3, result.errorOutputList().size());
    verify(mockDeliveredCountCounter, never()).increase(any());
    verify(mockDeliveredSizeCounter, never()).increase(anyLong(), any());
    verify(mockAsyncErrorCounter, never()).increase(any());

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
