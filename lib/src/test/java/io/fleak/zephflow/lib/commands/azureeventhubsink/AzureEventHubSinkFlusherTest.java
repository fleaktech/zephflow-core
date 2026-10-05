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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AzureEventHubSinkFlusherTest {

  @Test
  void boundedOversizeRejectionsAreDefiniteWithoutSending() throws Exception {
    EventDataBatch batch = mock(EventDataBatch.class);
    when(producerClient.createBatch()).thenReturn(batch);
    when(batch.tryAdd(any())).thenReturn(false);
    var result =
        new AzureEventHubSinkFlusher(producerClient, serializer, null)
            .flushBounded(
                prepared(record(Map.of("id", 1)), record(Map.of("id", 2))),
                Map.of(),
                boundedHooks(() -> {}));
    assertEquals(2, result.errorOutputList().size());
    assertEquals(0L, result.effectOutcome().attemptedCount());
    assertEquals(2L, result.effectOutcome().definiteFailureCount());
    assertEquals(2L, result.effectOutcome().notAttemptedCount());
    assertEquals(
        io.fleak.zephflow.api.execution.EffectOutcome.Delivery.FAILED,
        result.effectOutcome().delivery());
    verify(producerClient, never()).send(any(EventDataBatch.class));
  }

  @Test
  void boundedNullSerializationRejectsOnlyThatRecordAndPreservesSuccessfulBatch() throws Exception {
    FleakSerializer<?> mixed = mock();
    when(mixed.serialize(anyList()))
        .thenReturn(new io.fleak.zephflow.lib.serdes.SerializedEvent(null, null, null))
        .thenReturn(new io.fleak.zephflow.lib.serdes.SerializedEvent(null, new byte[] {1}, null));
    EventDataBatch batch = mock();
    when(producerClient.createBatch()).thenReturn(batch);
    when(batch.tryAdd(any())).thenReturn(true);
    when(batch.getCount()).thenReturn(1);
    var result =
        new AzureEventHubSinkFlusher(producerClient, mixed, null)
            .flushBounded(
                prepared(record(Map.of("id", 1)), record(Map.of("id", 2))),
                Map.of(),
                boundedHooks(() -> {}));
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
                new AzureEventHubSinkFlusher(producerClient, serializer, null)
                    .flushBounded(
                        prepared(record(Map.of("id", 1))),
                        Map.of(),
                        boundedHooks(
                            () -> {
                              throw new io.fleak.zephflow.api.execution.ExecutionStoppedException(
                                  "stop");
                            })));
    assertEquals(0L, failure.result().effectOutcome().attemptedCount());
    assertEquals(0L, failure.result().effectOutcome().unknownCount());
    assertEquals(1L, failure.result().effectOutcome().notAttemptedCount());
    assertEquals(
        io.fleak.zephflow.api.execution.EffectOutcome.Delivery.NOT_ATTEMPTED,
        failure.result().effectOutcome().delivery());
    verifyNoInteractions(producerClient);
  }

  private static io.fleak.zephflow.api.execution.ExecutionHooks boundedHooks(
      io.fleak.zephflow.api.execution.ExecutionControl control) {
    return new io.fleak.zephflow.api.execution.ExecutionHooks(control, mock(), Runnable::run);
  }

  @Test
  void boundedFailureInSecondPhysicalBatchPreservesFirstAcknowledgement() {
    EventDataBatch batch = mock(EventDataBatch.class);
    when(producerClient.createBatch()).thenReturn(batch);
    when(batch.tryAdd(any())).thenReturn(true, true, false, true);
    when(batch.getCount()).thenReturn(2);
    doNothing()
        .doThrow(new IllegalStateException("accepted remotely, acknowledgement lost"))
        .when(producerClient)
        .send(batch);
    var flusher = new AzureEventHubSinkFlusher(producerClient, serializer, null);
    var hooks =
        new io.fleak.zephflow.api.execution.ExecutionHooks(
            () -> {},
            mock(io.fleak.zephflow.api.execution.ExecutionHooks.Effects.class),
            Runnable::run);
    var failure =
        assertThrows(
            io.fleak.zephflow.lib.commands.sink.BoundedFlushException.class,
            () ->
                flusher.flushBounded(
                    prepared(
                        record(Map.of("id", 1)), record(Map.of("id", 2)), record(Map.of("id", 3))),
                    Map.of(),
                    hooks));
    assertEquals(2, failure.result().successCount());
    assertEquals(2L, failure.result().effectOutcome().acknowledgedCount());
    assertEquals(1L, failure.result().effectOutcome().unknownCount());
    assertEquals(0L, failure.result().effectOutcome().notAttemptedCount());
    verify(producerClient, times(2)).send(batch);
  }

  private EventHubProducerClient producerClient;
  private FleakSerializer<?> serializer;

  @BeforeEach
  void setUp() {
    producerClient = mock(EventHubProducerClient.class);
    serializer =
        SerializerFactory.createSerializerFactory(EncodingType.JSON_OBJECT).createSerializer();
  }

  private static SimpleSinkCommand.PreparedInputEvents<RecordFleakData> prepared(
      RecordFleakData... events) {
    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> prepared =
        new SimpleSinkCommand.PreparedInputEvents<>();
    for (RecordFleakData event : events) {
      prepared.add(event, event); // PassThrough preprocessor: prepared == raw
    }
    return prepared;
  }

  private static RecordFleakData record(Map<String, Object> fields) {
    return (RecordFleakData) FleakData.wrap(fields);
  }

  @Test
  void sendsAllEventsInOneBatchWhenTheyFit() throws Exception {
    EventDataBatch batch = mock(EventDataBatch.class);
    when(producerClient.createBatch()).thenReturn(batch);
    when(batch.tryAdd(any())).thenReturn(true);
    when(batch.getCount()).thenReturn(3);

    AzureEventHubSinkFlusher flusher =
        new AzureEventHubSinkFlusher(producerClient, serializer, null);

    SimpleSinkCommand.FlushResult result =
        flusher.flush(
            prepared(record(Map.of("id", 1)), record(Map.of("id", 2)), record(Map.of("id", 3))),
            Map.of());

    assertEquals(3, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    assertTrue(result.flushedDataSize() > 0);
    verify(producerClient, times(1)).send(batch);
  }

  @Test
  void rollsToANewBatchWhenTheCurrentBatchIsFull() throws Exception {
    EventDataBatch batch = mock(EventDataBatch.class);
    when(producerClient.createBatch()).thenReturn(batch);
    // First two events fit, the third does not (batch full), then it fits in the fresh batch.
    when(batch.tryAdd(any())).thenReturn(true, true, false, true);
    when(batch.getCount())
        .thenReturn(2); // non-empty so the full batch is sent, not treated as oversize

    AzureEventHubSinkFlusher flusher =
        new AzureEventHubSinkFlusher(producerClient, serializer, null);

    SimpleSinkCommand.FlushResult result =
        flusher.flush(
            prepared(record(Map.of("id", 1)), record(Map.of("id", 2)), record(Map.of("id", 3))),
            Map.of());

    assertEquals(3, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    verify(producerClient, times(2)).send(batch); // rolled: one full batch + the remainder
  }

  @Test
  void groupsEventsByPartitionKeyIntoSeparateBatches() throws Exception {
    EventDataBatch batch = mock(EventDataBatch.class);
    when(producerClient.createBatch(any(CreateBatchOptions.class))).thenReturn(batch);
    when(batch.tryAdd(any())).thenReturn(true);
    when(batch.getCount()).thenReturn(1);

    AzureEventHubSinkFlusher flusher =
        new AzureEventHubSinkFlusher(producerClient, serializer, PathExpression.fromString("$.k"));

    SimpleSinkCommand.FlushResult result =
        flusher.flush(
            prepared(
                record(Map.of("k", "a", "id", 1)),
                record(Map.of("k", "a", "id", 2)),
                record(Map.of("k", "b", "id", 3))),
            Map.of());

    assertEquals(3, result.successCount());
    // Two distinct partition keys -> two keyed batches -> two sends.
    verify(producerClient, times(2)).createBatch(any(CreateBatchOptions.class));
    verify(producerClient, times(2)).send(batch);
    verify(producerClient, never()).createBatch();
  }

  @Test
  void reportsOversizedEventAsErrorWithoutSending() throws Exception {
    EventDataBatch batch = mock(EventDataBatch.class);
    when(producerClient.createBatch()).thenReturn(batch);
    when(batch.tryAdd(any())).thenReturn(false); // never fits
    when(batch.getCount()).thenReturn(0); // empty -> event itself is too large

    AzureEventHubSinkFlusher flusher =
        new AzureEventHubSinkFlusher(producerClient, serializer, null);

    SimpleSinkCommand.FlushResult result =
        flusher.flush(prepared(record(Map.of("id", 1))), Map.of());

    assertEquals(0, result.successCount());
    assertEquals(1, result.errorOutputList().size());
    verify(producerClient, never()).send(any(EventDataBatch.class));
  }

  @Test
  void returnsEmptyResultForNoEvents() throws Exception {
    AzureEventHubSinkFlusher flusher =
        new AzureEventHubSinkFlusher(producerClient, serializer, null);

    SimpleSinkCommand.FlushResult result =
        flusher.flush(new SimpleSinkCommand.PreparedInputEvents<>(), Map.of());

    assertEquals(0, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    verifyNoInteractions(producerClient);
  }

  @Test
  void throwsWhenFlushingAfterClose() {
    AzureEventHubSinkFlusher flusher =
        new AzureEventHubSinkFlusher(producerClient, serializer, null);
    flusher.close();
    assertThrows(
        IllegalStateException.class,
        () -> flusher.flush(prepared(record(Map.of("id", 1))), Map.of()));
    verify(producerClient).close();
  }
}
