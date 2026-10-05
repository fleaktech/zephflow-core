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
package io.fleak.zephflow.lib.commands.kinesis;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.execution.EffectOutcome;
import io.fleak.zephflow.api.execution.ExecutionHooks;
import io.fleak.zephflow.api.execution.ExecutionStoppedException;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.commands.sink.BoundedFlushException;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResponse;
import software.amazon.awssdk.services.kinesis.model.PutRecordsResultEntry;

class KinesisFlusherTest {

  private static final String STREAM_NAME = "test-stream";
  private KinesisClient kinesisClient;
  private KinesisFlusher flusher;
  private FleakSerializer<?> serializer;

  @BeforeEach
  void setUp() {
    kinesisClient = mock(KinesisClient.class);
    PathExpression partitionKeyPathExpression = PathExpression.fromString("$.partitionKey");

    SerializerFactory<?> serializerFactory =
        SerializerFactory.createSerializerFactory(EncodingType.JSON_OBJECT);
    serializer = serializerFactory.createSerializer();

    flusher =
        new KinesisFlusher(kinesisClient, STREAM_NAME, partitionKeyPathExpression, serializer);
  }

  @Test
  void testSuccessfulFlush() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedInputEvents =
        new SimpleSinkCommand.PreparedInputEvents<>();

    RecordFleakData record1 =
        new RecordFleakData(
            Map.of(
                "partitionKey",
                new StringPrimitiveFleakData("key1"),
                "data",
                new StringPrimitiveFleakData("value1")));
    long record1Size = serializer.serialize(List.of(record1)).value().length;
    RecordFleakData record2 =
        new RecordFleakData(
            Map.of(
                "partitionKey",
                new StringPrimitiveFleakData("key2"),
                "data",
                new StringPrimitiveFleakData("value2")));
    long record2Size = serializer.serialize(List.of(record2)).value().length;
    preparedInputEvents.add(record1, record1);
    preparedInputEvents.add(record2, record2);

    PutRecordsResponse mockResponse =
        PutRecordsResponse.builder()
            .failedRecordCount(0)
            .records(
                PutRecordsResultEntry.builder().build(), PutRecordsResultEntry.builder().build())
            .build();

    when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(mockResponse);

    SimpleSinkCommand.FlushResult result = flusher.flush(preparedInputEvents, Map.of());

    assertEquals(2, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    assertEquals(record1Size + record2Size, result.flushedDataSize());
    verify(kinesisClient)
        .putRecords(
            argThat(
                (PutRecordsRequest request) -> {
                  assertEquals(STREAM_NAME, request.streamName());
                  assertEquals(2, request.records().size());
                  assertEquals("key1", request.records().get(0).partitionKey());
                  assertEquals("key2", request.records().get(1).partitionKey());
                  return true;
                }));
  }

  @Test
  void testPartialFailure() {
    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedInputEvents =
        new SimpleSinkCommand.PreparedInputEvents<>();
    preparedInputEvents.add(
        new RecordFleakData(
            Map.of(
                "partitionKey",
                new StringPrimitiveFleakData("key1"),
                "data",
                new StringPrimitiveFleakData("value1"))),
        new RecordFleakData(
            Map.of(
                "partitionKey",
                new StringPrimitiveFleakData("key1"),
                "data",
                new StringPrimitiveFleakData("value1"))));
    preparedInputEvents.add(
        new RecordFleakData(
            Map.of(
                "partitionKey",
                new StringPrimitiveFleakData("key2"),
                "data",
                new StringPrimitiveFleakData("value2"))),
        new RecordFleakData(
            Map.of(
                "partitionKey",
                new StringPrimitiveFleakData("key2"),
                "data",
                new StringPrimitiveFleakData("value2"))));

    PutRecordsResponse mockResponse =
        PutRecordsResponse.builder()
            .failedRecordCount(1)
            .records(
                PutRecordsResultEntry.builder().build(),
                PutRecordsResultEntry.builder()
                    .errorCode("InternalFailure")
                    .errorMessage("Internal error")
                    .build())
            .build();

    when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(mockResponse);

    SimpleSinkCommand.FlushResult result = flusher.flush(preparedInputEvents, Map.of());

    assertEquals(1, result.successCount());
    assertEquals(1, result.errorOutputList().size());
    assertEquals("Internal error", result.errorOutputList().get(0).errorMessage());
  }

  @Test
  void testNullResponseFromKinesisClient() {
    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedInputEvents =
        new SimpleSinkCommand.PreparedInputEvents<>();
    preparedInputEvents.add(
        new RecordFleakData(
            Map.of(
                "partitionKey",
                new StringPrimitiveFleakData("key1"),
                "data",
                new StringPrimitiveFleakData("value1"))),
        new RecordFleakData(
            Map.of(
                "partitionKey",
                new StringPrimitiveFleakData("key1"),
                "data",
                new StringPrimitiveFleakData("value1"))));

    when(kinesisClient.putRecords(any(PutRecordsRequest.class))).thenReturn(null);

    SimpleSinkCommand.FlushResult result = flusher.flush(preparedInputEvents, Map.of());

    assertEquals(0, result.successCount());
    assertEquals(1, result.errorOutputList().size());
    assertTrue(result.errorOutputList().get(0).errorMessage().contains("Kinesis client error"));
    assertTrue(
        result
            .errorOutputList()
            .get(0)
            .errorMessage()
            .contains("Received null response from Kinesis client"));

    verify(kinesisClient).putRecords(any(PutRecordsRequest.class));
  }

  @Test
  void testEmptyBatch() {
    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedInputEvents =
        new SimpleSinkCommand.PreparedInputEvents<>();

    SimpleSinkCommand.FlushResult result = flusher.flush(preparedInputEvents, Map.of());

    assertEquals(0, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    verify(kinesisClient, never()).putRecords(any(PutRecordsRequest.class));
  }

  private static RecordFleakData event(String value) {
    return new RecordFleakData(Map.of("data", new StringPrimitiveFleakData(value)));
  }

  private static ExecutionHooks hooks() {
    return new ExecutionHooks(
        () -> {},
        new ExecutionHooks.Effects() {
          public long started() {
            return 1;
          }

          public void finished(long effectId, EffectOutcome outcome) {}
        },
        Runnable::run);
  }

  @Test
  void boundedReceiptsMapSubmittedPositionsAfterSerializationFailure() throws Exception {
    var failingSerializer = mock(FleakSerializer.class);
    when(failingSerializer.serialize(anyList()))
        .thenThrow(new IllegalArgumentException("bad data"))
        .thenReturn(
            new io.fleak.zephflow.lib.serdes.SerializedEvent(null, new byte[] {1, 2, 3}, Map.of()))
        .thenReturn(
            new io.fleak.zephflow.lib.serdes.SerializedEvent(null, new byte[] {4, 5}, Map.of()));
    var bounded = new KinesisFlusher(kinesisClient, STREAM_NAME, null, failingSerializer);
    var malformed = event("bad");
    var accepted = event("accepted");
    var rejected = event("rejected");
    var events = new SimpleSinkCommand.PreparedInputEvents<RecordFleakData>();
    events.add(malformed, malformed);
    events.add(accepted, accepted);
    events.add(rejected, rejected);
    when(kinesisClient.putRecords(any(PutRecordsRequest.class)))
        .thenReturn(
            PutRecordsResponse.builder()
                .failedRecordCount(1)
                .records(
                    PutRecordsResultEntry.builder()
                        .sequenceNumber("receipt")
                        .shardId("shard")
                        .build(),
                    PutRecordsResultEntry.builder()
                        .errorCode("Rejected")
                        .errorMessage("rejected")
                        .build())
                .build());
    var result = bounded.flushBounded(events, Map.of(), hooks());
    assertEquals(1, result.successCount());
    assertEquals(3, result.flushedDataSize());
    assertEquals(2L, result.effectOutcome().attemptedCount());
    assertEquals(1L, result.effectOutcome().acknowledgedCount());
    assertEquals(2L, result.effectOutcome().definiteFailureCount());
    assertEquals(0L, result.effectOutcome().unknownCount());
    assertEquals(1L, result.effectOutcome().notAttemptedCount());
    assertEquals(EffectOutcome.Delivery.PARTIAL, result.effectOutcome().delivery());
    assertEquals(malformed, result.errorOutputList().get(0).inputEvent());
    assertEquals(rejected, result.errorOutputList().get(1).inputEvent());
    verify(kinesisClient)
        .putRecords(argThat((PutRecordsRequest request) -> request.records().size() == 2));
  }

  @Test
  void boundedTransportAndMissingReceiptsStayUnknownWhileKnownReceiptsRemain() {
    var events = new SimpleSinkCommand.PreparedInputEvents<RecordFleakData>();
    events.add(event("a"), event("a"));
    events.add(event("b"), event("b"));
    when(kinesisClient.putRecords(any(PutRecordsRequest.class)))
        .thenReturn(
            PutRecordsResponse.builder()
                .failedRecordCount(0)
                .records(
                    PutRecordsResultEntry.builder()
                        .sequenceNumber("123")
                        .shardId("shard-0")
                        .build())
                .build())
        .thenThrow(new RuntimeException("response lost"));
    var incomplete = flusher.flushBounded(events, Map.of(), hooks());
    assertEquals(1L, incomplete.effectOutcome().acknowledgedCount());
    assertEquals(1L, incomplete.effectOutcome().unknownCount());
    var transport = flusher.flushBounded(events, Map.of(), hooks());
    assertEquals(0L, transport.effectOutcome().definiteFailureCount());
    assertEquals(2L, transport.effectOutcome().unknownCount());
    assertEquals(EffectOutcome.Delivery.UNKNOWN, transport.effectOutcome().delivery());
  }

  static Stream<PutRecordsResultEntry> incompleteReceipts() {
    return Stream.of(
        PutRecordsResultEntry.builder().build(),
        PutRecordsResultEntry.builder().sequenceNumber("123").build(),
        PutRecordsResultEntry.builder().shardId("shard-0").build(),
        PutRecordsResultEntry.builder().sequenceNumber("").shardId("shard-0").build(),
        PutRecordsResultEntry.builder().sequenceNumber("123").shardId(" ").build());
  }

  @ParameterizedTest
  @MethodSource("incompleteReceipts")
  void boundedIncompleteReceiptPreservesUnknownBesideConfirmedAckAndRejection(
      PutRecordsResultEntry incompleteReceipt) throws Exception {
    var unknown = event("unknown");
    var accepted = event("accepted");
    var rejected = event("rejected");
    var events = new SimpleSinkCommand.PreparedInputEvents<RecordFleakData>();
    events.add(unknown, unknown);
    events.add(accepted, accepted);
    events.add(rejected, rejected);
    when(kinesisClient.putRecords(any(PutRecordsRequest.class)))
        .thenReturn(
            PutRecordsResponse.builder()
                .failedRecordCount(1)
                .records(
                    incompleteReceipt,
                    PutRecordsResultEntry.builder()
                        .sequenceNumber("123")
                        .shardId("shard-0")
                        .build(),
                    PutRecordsResultEntry.builder()
                        .errorCode("InternalFailure")
                        .errorMessage("rejected")
                        .build())
                .build());

    var result = flusher.flushBounded(events, Map.of(), hooks());

    assertEquals(1, result.successCount());
    assertEquals(serializer.serialize(List.of(accepted)).value().length, result.flushedDataSize());
    assertEquals(3L, result.effectOutcome().attemptedCount());
    assertEquals(1L, result.effectOutcome().acknowledgedCount());
    assertEquals(1L, result.effectOutcome().definiteFailureCount());
    assertEquals(1L, result.effectOutcome().unknownCount());
    assertEquals(0L, result.effectOutcome().notAttemptedCount());
    assertEquals(EffectOutcome.Delivery.PARTIAL, result.effectOutcome().delivery());
    assertEquals(2, result.errorOutputList().size());
    assertEquals(unknown, result.errorOutputList().getFirst().inputEvent());
    assertEquals(
        "Kinesis delivery acknowledgement unavailable",
        result.errorOutputList().getFirst().errorMessage());
    assertEquals(rejected, result.errorOutputList().getLast().inputEvent());
    verify(kinesisClient, times(1)).putRecords(any(PutRecordsRequest.class));
  }

  @Test
  void boundedStopBeforeRequestRetainsNotAttemptedReceiptAndDoesNotCallClient() {
    var events = new SimpleSinkCommand.PreparedInputEvents<RecordFleakData>();
    events.add(event("a"), event("a"));
    var base = hooks();
    var stopped =
        new ExecutionHooks(
            () -> {
              throw new ExecutionStoppedException("cancelled");
            },
            base.effects(),
            base.backgroundWork());
    var failure =
        assertThrows(
            BoundedFlushException.class, () -> flusher.flushBounded(events, Map.of(), stopped));
    assertEquals(1L, failure.result().effectOutcome().notAttemptedCount());
    assertEquals(0L, failure.result().effectOutcome().unknownCount());
    verifyNoInteractions(kinesisClient);
  }
}
