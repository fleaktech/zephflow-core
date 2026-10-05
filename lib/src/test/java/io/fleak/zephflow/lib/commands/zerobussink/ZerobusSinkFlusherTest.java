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
package io.fleak.zephflow.lib.commands.zerobussink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.databricks.zerobus.ZerobusException;
import com.databricks.zerobus.ZerobusJsonStream;
import com.databricks.zerobus.ZerobusProtoStream;
import com.databricks.zerobus.ZerobusSdk;
import com.google.protobuf.Descriptors;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand.FlushResult;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand.PreparedInputEvents;
import io.fleak.zephflow.lib.commands.sink.UnknownSinkCommitStateException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

/**
 * Unit tests for {@link ZerobusSinkFlusher} using mocked SDK streams. These lock down the
 * data-consistency behavior the flusher owns: per-record encode failures must not fail the whole
 * batch, durability-wait failure must surface as {@link UnknownSinkCommitStateException}, and an
 * all-failed batch must not touch the stream.
 */
class ZerobusSinkFlusherTest {

  // {id: int (non-nullable), name: string (non-nullable)} — a record missing "id" fails to encode.
  private static final Map<String, Object> AVRO_SCHEMA_MAP =
      Map.of(
          "type", "record",
          "name", "TestRecord",
          "fields",
              List.of(
                  Map.of("name", "id", "type", "int"), Map.of("name", "name", "type", "string")));

  private static Schema avroSchema() {
    return AvroToProtoDescriptorConverter.parseAvro(AVRO_SCHEMA_MAP);
  }

  private static Descriptors.Descriptor descriptor() {
    return AvroToProtoDescriptorConverter.toDescriptor(
        AvroToProtoDescriptorConverter.toDescriptorProto(AVRO_SCHEMA_MAP, "c.s.t"));
  }

  private static PreparedInputEvents<Map<String, Object>> events(List<Map<String, Object>> rows) {
    PreparedInputEvents<Map<String, Object>> events = new PreparedInputEvents<>();
    for (Map<String, Object> row : rows) {
      events.add(new RecordFleakData(), row);
    }
    return events;
  }

  @Test
  void boundedStopBeforeStreamWritePreservesNotAttemptedReceipt() throws Exception {
    ZerobusProtoStream stream = mock();
    var flusher = new ZerobusSinkFlusher("c.s.t", mock(), stream, avroSchema(), descriptor(), null);
    var base = hooks();
    var stopped =
        new io.fleak.zephflow.api.execution.ExecutionHooks(
            () -> {
              throw new io.fleak.zephflow.api.execution.ExecutionStoppedException("cancelled");
            },
            base.effects(),
            base.backgroundWork());
    try {
      var failure =
          assertThrows(
              io.fleak.zephflow.lib.commands.sink.BoundedFlushException.class,
              () ->
                  flusher.flushBounded(
                      events(List.of(Map.of("id", 1, "name", "ok"))), Map.of(), stopped));
      var outcome = failure.result().effectOutcome();
      assertEquals(0L, outcome.attemptedCount());
      assertEquals(0L, outcome.unknownCount());
      assertEquals(1L, outcome.notAttemptedCount());
      assertEquals(
          io.fleak.zephflow.api.execution.EffectOutcome.Delivery.NOT_ATTEMPTED, outcome.delivery());
      verifyNoInteractions(stream);
    } finally {
      flusher.close();
    }
  }

  @Test
  void boundedSdkFailuresDoNotLogRawCredentialsOrCauses() throws Exception {
    var logger =
        (org.apache.logging.log4j.core.Logger)
            org.apache.logging.log4j.LogManager.getLogger(ZerobusSinkFlusher.class);
    var logs = new java.util.ArrayList<org.apache.logging.log4j.core.LogEvent>();
    var appender =
        new org.apache.logging.log4j.core.appender.AbstractAppender(
            "bounded-zerobus-test",
            null,
            org.apache.logging.log4j.core.layout.PatternLayout.createDefaultLayout(),
            false,
            org.apache.logging.log4j.core.config.Property.EMPTY_ARRAY) {
          public void append(org.apache.logging.log4j.core.LogEvent event) {
            logs.add(event.toImmutable());
          }
        };
    appender.start();
    logger.addAppender(appender);
    try {
      for (boolean duringIngest : List.of(true, false)) {
        ZerobusProtoStream stream = mock();
        var sdkFailure = new ZerobusException("clientSecret=private-zerobus-test-token");
        if (duringIngest) {
          when(stream.ingestRecordsOffset(ArgumentMatchers.<byte[]>anyList()))
              .thenThrow(sdkFailure);
        } else {
          when(stream.ingestRecordsOffset(ArgumentMatchers.<byte[]>anyList()))
              .thenReturn(Optional.of(10L));
          doThrow(sdkFailure).when(stream).waitForOffset(10L);
        }
        var flusher =
            new ZerobusSinkFlusher("c.s.t", mock(), stream, avroSchema(), descriptor(), null);
        var failure =
            assertThrows(
                io.fleak.zephflow.lib.commands.sink.BoundedFlushException.class,
                () ->
                    flusher.flushBounded(
                        events(List.of(Map.of("id", 1, "name", "ok"))), Map.of(), hooks()));
        assertEquals(1L, failure.result().effectOutcome().unknownCount());
        assertNotNull(failure.getCause());
        flusher.close();
      }
      assertTrue(
          logs.stream()
              .noneMatch(
                  event ->
                      event
                          .getMessage()
                          .getFormattedMessage()
                          .contains("private-zerobus-test-token")));
      assertTrue(logs.stream().allMatch(event -> event.getThrown() == null));
    } finally {
      logger.removeAppender(appender);
      appender.stop();
    }
  }

  @Test
  void boundedEncodingRejectionsAreNotCountedAsUnknownDeliveries() throws Exception {
    ZerobusProtoStream stream = mock();
    when(stream.ingestRecordsOffset(ArgumentMatchers.<byte[]>anyList()))
        .thenReturn(Optional.of(10L));
    var flusher = new ZerobusSinkFlusher("c.s.t", mock(), stream, avroSchema(), descriptor(), null);
    var result =
        flusher.flushBounded(
            events(List.of(Map.of("id", 1, "name", "ok"), Map.of("name", "missing-id"))),
            Map.of(),
            hooks());
    assertEquals(1L, result.effectOutcome().acknowledgedCount());
    assertEquals(1L, result.effectOutcome().notAttemptedCount());
    assertEquals(1L, result.effectOutcome().definiteFailureCount());
    assertEquals(0L, result.effectOutcome().unknownCount());
    verify(stream).waitForOffset(10L);
    flusher.close();
  }

  @Test
  void boundedUnknownRemoteOutcomeKeepsKnownEncodingRejections() throws Exception {
    ZerobusProtoStream stream = mock();
    when(stream.ingestRecordsOffset(ArgumentMatchers.<byte[]>anyList()))
        .thenReturn(Optional.of(10L));
    doThrow(new ZerobusException("ack lost")).when(stream).waitForOffset(10L);
    var flusher = new ZerobusSinkFlusher("c.s.t", mock(), stream, avroSchema(), descriptor(), null);
    var failure =
        assertThrows(
            io.fleak.zephflow.lib.commands.sink.BoundedFlushException.class,
            () ->
                flusher.flushBounded(
                    events(List.of(Map.of("id", 1, "name", "ok"), Map.of("name", "missing-id"))),
                    Map.of(),
                    hooks()));
    assertEquals(1L, failure.result().effectOutcome().notAttemptedCount());
    assertEquals(1L, failure.result().effectOutcome().definiteFailureCount());
    assertEquals(1L, failure.result().effectOutcome().unknownCount());
    assertEquals(1, failure.result().errorOutputList().size());
    flusher.close();
  }

  @Test
  void boundedCloseFailureIsReportedAndSdkStillCloses() throws Exception {
    ZerobusProtoStream stream = mock();
    ZerobusSdk sdk = mock();
    var flusher = new ZerobusSinkFlusher("c.s.t", sdk, stream, avroSchema(), descriptor(), null);
    flusher.flushBounded(events(List.of()), Map.of(), hooks());
    doThrow(new ZerobusException("close failed")).when(stream).close();
    assertThrows(IllegalStateException.class, flusher::close);
    verify(sdk).close();
  }

  @Test
  void boundedFailureDoesNotReconnectOrSendFollowingBatch() throws Exception {
    ZerobusProtoStream stream = mock();
    when(stream.ingestRecordsOffset(ArgumentMatchers.<byte[]>anyList()))
        .thenReturn(Optional.of(7L));
    doThrow(new ZerobusException("ack lost")).when(stream).waitForOffset(7L);
    var flusher = new ZerobusSinkFlusher("c.s.t", mock(), stream, avroSchema(), descriptor(), null);
    var input = events(List.of(Map.of("id", 1, "name", "ok")));
    var failure =
        assertThrows(
            io.fleak.zephflow.lib.commands.sink.BoundedFlushException.class,
            () -> flusher.flushBounded(input, Map.of(), hooks()));
    assertEquals(1L, failure.result().effectOutcome().unknownCount());
    var next = flusher.flushBounded(input, Map.of(), hooks());
    assertEquals(1L, next.effectOutcome().notAttemptedCount());
    assertEquals(0L, next.effectOutcome().attemptedCount());
    verify(stream, times(1)).ingestRecordsOffset(ArgumentMatchers.<byte[]>anyList());
    flusher.close();
  }

  private static io.fleak.zephflow.api.execution.ExecutionHooks hooks() {
    return new io.fleak.zephflow.api.execution.ExecutionHooks(
        () -> {},
        new io.fleak.zephflow.api.execution.ExecutionHooks.Effects() {
          public long started() {
            return 1;
          }

          public void finished(long id, io.fleak.zephflow.api.execution.EffectOutcome outcome) {}
        },
        Runnable::run);
  }

  @Test
  void protoFlushReportsEncodeErrorsAndIngestsOnlyValidPayloads() throws Exception {
    ZerobusProtoStream stream = mock(ZerobusProtoStream.class);
    when(stream.ingestRecordsOffset(ArgumentMatchers.<byte[]>anyList()))
        .thenReturn(Optional.of(10L));
    ZerobusSinkFlusher flusher =
        new ZerobusSinkFlusher(
            "c.s.t", mock(ZerobusSdk.class), stream, avroSchema(), descriptor(), null);

    // one valid record, one missing the non-nullable "id" field (fails to encode)
    FlushResult result =
        flusher.flush(
            events(List.of(Map.of("id", 1, "name", "ok"), Map.of("name", "missing-id"))), Map.of());

    assertEquals(1, result.successCount());
    assertEquals(1, result.errorOutputList().size());
    assertTrue(result.errorOutputList().getFirst().errorMessage().contains("non-nullable"));
    verify(stream).waitForOffset(10L);
  }

  @Test
  void protoDurabilityWaitFailureThrowsUnknownCommitState() throws Exception {
    ZerobusProtoStream stream = mock(ZerobusProtoStream.class);
    when(stream.ingestRecordsOffset(ArgumentMatchers.<byte[]>anyList()))
        .thenReturn(Optional.of(7L));
    doThrow(new ZerobusException("ack timeout")).when(stream).waitForOffset(7L);
    ZerobusSinkFlusher flusher =
        new ZerobusSinkFlusher(
            "c.s.t", mock(ZerobusSdk.class), stream, avroSchema(), descriptor(), null);

    UnknownSinkCommitStateException e =
        assertThrows(
            UnknownSinkCommitStateException.class,
            () -> flusher.flush(events(List.of(Map.of("id", 1, "name", "ok"))), Map.of()));
    assertTrue(e.getMessage().contains("commit state is unknown"));
  }

  @Test
  void protoIngestFailureThrowsUnknownCommitState() throws Exception {
    ZerobusProtoStream stream = mock(ZerobusProtoStream.class);
    when(stream.ingestRecordsOffset(ArgumentMatchers.<byte[]>anyList()))
        .thenThrow(new ZerobusException("native ingest failed"));
    ZerobusSinkFlusher flusher =
        new ZerobusSinkFlusher(
            "c.s.t", mock(ZerobusSdk.class), stream, avroSchema(), descriptor(), null);

    UnknownSinkCommitStateException e =
        assertThrows(
            UnknownSinkCommitStateException.class,
            () -> flusher.flush(events(List.of(Map.of("id", 1, "name", "ok"))), Map.of()));
    assertTrue(e.getMessage().contains("commit state is unknown"));
    // never reached the durability wait
    verify(stream, never()).waitForOffset(anyLong());
  }

  @Test
  void jsonIngestFailureThrowsUnknownCommitState() throws Exception {
    ZerobusJsonStream stream = mock(ZerobusJsonStream.class);
    when(stream.ingestRecordsOffset(ArgumentMatchers.<String>anyIterable()))
        .thenThrow(new ZerobusException("native ingest failed"));
    ZerobusSinkFlusher flusher =
        new ZerobusSinkFlusher("c.s.t", mock(ZerobusSdk.class), null, null, null, stream);

    UnknownSinkCommitStateException e =
        assertThrows(
            UnknownSinkCommitStateException.class,
            () -> flusher.flush(events(List.of(Map.of("id", 1, "name", "ok"))), Map.of()));
    assertTrue(e.getMessage().contains("commit state is unknown"));
    verify(stream, never()).waitForOffset(anyLong());
  }

  @Test
  void allRecordsFailingToEncodeDoesNotTouchTheStream() throws Exception {
    ZerobusProtoStream stream = mock(ZerobusProtoStream.class);
    ZerobusSinkFlusher flusher =
        new ZerobusSinkFlusher(
            "c.s.t", mock(ZerobusSdk.class), stream, avroSchema(), descriptor(), null);

    FlushResult result =
        flusher.flush(
            events(List.of(Map.of("name", "no-id-1"), Map.of("name", "no-id-2"))), Map.of());

    assertEquals(0, result.successCount());
    assertEquals(2, result.errorOutputList().size());
    verify(stream, never()).ingestRecordsOffset(ArgumentMatchers.<byte[]>anyList());
    verify(stream, never()).waitForOffset(anyLong());
  }

  @Test
  void jsonFlushIngestsValidPayloadsAndWaits() throws Exception {
    ZerobusJsonStream stream = mock(ZerobusJsonStream.class);
    when(stream.ingestRecordsOffset(ArgumentMatchers.<String>anyIterable()))
        .thenReturn(Optional.of(3L));
    // JSON mode needs no avro schema / descriptor
    ZerobusSinkFlusher flusher =
        new ZerobusSinkFlusher("c.s.t", mock(ZerobusSdk.class), null, null, null, stream);

    FlushResult result =
        flusher.flush(events(List.of(Map.of("id", 1, "name", "a"), Map.of("id", 2))), Map.of());

    assertEquals(2, result.successCount());
    assertEquals(0, result.errorOutputList().size());
    verify(stream).waitForOffset(3L);
  }

  @Test
  void emptyBatchNeverWaitsForOffset() throws Exception {
    ZerobusProtoStream stream = mock(ZerobusProtoStream.class);
    ZerobusSinkFlusher flusher =
        new ZerobusSinkFlusher(
            "c.s.t", mock(ZerobusSdk.class), stream, avroSchema(), descriptor(), null);

    FlushResult result = flusher.flush(events(List.of()), Map.of());

    assertEquals(0, result.successCount());
    assertEquals(0, result.errorOutputList().size());
    verify(stream, never()).ingestRecordsOffset(ArgumentMatchers.<byte[]>anyList());
    verify(stream, never()).waitForOffset(anyLong());
  }
}
