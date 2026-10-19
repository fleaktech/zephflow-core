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
package io.fleak.zephflow.lib.commands.source;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.NodeExecutionException;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SimpleSourceCommandBatchEmittingTest {

  private static final String NODE_ID = "source";

  /** A DLQ entry as a comparable value; the error text is cut to its first line. */
  record DlqEntry(String key, String value, String errorFirstLine, String nodeId) {}

  private final List<String> events = new ArrayList<>();
  private final List<DlqEntry> dlq = new ArrayList<>();
  private Fetcher<String> fetcher;
  private DlqWriter dlqWriter;
  private RawDataSampler<String> sampler;
  private SourceEventAcceptor acceptor;
  private TestSource source;

  /** Emits two batches per fetched record, each one event named after the record. */
  private class TwoBatches implements BatchEmittingConverter<String> {
    @Override
    public void emit(String sourceRecord, SourceExecutionContext<?> context, Emitter emitter) {
      for (int batch = 1; batch <= 2; batch++) {
        if (emitter.stopped()) {
          throw new IllegalStateException("client closed");
        }
        emitter.accept(List.of(record(sourceRecord + "-" + batch)));
      }
    }

    @Override
    public void acknowledge(String sourceRecord) {
      events.add("ack " + sourceRecord);
    }
  }

  /** The body of a converter's emit; the converter built from it records acknowledgements. */
  @FunctionalInterface
  private interface EmitBody {
    void emit(String sourceRecord, BatchEmittingConverter.Emitter emitter);
  }

  private BatchEmittingConverter<String> converter(EmitBody body) {
    return new BatchEmittingConverter<>() {
      @Override
      public void emit(String sourceRecord, SourceExecutionContext<?> context, Emitter emitter) {
        body.emit(sourceRecord, emitter);
      }

      @Override
      public void acknowledge(String sourceRecord) {
        events.add("ack " + sourceRecord);
      }
    };
  }

  @BeforeEach
  void setUp() {
    fetcher = mock();
    when(fetcher.committer()).thenReturn(() -> events.add("commit"));
    dlqWriter = mock(DlqWriter.class);
    doAnswer(
            invocation -> {
              SerializedEvent raw = invocation.getArgument(1);
              String error = invocation.getArgument(2);
              dlq.add(
                  new DlqEntry(
                      raw.key() == null ? null : new String(raw.key()),
                      new String(raw.value()),
                      error.lines().findFirst().orElse(""),
                      invocation.getArgument(3)));
              return null;
            })
        .when(dlqWriter)
        .writeToDlq(anyLong(), any(), anyString(), anyString());
    //noinspection unchecked
    sampler = mock(RawDataSampler.class);
    acceptor = mock(SourceEventAcceptor.class);
    doAnswer(
            invocation -> {
              List<RecordFleakData> batch = invocation.getArgument(0);
              batch.forEach(r -> events.add("accept " + r.unwrap().get("id")));
              return null;
            })
        .when(acceptor)
        .accept(anyList());
  }

  @Test
  void eachRecordIsAcknowledgedAndCommittedAfterItsLastBatchEvenWhenTheFetcherIsExhausted() {
    when(fetcher.fetch()).thenReturn(List.of("a", "b"));
    when(fetcher.isExhausted()).thenReturn(true);

    run(new TwoBatches());

    assertEquals(
        List.of(
            "accept a-1",
            "accept a-2",
            "ack a",
            "commit",
            "accept b-1",
            "accept b-2",
            "ack b",
            "commit"),
        events);
    verify(sampler).maybeSample(List.of("a", "b"));
    assertEquals(List.of(), dlq);
  }

  @Test
  void aStopBetweenBatchesLeavesTheRecordUnacknowledgedAndTheRestUnhandled() {
    when(fetcher.fetch()).thenReturn(List.of("a", "b"));
    when(fetcher.isExhausted()).thenReturn(true);
    stopAfterFirstBatch();

    run(
        converter(
            (sourceRecord, emitter) -> {
              for (int batch = 1; batch <= 2 && !emitter.stopped(); batch++) {
                emitter.accept(List.of(record(sourceRecord + "-" + batch)));
              }
            }));

    assertEquals(List.of("accept a-1"), events);
    assertEquals(List.of(), dlq);
  }

  @Test
  void aFailureAfterTheStopIsNotDeadLettered() {
    when(fetcher.fetch()).thenReturn(List.of("a", "b"));
    when(fetcher.isExhausted()).thenReturn(true);
    stopAfterFirstBatch();

    run(new TwoBatches());

    assertEquals(List.of("accept a-1"), events);
    assertEquals(List.of(), dlq);
  }

  @Test
  void aStopDuringACommitLeavesTheNextRecordUnhandled() {
    when(fetcher.fetch()).thenReturn(List.of("a", "b"));
    when(fetcher.isExhausted()).thenReturn(true);
    when(fetcher.committer())
        .thenReturn(
            () -> {
              events.add("commit");
              source.terminate();
            });

    run(converter((sourceRecord, emitter) -> emitter.accept(List.of(record(sourceRecord)))));

    assertEquals(List.of("accept a", "ack a", "commit"), events);
  }

  @Test
  void aStopDuringTheFetchLeavesEveryFetchedRecordUnhandled() {
    when(fetcher.fetch())
        .thenAnswer(
            invocation -> {
              source.terminate();
              return List.of("a", "b");
            });

    run(converter((sourceRecord, emitter) -> emitter.accept(List.of(record(sourceRecord)))));

    assertEquals(List.of(), events);
  }

  @Test
  void anExceptionEscapingEmitIsDeadLetteredAndTheRecordAcknowledged() {
    when(fetcher.fetch()).thenReturn(List.of("a", "b"));
    when(fetcher.isExhausted()).thenReturn(true);

    run(
        converter(
            (sourceRecord, emitter) -> {
              if (sourceRecord.equals("a")) {
                throw new IllegalStateException("bug");
              }
              emitter.accept(List.of(record(sourceRecord)));
            }));

    assertEquals(List.of("ack a", "commit", "accept b", "ack b", "commit"), events);
    assertEquals(
        List.of(new DlqEntry(null, "a", "java.lang.IllegalStateException: bug", NODE_ID)), dlq);
  }

  @Test
  void aNodeExecutionExceptionEscapingEmitNamesTheFailingNode() {
    when(fetcher.fetch()).thenReturn(List.of("a"));
    when(fetcher.isExhausted()).thenReturn(true);

    run(
        converter(
            (sourceRecord, emitter) -> {
              throw new NodeExecutionException("eval_1", "eval", "missing field", null);
            }));

    assertEquals(List.of("ack a", "commit"), events);
    assertEquals(
        List.of(
            new DlqEntry(
                null,
                "a",
                "io.fleak.zephflow.lib.commands.NodeExecutionException: missing field",
                "eval_1")),
        dlq);
  }

  @Test
  void aFailedDlqWriteEndsTheSourceWithNothingAcknowledged() {
    when(fetcher.fetch()).thenReturn(List.of("a", "b"), List.of("c"));
    // Bounded, so a regression that keeps going fails the assertions instead of looping.
    when(fetcher.isExhausted()).thenReturn(false, true);
    doThrow(new IllegalStateException("dlq closed"))
        .when(dlqWriter)
        .writeToDlq(anyLong(), any(), anyString(), anyString());

    run(
        converter(
            (sourceRecord, emitter) -> {
              if (sourceRecord.equals("a")) {
                throw new IllegalStateException("bug");
              }
              emitter.accept(List.of(record(sourceRecord)));
            }));

    assertEquals(List.of(), events);
    verify(fetcher, times(1)).fetch();
  }

  @Test
  void withoutADlqAFailedRecordIsDroppedAndAcknowledged() {
    when(fetcher.fetch()).thenReturn(List.of("a", "b"));
    when(fetcher.isExhausted()).thenReturn(true);
    dlqWriter = null;

    run(
        converter(
            (sourceRecord, emitter) -> {
              if (sourceRecord.equals("a")) {
                throw new IllegalStateException("bug");
              }
              emitter.accept(List.of(record(sourceRecord)));
            }));

    assertEquals(List.of("ack a", "commit", "accept b", "ack b", "commit"), events);
  }

  @Test
  void theDefaultConvertRefusesToRun() {
    TwoBatches converter = new TwoBatches();
    assertThrows(UnsupportedOperationException.class, () -> converter.convert("a", null));
  }

  @Test
  void aFailedCommitIsLoggedAndTheNextRecordIsHandled() {
    when(fetcher.fetch()).thenReturn(List.of("a", "b"));
    when(fetcher.isExhausted()).thenReturn(true);
    when(fetcher.committer())
        .thenReturn(
            () -> {
              events.add("commit");
              throw new IllegalStateException("commit failed");
            });

    run(new TwoBatches());

    assertEquals(
        List.of(
            "accept a-1",
            "accept a-2",
            "ack a",
            "commit",
            "accept b-1",
            "accept b-2",
            "ack b",
            "commit"),
        events);
    assertEquals(List.of(), dlq);
  }

  @Test
  void aConverterWithoutAcknowledgeIsStillCommitted() {
    when(fetcher.fetch()).thenReturn(List.of("a"));
    when(fetcher.isExhausted()).thenReturn(true);

    run(
        new BatchEmittingConverter<>() {
          @Override
          public void emit(
              String sourceRecord, SourceExecutionContext<?> context, Emitter emitter) {
            emitter.accept(List.of(record(sourceRecord)));
          }
        });

    assertEquals(List.of("accept a", "commit"), events);
  }

  private void stopAfterFirstBatch() {
    doAnswer(
            invocation -> {
              List<RecordFleakData> batch = invocation.getArgument(0);
              events.add("accept " + batch.getFirst().unwrap().get("id"));
              source.terminate();
              return null;
            })
        .when(acceptor)
        .accept(anyList());
  }

  private void run(BatchEmittingConverter<String> converter) {
    RawDataEncoder<String> encoder = SimpleSourceCommandBatchEmittingTest::encoded;
    source =
        new TestSource(
            new SourceExecutionContext<>(
                fetcher, converter, encoder, mock(), mock(), mock(), dlqWriter));
    source.initialize(mock(MetricClientProvider.class));
    source.execute("user", acceptor);
  }

  private static SerializedEvent encoded(String sourceRecord) {
    return new SerializedEvent(null, sourceRecord.getBytes(), null);
  }

  private static RecordFleakData record(String id) {
    return (RecordFleakData) FleakData.wrap(Map.of("id", id));
  }

  private class TestSource extends SimpleSourceCommand<String> {
    private final SourceExecutionContext<String> context;

    TestSource(SourceExecutionContext<String> context) {
      super(NODE_ID, mock(JobContext.class), mock(ConfigParser.class), mock(ConfigValidator.class));
      this.context = context;
    }

    @Override
    public String commandName() {
      return "test-source";
    }

    @Override
    public SourceType sourceType() {
      return SourceType.STREAMING;
    }

    @Override
    protected SourceExecutionContext<String> createExecutionContext(
        MetricClientProvider metricClientProvider,
        JobContext jobContext,
        CommandConfig commandConfig,
        String nodeId) {
      return context;
    }

    @Override
    RawDataSampler<String> createRawDataSampler(RawDataEncoder<String> encoder) {
      return sampler;
    }
  }
}
