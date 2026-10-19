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

  private final List<String> events = new ArrayList<>();
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
  }

  @BeforeEach
  void setUp() {
    fetcher = mock();
    when(fetcher.committer()).thenReturn(() -> events.add("commit"));
    dlqWriter = mock(DlqWriter.class);
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
  void eachRecordIsCommittedAfterItsLastBatchAndNotBefore() {
    when(fetcher.fetch()).thenReturn(List.of("a", "b"));
    when(fetcher.isExhausted()).thenReturn(true);

    run(new TwoBatches());

    assertEquals(
        List.of("accept a-1", "accept a-2", "commit", "accept b-1", "accept b-2", "commit"),
        events);
    verify(sampler).maybeSample(List.of("a", "b"));
    verify(dlqWriter, never()).writeToDlq(anyLong(), any(), any(), any());
  }

  @Test
  void aFetcherThatReportsExhaustionStillGetsItsLastRecordsHandledAndCommitted() {
    when(fetcher.fetch()).thenReturn(List.of(), List.of("a"));
    when(fetcher.isExhausted()).thenReturn(false, true);

    run(new TwoBatches());

    assertEquals(List.of("accept a-1", "accept a-2", "commit"), events);
  }

  @Test
  void aStopBetweenBatchesLeavesTheRecordUncommittedAndAFailureAfterItIsNotDeadLettered() {
    when(fetcher.fetch()).thenReturn(List.of("a", "b"));
    when(fetcher.isExhausted()).thenReturn(true);
    doAnswer(
            invocation -> {
              events.add(
                  "accept "
                      + ((List<RecordFleakData>) invocation.getArgument(0))
                          .getFirst()
                          .unwrap()
                          .get("id"));
              source.terminate();
              return null;
            })
        .when(acceptor)
        .accept(anyList());

    run(new TwoBatches());

    assertEquals(List.of("accept a-1"), events);
    verify(dlqWriter, never()).writeToDlq(anyLong(), any(), any(), any());
  }

  @Test
  void anExceptionEscapingEmitIsDeadLetteredWithTheRecordAndTheRecordIsCommitted() {
    when(fetcher.fetch()).thenReturn(List.of("a", "b"));
    when(fetcher.isExhausted()).thenReturn(true);
    IllegalStateException bug = new IllegalStateException("bug");

    run(
        (sourceRecord, context, emitter) -> {
          if (sourceRecord.equals("a")) {
            throw bug;
          }
          emitter.accept(List.of(record(sourceRecord)));
        });

    assertEquals(List.of("commit", "accept b", "commit"), events);
    verify(dlqWriter)
        .writeToDlq(
            anyLong(),
            argThat(value -> new String(value.value()).equals("a")),
            startsWith("java.lang.IllegalStateException: bug"),
            eq(NODE_ID));
    verify(dlqWriter, times(1)).writeToDlq(anyLong(), any(), any(), any());
  }

  @Test
  void aNodeExecutionExceptionEscapingEmitNamesTheFailingNode() {
    when(fetcher.fetch()).thenReturn(List.of("a"));
    when(fetcher.isExhausted()).thenReturn(true);

    run(
        (sourceRecord, context, emitter) -> {
          throw new NodeExecutionException("eval_1", "eval", "missing field", null);
        });

    verify(dlqWriter)
        .writeToDlq(
            anyLong(),
            argThat(value -> new String(value.value()).equals("a")),
            anyString(),
            eq("eval_1"));
    assertEquals(List.of("commit"), events);
  }

  @Test
  void aFailedDlqWriteDoesNotStopTheSource() {
    when(fetcher.fetch()).thenReturn(List.of("a", "b"));
    when(fetcher.isExhausted()).thenReturn(true);
    doThrow(new IllegalStateException("dlq down"))
        .when(dlqWriter)
        .writeToDlq(anyLong(), any(), anyString(), anyString());

    run(
        (sourceRecord, context, emitter) -> {
          if (sourceRecord.equals("a")) {
            throw new IllegalStateException("bug");
          }
          emitter.accept(List.of(record(sourceRecord)));
        });

    assertEquals(List.of("commit", "accept b", "commit"), events);
  }

  @Test
  void convertIsNotCalledForABatchEmittingConverter() {
    TwoBatches converter = new TwoBatches();
    assertThrows(UnsupportedOperationException.class, () -> converter.convert("a", null));
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
