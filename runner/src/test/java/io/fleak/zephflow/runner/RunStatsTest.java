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
package io.fleak.zephflow.runner;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.api.SourceCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.commands.sink.SinkStoreForward;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.commands.source.SourceBacklog;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import java.util.List;
import org.junit.jupiter.api.Test;

class RunStatsTest {

  @Test
  void reportsTheSourceBacklogInRecords() {
    RunStats stats =
        RunStats.collect(
            "kafka",
            source(SourceBacklog.records(150L, 6)),
            List.of(sink(false)),
            RunStats.DrainState.RUNNING,
            false,
            null);

    assertEquals(
        new RunStats(
            List.of(new RunStats.SourceStats("kafka", 150L, "records", 6, null)),
            RunStats.BufferState.EMPTY,
            RunStats.DrainState.RUNNING,
            false),
        stats);
  }

  @Test
  void reportsKinesisTimeBehindInMillis() {
    RunStats stats =
        RunStats.collect(
            "kinesis",
            source(SourceBacklog.millisBehind(7_000L)),
            List.of(),
            RunStats.DrainState.RUNNING,
            false,
            null);

    assertEquals(
        new RunStats.SourceStats("kinesis", 7_000L, "millis", null, null),
        stats.sources().getFirst());
  }

  @Test
  void aSourceWhoseFetcherCannotReadItsBacklogReportsUnknown() {
    RunStats stats =
        RunStats.collect(
            "pubsub", source(null), List.of(), RunStats.DrainState.RUNNING, false, null);

    assertEquals(
        new RunStats.SourceStats("pubsub", null, "records", null, null),
        stats.sources().getFirst());
  }

  @Test
  void aSourceWithoutAFetcherReportsUnknown() {
    SourceCommand syslog = mock(SourceCommand.class);
    when(syslog.isInitialized()).thenReturn(true);

    RunStats stats =
        RunStats.collect("syslog", syslog, List.of(), RunStats.DrainState.RUNNING, false, null);

    assertEquals(
        new RunStats.SourceStats("syslog", null, "records", null, null),
        stats.sources().getFirst());
    assertEquals(false, stats.supportsDrain());
  }

  @Test
  void reportsHowManyRecordsTheSourceHasConsumed() {
    RunStats stats =
        RunStats.collect(
            "kafka",
            source(SourceBacklog.records(150L, 6)),
            List.of(),
            RunStats.DrainState.RUNNING,
            false,
            42L);

    assertEquals(
        new RunStats.SourceStats("kafka", 150L, "records", 6, 42L), stats.sources().getFirst());
  }

  @Test
  void aSinkBufferingToDiskMakesTheBufferPending() {
    RunStats stats =
        RunStats.collect(
            "kafka",
            source(SourceBacklog.UNKNOWN),
            List.of(sink(false), sink(true), notASink()),
            RunStats.DrainState.RUNNING,
            false,
            null);

    assertEquals(RunStats.BufferState.PENDING, stats.bufferState());
  }

  @Test
  void recordsLeftInABufferMakeItPending() {
    SinkStoreForward storeForward = mock(SinkStoreForward.class);
    when(storeForward.pendingRecords()).thenReturn(3L);

    assertEquals(RunStats.BufferState.PENDING, RunStats.bufferState(List.of(sink(storeForward))));
  }

  @Test
  void aWriterBlockedOnAFullBufferIsWaitingForSpace() {
    SinkStoreForward storeForward = mock(SinkStoreForward.class);
    when(storeForward.isBuffering()).thenReturn(true);
    when(storeForward.isWaitingForSpace()).thenReturn(true);

    assertEquals(
        RunStats.BufferState.WAITING_FOR_SPACE,
        RunStats.bufferState(List.of(sink(false), sink(storeForward))));
  }

  @Test
  void carriesTheDrainStateAndWhetherTheSourceCanDrain() {
    RunStats stats =
        RunStats.collect(
            "kafka",
            source(SourceBacklog.UNKNOWN),
            List.of(),
            RunStats.DrainState.DRAINING,
            true,
            null);

    assertEquals(RunStats.DrainState.DRAINING, stats.drainState());
    assertTrue(stats.supportsDrain());
  }

  @Test
  void uninitializedCommandsAreSkipped() {
    OperatorCommand sink = mock(OperatorCommand.class);
    SourceCommand source = mock(SourceCommand.class);

    RunStats stats =
        RunStats.collect("src", source, List.of(sink), RunStats.DrainState.RUNNING, false, null);

    assertEquals(RunStats.BufferState.EMPTY, stats.bufferState());
    assertEquals(null, stats.sources().getFirst().backlog());
  }

  /** A null backlog means a fetcher that keeps the interface default. */
  private static SourceCommand source(SourceBacklog backlog) {
    Fetcher<Object> fetcher =
        backlog == null
            ? new Fetcher<>() {
              @Override
              public List<Object> fetch() {
                return List.of();
              }

              @Override
              public void close() {}
            }
            : new Fetcher<>() {
              @Override
              public List<Object> fetch() {
                return List.of();
              }

              @Override
              public SourceBacklog backlog() {
                return backlog;
              }

              @Override
              public void close() {}
            };
    SourceCommand source = mock(SourceCommand.class);
    when(source.isInitialized()).thenReturn(true);
    when(source.getExecutionContext())
        .thenReturn(new SourceExecutionContext<>(fetcher, null, null, null, null, null, null));
    return source;
  }

  private static OperatorCommand sink(boolean buffering) {
    SinkStoreForward storeForward = mock(SinkStoreForward.class);
    when(storeForward.isBuffering()).thenReturn(buffering);
    return sink(storeForward);
  }

  private static OperatorCommand sink(SinkStoreForward storeForward) {
    OperatorCommand sink = mock(OperatorCommand.class);
    when(sink.isInitialized()).thenReturn(true);
    when(sink.getExecutionContext())
        .thenReturn(
            new SinkExecutionContext<>(null, null, storeForward, null, null, null, null, null));
    return sink;
  }

  private static OperatorCommand notASink() {
    OperatorCommand command = mock(OperatorCommand.class);
    when(command.isInitialized()).thenReturn(true);
    return command;
  }
}
