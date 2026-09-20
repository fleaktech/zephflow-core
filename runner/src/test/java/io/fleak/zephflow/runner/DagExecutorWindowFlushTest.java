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

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.noop.NoopConfigParser;
import io.fleak.zephflow.lib.commands.noop.NoopConfigValidator;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition.DagNode;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises the real streaming path end to end: {@link DagExecutor#executeDag()} starts the flush
 * scheduler, and a time-triggered window fires on a background tick with no new input, routing its
 * rollup through the real terminate/close path to the sink.
 *
 * <p>The source stays alive until the sink observes a flush, so if the timer wiring were broken the
 * pipeline would never terminate and the test would fail on timeout rather than pass silently.
 */
class DagExecutorWindowFlushTest {

  private static final List<RecordFleakData> SINK_OUTPUT = new CopyOnWriteArrayList<>();
  private static final AtomicBoolean FLUSH_SEEN = new AtomicBoolean(false);

  private static final String SOURCE_CMD = "winFlushSource";
  private static final String WINDOW_CMD = "windowcount";
  private static final String SINK_CMD = "winFlushSink";

  @BeforeEach
  void reset() {
    SINK_OUTPUT.clear();
    FLUSH_SEEN.set(false);
  }

  private static RecordFleakData event(String host) {
    return (RecordFleakData) FleakData.wrap(Map.of("host", host));
  }

  private static RecordFleakData rollup(String host, long count) {
    return (RecordFleakData) FleakData.wrap(Map.of("host", host, "count", count));
  }

  @Test
  void executeDag_timeTriggeredWindowFlushesToSinkWithNoNewInput() {
    Map<String, CommandFactory> factories =
        Map.of(
            SOURCE_CMD, new SourceFactory(),
            WINDOW_CMD, new WindowFactory(),
            SINK_CMD, new SinkFactory());

    AdjacencyListDagDefinition dag =
        AdjacencyListDagDefinition.builder()
            .jobContext(JobContext.builder().build())
            .dag(
                List.of(
                    DagNode.builder().id("s").commandName(SOURCE_CMD).outputs(List.of("w")).build(),
                    DagNode.builder().id("w").commandName(WINDOW_CMD).outputs(List.of("k")).build(),
                    DagNode.builder().id("k").commandName(SINK_CMD).outputs(List.of()).build()))
            .build();

    JobConfig jobConfig =
        JobConfig.builder()
            .dagDefinition(dag)
            .jobId("win_flush_job")
            .environment("test_env")
            .service("test_service")
            .build();

    DagExecutor executor =
        DagExecutor.createDagExecutor(
            jobConfig, factories, new MetricClientProvider.NoopMetricClientProvider());

    assertTimeoutPreemptively(Duration.ofSeconds(20), executor::executeDag);

    SINK_OUTPUT.sort(Comparator.comparing(r -> r.getPayload().get("host").unwrap().toString()));
    assertEquals(List.of(rollup("a", 3), rollup("b", 2)), SINK_OUTPUT);
  }

  private static class SourceFactory extends CommandFactory {
    @Override
    public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
      return new WinFlushSource(
          nodeId, jobContext, new NoopConfigParser(), new NoopConfigValidator());
    }

    @Override
    public CommandType commandType() {
      return CommandType.SOURCE;
    }
  }

  private static class WindowFactory extends CommandFactory {
    @Override
    public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
      // period 200ms so it never fires inline on the batch, only on a scheduled tick
      return new WindowCountCommand(nodeId, 200, Long.MAX_VALUE);
    }

    @Override
    public CommandType commandType() {
      return CommandType.INTERMEDIATE_COMMAND;
    }
  }

  private static class SinkFactory extends CommandFactory {
    @Override
    public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
      return new WinFlushSink(
          nodeId, jobContext, new NoopConfigParser(), new NoopConfigValidator());
    }

    @Override
    public CommandType commandType() {
      return CommandType.SINK;
    }
  }

  /** Emits one batch, then stays alive (empty fetches) until a flush reaches the sink. */
  static class WinFlushSource extends SimpleSourceCommand<RecordFleakData> {
    WinFlushSource(
        String nodeId,
        JobContext jobContext,
        ConfigParser configParser,
        ConfigValidator configValidator) {
      super(nodeId, jobContext, configParser, configValidator);
    }

    @Override
    public SourceType sourceType() {
      return SourceType.STREAMING;
    }

    @Override
    public String commandName() {
      return SOURCE_CMD;
    }

    @Override
    protected SourceExecutionContext<RecordFleakData> createExecutionContext(
        MetricClientProvider metricClientProvider,
        JobContext jobContext,
        CommandConfig commandConfig,
        String nodeId) {
      Fetcher<RecordFleakData> fetcher =
          new Fetcher<>() {
            private final AtomicBoolean emitted = new AtomicBoolean(false);

            @Override
            public List<RecordFleakData> fetch() {
              if (emitted.getAndSet(true)) {
                return List.of();
              }
              return List.of(event("a"), event("a"), event("a"), event("b"), event("b"));
            }

            @Override
            public boolean isExhausted() {
              // Only finish once the background flush has delivered rollups to the sink.
              return FLUSH_SEEN.get();
            }

            @Override
            public void close() {}
          };
      RawDataConverter<RecordFleakData> converter =
          (sourceRecord, config) -> ConvertedResult.success(List.of(sourceRecord), sourceRecord);
      RawDataEncoder<RecordFleakData> encoder =
          sourceRecord -> new SerializedEvent(null, toJsonString(sourceRecord).getBytes(), null);
      return new SourceExecutionContext<>(
          fetcher,
          converter,
          encoder,
          metricClientProvider.counter("input_event_size_count", Map.of()),
          metricClientProvider.counter("input_event_count", Map.of()),
          metricClientProvider.counter("input_deser_err_count", Map.of()),
          null);
    }
  }

  /** Captures non-empty batches and signals the source to finish. */
  static class WinFlushSink extends ScalarSinkCommand {
    WinFlushSink(
        String nodeId,
        JobContext jobContext,
        ConfigParser configParser,
        ConfigValidator configValidator) {
      super(nodeId, jobContext, configParser, configValidator);
    }

    @Override
    public String commandName() {
      return SINK_CMD;
    }

    @Override
    protected ExecutionContext createExecutionContext(
        MetricClientProvider metricClientProvider,
        JobContext jobContext,
        CommandConfig commandConfig,
        String nodeId) {
      return () -> {};
    }

    @Override
    public SinkResult writeToSink(
        List<RecordFleakData> events, String callingUser, ExecutionContext context) {
      if (!events.isEmpty()) {
        SINK_OUTPUT.addAll(events);
        FLUSH_SEEN.set(true);
      }
      return new SinkResult(events.size(), events.size(), List.of());
    }
  }
}
