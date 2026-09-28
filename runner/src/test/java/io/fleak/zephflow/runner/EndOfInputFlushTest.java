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
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_TAG_ENV;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_TAG_SERVICE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.noop.NoopConfigParser;
import io.fleak.zephflow.lib.commands.noop.NoopConfigValidator;
import io.fleak.zephflow.lib.commands.sample.SampleCommandFactory;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.commands.throttle.ThrottleCommandFactory;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition.DagNode;
import io.fleak.zephflow.runner.dag.Dag;
import io.fleak.zephflow.runner.dag.Edge;
import io.fleak.zephflow.runner.dag.Node;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EndOfInputFlushTest {

  private static final String SOURCE = "source";
  private static final String SINK = "sink";
  private static final String USER = "u";
  private static final NoSourceDagRunner.DagRunConfig DEBUG_CONFIG =
      new NoSourceDagRunner.DagRunConfig(true, true);
  private static final JobContext JOB_CONTEXT =
      JobContext.builder().metricTags(Map.of(METRIC_TAG_SERVICE, "t", METRIC_TAG_ENV, "t")).build();

  private static final List<RecordFleakData> EXECUTOR_SINK_OUTPUT = new CopyOnWriteArrayList<>();

  @BeforeEach
  void reset() {
    EXECUTOR_SINK_OUTPUT.clear();
  }

  private static RecordFleakData event(String host) {
    return (RecordFleakData) FleakData.wrap(Map.of("host", host));
  }

  private static List<RecordFleakData> events(String host, int n) {
    List<RecordFleakData> out = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      out.add(event(host));
    }
    return out;
  }

  private static RecordFleakData sampled(String host, int groupSize) {
    return event(host).copyAndMerge(Map.of("__sampled__", FleakData.wrap(groupSize)));
  }

  private static RecordFleakData rollup(String host, long count) {
    return (RecordFleakData) FleakData.wrap(Map.of("host", host, "count", count));
  }

  private static RecordFleakData sinkResult(long count) {
    return (RecordFleakData) FleakData.wrap(Map.of("inputCount", count, "successCount", count));
  }

  private static OperatorCommand sample(String id, int rate) {
    ScalarCommand cmd = (ScalarCommand) new SampleCommandFactory().createCommand(id, JOB_CONTEXT);
    cmd.parseAndValidateArg(Map.of("rules", List.of(Map.of("sampleRate", rate))));
    return cmd;
  }

  private static Node<OperatorCommand> node(String id, OperatorCommand cmd) {
    return Node.<OperatorCommand>builder().id(id).nodeContent(cmd).build();
  }

  private static Edge edge(String from, String to) {
    return Edge.builder().from(from).to(to).build();
  }

  private static NoSourceDagRunner runner(
      List<Node<OperatorCommand>> nodes, List<Edge> edges, List<String> entryNodeIds) {
    return new NoSourceDagRunner(
        entryNodeIds.stream().map(id -> edge(SOURCE, id)).toList(),
        new Dag<>(nodes, edges),
        new MetricClientProvider.NoopMetricClientProvider(),
        DagRunCounters.createPipelineCounters(
            new MetricClientProvider.NoopMetricClientProvider(), Map.of()),
        false);
  }

  private static NoSourceDagRunner sampleToSink(int rate, CapturingSinkCommand sink) {
    return runner(
        List.of(node("s", sample("s", rate)), node(SINK, sink)),
        List.of(edge("s", SINK)),
        List.of("s"));
  }

  private static Map<String, List<RecordFleakData>> byUpstream(Object... keyAndRecords) {
    Map<String, List<RecordFleakData>> map = new LinkedHashMap<>();
    for (int i = 0; i < keyAndRecords.length; i += 2) {
      @SuppressWarnings("unchecked")
      List<RecordFleakData> records = (List<RecordFleakData>) keyAndRecords[i + 1];
      map.put((String) keyAndRecords[i], records);
    }
    return map;
  }

  private static List<RecordFleakData> flattened(DagResult result, String nodeId) {
    return result.getOutputByStep().get(nodeId).values().stream().flatMap(List::stream).toList();
  }

  @Test
  void sample_endOfInput_emitsIncompleteGroupAfterRegularOutput() {
    CapturingSinkCommand sink = new CapturingSinkCommand(SINK, null);
    NoSourceDagRunner runner = sampleToSink(2, sink);

    DagResult result = runner.run(events("a", 5), USER, DEBUG_CONFIG, true);

    List<RecordFleakData> expected = List.of(sampled("a", 2), sampled("a", 2), sampled("a", 1));
    assertEquals(byUpstream(SOURCE, expected), result.getOutputByStep().get("s"));
    assertEquals(expected, flattened(result, "s"));
    assertEquals(expected, sink.captured);
    runner.terminate();
  }

  @Test
  void sample_withoutEndOfInput_holdsIncompleteGroup() {
    CapturingSinkCommand sink = new CapturingSinkCommand(SINK, null);
    NoSourceDagRunner runner = sampleToSink(2, sink);

    DagResult result = runner.run(events("a", 5), USER, DEBUG_CONFIG);

    assertEquals(List.of(sampled("a", 2), sampled("a", 2)), flattened(result, "s"));
    assertEquals(List.of(sampled("a", 2), sampled("a", 2)), sink.captured);
  }

  @Test
  void sample_terminate_emitsIncompleteGroupToOpenSink() {
    CapturingSinkCommand sink = new CapturingSinkCommand(SINK, null);
    NoSourceDagRunner runner = sampleToSink(10, sink);

    runner.run(events("a", 3), USER, DEBUG_CONFIG);
    assertTrue(sink.captured.isEmpty());
    runner.terminate();

    assertEquals(List.of(sampled("a", 3)), sink.captured);
  }

  @Test
  void sample_endOfInputThenTerminate_emitsOnce() {
    CapturingSinkCommand sink = new CapturingSinkCommand(SINK, null);
    NoSourceDagRunner runner = sampleToSink(10, sink);

    runner.run(events("a", 3), USER, DEBUG_CONFIG, true);
    runner.terminate();

    assertEquals(List.of(sampled("a", 3)), sink.captured);
  }

  @Test
  void sample_nothingPending_endOfInputChangesNothing() {
    DagResult withEnd =
        sampleToSink(2, new CapturingSinkCommand(SINK, null))
            .run(events("a", 4), USER, DEBUG_CONFIG, true);
    DagResult withoutEnd =
        sampleToSink(2, new CapturingSinkCommand(SINK, null))
            .run(events("a", 4), USER, DEBUG_CONFIG, false);

    assertEquals(withoutEnd, withEnd);
  }

  @Test
  void endOfInput_recordedUnderFirstUpstream() {
    CountingPassThrough f1 = new CountingPassThrough("f1");
    CountingPassThrough f2 = new CountingPassThrough("f2");
    NoSourceDagRunner runner =
        runner(
            List.of(node("f1", f1), node("f2", f2), node("s", sample("s", 4))),
            List.of(edge("f1", "s"), edge("f2", "s")),
            List.of("f1", "f2"));

    DagResult result = runner.run(events("a", 3), USER, DEBUG_CONFIG, true);

    assertEquals(
        byUpstream("f1", List.of(sampled("a", 2)), "f2", List.of(sampled("a", 4))),
        result.getOutputByStep().get("s"));
  }

  @Test
  void finishInput_onlyFlushes() {
    CountingPassThrough filter = new CountingPassThrough("f");
    CapturingSinkCommand sink = new CapturingSinkCommand(SINK, null);
    NoSourceDagRunner runner =
        runner(
            List.of(node("f", filter), node("s", sample("s", 2)), node(SINK, sink)),
            List.of(edge("f", "s"), edge("s", SINK)),
            List.of("f"));

    runner.run(events("a", 2), USER, DEBUG_CONFIG);
    runner.run(events("a", 1), USER, DEBUG_CONFIG);
    DagResult last = runner.finishInput(USER, DEBUG_CONFIG);

    assertEquals(2, filter.processCalls.get());
    assertEquals(
        Map.of(
            "s",
            byUpstream("f", List.of(sampled("a", 1))),
            SINK,
            byUpstream("s", List.of(sinkResult(1)))),
        last.getOutputByStep());
    assertEquals(List.of(sampled("a", 2), sampled("a", 1)), sink.captured);
  }

  @Test
  void emptyRunWithEndOfInput_stillTraversesNodes() {
    CountingPassThrough filter = new CountingPassThrough("f");
    NoSourceDagRunner runner =
        runner(
            List.of(node("f", filter), node("s", sample("s", 2))),
            List.of(edge("f", "s")),
            List.of("f"));

    DagResult result = runner.run(List.of(), USER, DEBUG_CONFIG, true);

    assertEquals(1, filter.processCalls.get());
    assertEquals(
        Map.of("f", byUpstream(SOURCE, List.of()), "s", byUpstream("f", List.of())),
        result.getOutputByStep());
  }

  @Test
  void finishInput_withNothingToFlushRunsNoNode() {
    CountingPassThrough filter = new CountingPassThrough("f");
    NoSourceDagRunner withoutFlushable =
        runner(List.of(node("f", filter)), List.of(), List.of("f"));
    NoSourceDagRunner neverRun =
        runner(List.of(node("s", sample("s", 2))), List.of(), List.of("s"));

    assertEquals(new DagResult(), withoutFlushable.finishInput(USER, DEBUG_CONFIG));
    assertEquals(new DagResult(), neverRun.finishInput(USER, DEBUG_CONFIG));
    assertEquals(0, filter.processCalls.get());
    assertFalse(filter.isInitialized());
  }

  @Test
  void terminate_neverRunRunnerEmitsNothing() {
    CapturingSinkCommand sink = new CapturingSinkCommand(SINK, null);
    NoSourceDagRunner runner = sampleToSink(2, sink);

    runner.terminate();

    assertTrue(sink.captured.isEmpty());
  }

  @Test
  void terminate_failingFlushStillFlushesOthersAndCloses() {
    CapturingSinkCommand sink = new CapturingSinkCommand(SINK, null);
    NoSourceDagRunner runner =
        runner(
            List.of(
                node("bad", new FailingFlushCommand("bad")),
                node("s", sample("s", 10)),
                node(SINK, sink)),
            List.of(edge("s", SINK)),
            List.of("bad", "s"));
    runner.run(events("a", 3), USER, DEBUG_CONFIG);

    runner.terminate();

    assertEquals(List.of(sampled("a", 3)), sink.captured);
  }

  @Test
  void noEndOfInputNodes_endOfInputChangesNothing() {
    List<RecordFleakData> input = List.of(event("a"), event("b"));
    DagResult withEnd =
        runner(List.of(node("f", new CountingPassThrough("f"))), List.of(), List.of("f"))
            .run(input, USER, DEBUG_CONFIG, true);
    DagResult withoutEnd =
        runner(List.of(node("f", new CountingPassThrough("f"))), List.of(), List.of("f"))
            .run(input, USER, DEBUG_CONFIG, false);

    assertEquals(withoutEnd, withEnd);
  }

  @Test
  void window_firesOnlyAtEndOfInput() {
    CapturingSinkCommand sink = new CapturingSinkCommand(SINK, null);
    NoSourceDagRunner runner =
        runner(
            List.of(
                node("w", new WindowCountCommand("w", 3_600_000, Long.MAX_VALUE)),
                node(SINK, sink)),
            List.of(edge("w", SINK)),
            List.of("w"));
    List<RecordFleakData> input = List.of(event("a"), event("a"), event("b"));

    DagResult first = runner.run(input, USER, DEBUG_CONFIG, false);
    assertEquals(byUpstream(SOURCE, List.of()), first.getOutputByStep().get("w"));
    assertTrue(sink.captured.isEmpty());

    DagResult last = runner.finishInput(USER, DEBUG_CONFIG);
    List<RecordFleakData> expected = List.of(rollup("a", 2), rollup("b", 1));
    assertEquals(byUpstream(SOURCE, expected), last.getOutputByStep().get("w"));
    assertEquals(expected, sink.captured);
  }

  @Test
  void chunkedInput_windowSpansAllChunks() {
    CapturingSinkCommand sink = new CapturingSinkCommand(SINK, null);
    NoSourceDagRunner runner =
        runner(
            List.of(
                node("w", new WindowCountCommand("w", 3_600_000, Long.MAX_VALUE)),
                node(SINK, sink)),
            List.of(edge("w", SINK)),
            List.of("w"));

    runner.run(events("a", 2), USER, DEBUG_CONFIG);
    runner.run(events("a", 3), USER, DEBUG_CONFIG);
    runner.finishInput(USER, DEBUG_CONFIG);

    assertEquals(List.of(rollup("a", 5)), sink.captured);
  }

  private static NoSourceDagRunner chainedWindows(CapturingSinkCommand sink) {
    // w2 is listed before w1 although w1 feeds it, so a flush in list order would miss w1's rollups
    return runner(
        List.of(
            node("w2", new WindowCountCommand("w2", 3_600_000, Long.MAX_VALUE)),
            node("w1", new WindowCountCommand("w1", 3_600_000, Long.MAX_VALUE)),
            node(SINK, sink)),
        List.of(edge("w1", "w2"), edge("w2", SINK)),
        List.of("w1"));
  }

  @Test
  void chainedWindows_endOfInputFlushesUpstreamFirst() {
    CapturingSinkCommand sink = new CapturingSinkCommand(SINK, null);
    NoSourceDagRunner runner = chainedWindows(sink);

    runner.run(List.of(event("a"), event("a"), event("b")), USER, DEBUG_CONFIG, true);

    assertEquals(List.of(rollup("a", 1), rollup("b", 1)), sink.captured);
  }

  @Test
  void chainedWindows_terminateFlushesUpstreamFirst() {
    CapturingSinkCommand sink = new CapturingSinkCommand(SINK, null);
    NoSourceDagRunner runner = chainedWindows(sink);

    runner.run(List.of(event("a"), event("a"), event("b")), USER, DEBUG_CONFIG);
    runner.terminate();

    assertEquals(List.of(rollup("a", 1), rollup("b", 1)), sink.captured);
  }

  @Test
  void failingFlush_isRecordedAndOtherNodesStillFlush() {
    NoSourceDagRunner runner =
        runner(
            List.of(node("bad", new FailingFlushCommand("bad")), node("s", sample("s", 10))),
            List.of(),
            List.of("bad", "s"));

    DagResult result = runner.run(events("a", 3), USER, DEBUG_CONFIG, true);

    assertEquals(
        new DagResult.NodeFailure("bad", "failingflush", "flush failed"), result.getFirstFailure());
    assertEquals(byUpstream(SOURCE, List.of(sampled("a", 3))), result.getOutputByStep().get("s"));
  }

  @Test
  void createForTestRun_allowsSampleAndThrottle() {
    Map<String, CommandFactory> factories =
        Map.of("sample", new SampleCommandFactory(), "throttle", new ThrottleCommandFactory());
    DagRunnerService service =
        new DagRunnerService(
            new DagCompiler(factories), new MetricClientProvider.NoopMetricClientProvider());
    List<DagNode> dag =
        List.of(
            DagNode.builder()
                .id("s")
                .commandName("sample")
                .config(Map.of("rules", List.of(Map.of("sampleRate", 3))))
                .outputs(List.of("t"))
                .build(),
            DagNode.builder()
                .id("t")
                .commandName("throttle")
                .config(Map.of("keyExpression", "$.host", "numToAllow", 1))
                .outputs(List.of())
                .build());

    NoSourceDagRunner runner = service.createForTestRun(dag, JOB_CONTEXT);
    DagResult result = runner.run(events("a", 4), USER, DEBUG_CONFIG, true);

    assertEquals(
        Map.of(
            "s",
            byUpstream("sync_input", List.of(sampled("a", 3), sampled("a", 1))),
            "t",
            byUpstream("s", List.of(sampled("a", 3)))),
        result.getOutputByStep());
    runner.terminate();
  }

  @Test
  void createForTestRun_allowsWindowedNodes() {
    DagRunnerService service =
        new DagRunnerService(
            new DagCompiler(Map.of("windowcount", new WindowCountFactory())),
            new MetricClientProvider.NoopMetricClientProvider());
    List<DagNode> dag =
        List.of(DagNode.builder().id("w").commandName("windowcount").outputs(List.of()).build());

    NoSourceDagRunner runner = service.createForTestRun(dag, JOB_CONTEXT);
    DagResult result =
        runner.run(List.of(event("a"), event("a"), event("b")), USER, DEBUG_CONFIG, true);

    assertEquals(
        byUpstream("sync_input", List.of(rollup("a", 2), rollup("b", 1))),
        result.getOutputByStep().get("w"));
    runner.terminate();
  }

  @Test
  void throttle_passesFirstEventPerKey() {
    ScalarCommand throttle =
        (ScalarCommand) new ThrottleCommandFactory().createCommand("t", JOB_CONTEXT);
    throttle.parseAndValidateArg(Map.of("keyExpression", "$.host", "numToAllow", 1));
    NoSourceDagRunner runner = runner(List.of(node("t", throttle)), List.of(), List.of("t"));

    DagResult result =
        runner.run(List.of(event("a"), event("a"), event("b")), USER, DEBUG_CONFIG, true);

    assertEquals(
        byUpstream(SOURCE, List.of(event("a"), event("b"))), result.getOutputByStep().get("t"));
  }

  @Test
  void createForTestRun_stillRejectsSourceNodes() {
    DagRunnerService service =
        new DagRunnerService(
            new DagCompiler(Map.of("finite", new FiniteSourceFactory())),
            new MetricClientProvider.NoopMetricClientProvider());
    List<DagNode> dag =
        List.of(DagNode.builder().id("src").commandName("finite").outputs(List.of()).build());

    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> service.createForTestRun(dag, JOB_CONTEXT));
    assertEquals(
        "api backend doesn't support source function node in the dag. Found:finite",
        ex.getMessage());
  }

  @Test
  void dagExecutor_finiteSourceEnd_emitsIncompleteSampleGroup() {
    Map<String, CommandFactory> factories =
        Map.of(
            "finite", new FiniteSourceFactory(),
            "sample", new SampleCommandFactory(),
            "capture", new ExecutorSinkFactory());
    AdjacencyListDagDefinition dag =
        AdjacencyListDagDefinition.builder()
            .jobContext(JOB_CONTEXT)
            .dag(
                List.of(
                    DagNode.builder().id("src").commandName("finite").outputs(List.of("s")).build(),
                    DagNode.builder()
                        .id("s")
                        .commandName("sample")
                        .config(Map.of("rules", List.of(Map.of("sampleRate", 10))))
                        .outputs(List.of("k"))
                        .build(),
                    DagNode.builder().id("k").commandName("capture").outputs(List.of()).build()))
            .build();
    JobConfig jobConfig =
        JobConfig.builder()
            .dagDefinition(dag)
            .jobId("eoi_job")
            .environment("test_env")
            .service("test_service")
            .build();
    DagExecutor executor =
        DagExecutor.createDagExecutor(
            jobConfig, factories, new MetricClientProvider.NoopMetricClientProvider());

    assertTimeoutPreemptively(Duration.ofSeconds(20), executor::executeDag);

    assertEquals(List.of(sampled("a", 3)), EXECUTOR_SINK_OUTPUT);
  }

  @Test
  void dagResult_keepsUpstreamInsertionOrder() {
    DagResult result = new DagResult();
    DagRunCounters counters =
        DagRunCounters.createPipelineCounters(
            new MetricClientProvider.NoopMetricClientProvider(), Map.of());
    // "z" hashes after "a" in a HashMap, so only insertion order yields z's record first
    result.handleNodeResult(
        Map.of(), "n", "z", "c", DEBUG_CONFIG, List.of(event("z")), List.of(), counters);
    result.handleNodeResult(
        Map.of(), "n", "a", "c", DEBUG_CONFIG, List.of(event("a")), List.of(), counters);

    assertEquals(List.of(event("z"), event("a")), flattened(result, "n"));
  }

  static class CountingPassThrough extends ScalarCommand {
    final AtomicInteger processCalls = new AtomicInteger();

    CountingPassThrough(String nodeId) {
      super(nodeId, null, new NoopConfigParser(), new NoopConfigValidator());
    }

    @Override
    public String commandName() {
      return "countingpassthrough";
    }

    @Override
    public ProcessResult process(
        List<RecordFleakData> events, String callingUser, ExecutionContext context) {
      processCalls.incrementAndGet();
      return super.process(events, callingUser, context);
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
    protected List<RecordFleakData> processOneEvent(
        RecordFleakData event, String callingUser, ExecutionContext context) {
      return List.of(event);
    }
  }

  static class FailingFlushCommand extends ScalarCommand implements EndOfInputFlushable {
    FailingFlushCommand(String nodeId) {
      super(nodeId, null, new NoopConfigParser(), new NoopConfigValidator());
    }

    @Override
    public String commandName() {
      return "failingflush";
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
    protected List<RecordFleakData> processOneEvent(
        RecordFleakData event, String callingUser, ExecutionContext context) {
      return List.of();
    }

    @Override
    public List<RecordFleakData> flushAtEndOfInput(String callingUser, ExecutionContext context) {
      throw new IllegalStateException("flush failed");
    }
  }

  private static class WindowCountFactory extends CommandFactory {
    @Override
    public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
      return new WindowCountCommand(nodeId, 3_600_000, Long.MAX_VALUE);
    }

    @Override
    public CommandType commandType() {
      return CommandType.INTERMEDIATE_COMMAND;
    }
  }

  private static class FiniteSourceFactory extends CommandFactory {
    @Override
    public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
      return new FiniteSource(nodeId, jobContext);
    }

    @Override
    public CommandType commandType() {
      return CommandType.SOURCE;
    }
  }

  private static class ExecutorSinkFactory extends CommandFactory {
    @Override
    public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
      return new ExecutorSink(nodeId, jobContext);
    }

    @Override
    public CommandType commandType() {
      return CommandType.SINK;
    }
  }

  /** Emits one batch of three matching events, then reports itself exhausted. */
  static class FiniteSource extends SimpleSourceCommand<RecordFleakData> {
    FiniteSource(String nodeId, JobContext jobContext) {
      super(nodeId, jobContext, new NoopConfigParser(), new NoopConfigValidator());
    }

    @Override
    public SourceType sourceType() {
      return SourceType.BATCH;
    }

    @Override
    public String commandName() {
      return "finite";
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
              return emitted.getAndSet(true) ? List.of() : events("a", 3);
            }

            @Override
            public boolean isExhausted() {
              return emitted.get();
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

  static class ExecutorSink extends ScalarSinkCommand {
    ExecutorSink(String nodeId, JobContext jobContext) {
      super(nodeId, jobContext, new NoopConfigParser(), new NoopConfigValidator());
    }

    @Override
    public String commandName() {
      return "capture";
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
      assertNotNull(events);
      EXECUTOR_SINK_OUTPUT.addAll(events);
      return new SinkResult(events.size(), events.size(), List.of());
    }
  }
}
