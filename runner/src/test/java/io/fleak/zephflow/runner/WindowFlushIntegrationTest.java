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

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.noop.NoopConfigParser;
import io.fleak.zephflow.lib.commands.noop.NoopConfigValidator;
import io.fleak.zephflow.lib.windowing.KeyedWindowManager;
import io.fleak.zephflow.lib.windowing.WindowFunction;
import io.fleak.zephflow.lib.windowing.WindowTrigger;
import io.fleak.zephflow.runner.dag.Dag;
import io.fleak.zephflow.runner.dag.Edge;
import io.fleak.zephflow.runner.dag.Node;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/** End-to-end coverage of the runner injecting window-flush output into the DAG. */
class WindowFlushIntegrationTest {

  private static final String SOURCE = "source";
  private static final String WIN = "win";
  private static final String SINK = "sink";
  private static final String USER = "u";
  private static final NoSourceDagRunner.DagRunConfig RUN_CONFIG =
      new NoSourceDagRunner.DagRunConfig(false, false);

  private static RecordFleakData event(String host) {
    return (RecordFleakData) FleakData.wrap(Map.of("host", host));
  }

  private static RecordFleakData rollup(String host, long count) {
    return (RecordFleakData) FleakData.wrap(Map.of("host", host, "count", count));
  }

  private static NoSourceDagRunner buildRunner(
      WindowCountCommand winCmd, CapturingSinkCommand sink) {
    Node<OperatorCommand> winNode =
        Node.<OperatorCommand>builder().id(WIN).nodeContent(winCmd).build();
    Node<OperatorCommand> sinkNode =
        Node.<OperatorCommand>builder().id(SINK).nodeContent(sink).build();
    Dag<OperatorCommand> dag =
        new Dag<>(List.of(winNode, sinkNode), List.of(Edge.builder().from(WIN).to(SINK).build()));
    return new NoSourceDagRunner(
        List.of(Edge.builder().from(SOURCE).to(WIN).build()),
        dag,
        mock(MetricClientProvider.class),
        mock(DagRunCounters.class),
        false);
  }

  @Test
  void terminate_finalFlushRoutesRemainingWindowsToSink() {
    // Never fires on its own, so windows only leave via the final flush on terminate().
    WindowCountCommand winCmd = new WindowCountCommand(WIN, Long.MAX_VALUE, Long.MAX_VALUE);
    CapturingSinkCommand sink = new CapturingSinkCommand(SINK, null);
    NoSourceDagRunner runner = buildRunner(winCmd, sink);

    runner.run(
        List.of(event("a"), event("a"), event("a"), event("b"), event("b")), USER, RUN_CONFIG);
    assertTrue(sink.captured.isEmpty(), "nothing should be emitted before the window fires");

    runner.terminate();

    // Access-ordered emission: "a" before "b". Full-list compare also catches duplicate emission,
    // and the sink throws if written after close, so this proves the final flush precedes close.
    assertEquals(List.of(rollup("a", 3), rollup("b", 2)), sink.captured);
  }

  @Test
  void scheduler_periodicFlushRoutesDueWindowsToSink() throws InterruptedException {
    // Time trigger with a small period; fires on a scheduled tick, not during process().
    WindowCountCommand winCmd = new WindowCountCommand(WIN, 100, Long.MAX_VALUE);
    CountDownLatch latch = new CountDownLatch(2); // two rollups: host a and host b
    CapturingSinkCommand sink = new CapturingSinkCommand(SINK, latch);
    NoSourceDagRunner runner = buildRunner(winCmd, sink);

    try {
      runner.run(
          List.of(event("a"), event("a"), event("a"), event("b"), event("b")), USER, RUN_CONFIG);
      // Period is 100ms and the batch is processed in well under that, so neither window fires
      // inline during run(): everything below must arrive via the background flush thread.
      assertTrue(sink.captured.isEmpty(), "windows must not fire inline before a tick");
      runner.startFlushScheduler(USER, 20);
      assertTrue(
          latch.await(10, TimeUnit.SECONDS), "scheduled flush did not reach the sink in time");
    } finally {
      runner.terminate();
    }

    assertEquals(List.of(rollup("a", 3), rollup("b", 2)), sink.captured);
  }

  @Test
  void concurrentRunAndFlush_noEventLostOrDoubleCounted() {
    // Aggressive: 5ms window, 1ms tick, so the flush thread interleaves heavily with run(). The
    // pipeline lock must serialize them; if it didn't, counts would be lost or double-counted.
    WindowCountCommand winCmd = new WindowCountCommand(WIN, 5, Long.MAX_VALUE);
    CapturingSinkCommand sink = new CapturingSinkCommand(SINK, null);
    NoSourceDagRunner runner = buildRunner(winCmd, sink);

    long expected = 0;
    try {
      runner.startFlushScheduler(USER, 1);
      for (int i = 0; i < 2000; i++) {
        runner.run(List.of(event("a"), event("b"), event("a")), USER, RUN_CONFIG);
        expected += 3;
      }
    } finally {
      runner.terminate(); // final flush drains the last windows
    }

    long total =
        sink.captured.stream()
            .mapToLong(r -> ((Number) r.getPayload().get("count").unwrap()).longValue())
            .sum();
    assertEquals(expected, total, "every event must be counted exactly once across all flushes");
  }

  @Test
  void startFlushScheduler_afterTerminate_isNoOp() throws InterruptedException {
    WindowCountCommand winCmd = new WindowCountCommand(WIN, 5, Long.MAX_VALUE);
    CapturingSinkCommand sink = new CapturingSinkCommand(SINK, null);
    NoSourceDagRunner runner = buildRunner(winCmd, sink);

    runner.terminate(); // terminated=true; commands closed

    long before = flushThreadCount();
    runner.startFlushScheduler(USER, 5); // must not resurrect a scheduler
    Thread.sleep(100);
    assertEquals(before, flushThreadCount(), "no flush thread may start after terminate");
  }

  @Test
  void terminate_isIdempotent_doesNotReflushOrThrow() {
    WindowCountCommand winCmd = new WindowCountCommand(WIN, Long.MAX_VALUE, Long.MAX_VALUE);
    CapturingSinkCommand sink = new CapturingSinkCommand(SINK, null);
    NoSourceDagRunner runner = buildRunner(winCmd, sink);

    runner.run(List.of(event("a"), event("a"), event("a")), USER, RUN_CONFIG);
    runner.terminate(); // final flush emits a:3
    runner.terminate(); // second call: guarded, no re-flush, no write to closed sink

    assertEquals(List.of(rollup("a", 3)), sink.captured);
  }

  private static long flushThreadCount() {
    return Thread.getAllStackTraces().keySet().stream()
        .filter(Thread::isAlive)
        .filter(t -> "zephflow-window-flush".equals(t.getName()))
        .count();
  }
}

/** Groups events by their {@code host} field, counts them, and emits {@code {host, count}}. */
class WindowCountCommand extends ScalarCommand implements WindowFlushable {

  private final long periodMs;
  private final long countLimit;

  WindowCountCommand(String nodeId, long periodMs, long countLimit) {
    super(nodeId, null, new NoopConfigParser(), new NoopConfigValidator());
    this.periodMs = periodMs;
    this.countLimit = countLimit;
  }

  @Override
  public String commandName() {
    return "windowcount";
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    return new Ctx(
        KeyedWindowManager.<Long>builder()
            .windowFunction(new CountingFunction())
            .trigger(
                WindowTrigger.any(WindowTrigger.count(countLimit), WindowTrigger.time(periodMs)))
            .build());
  }

  @Override
  protected List<RecordFleakData> processOneEvent(
      RecordFleakData event, String callingUser, ExecutionContext context) {
    return ((Ctx) context).manager.onEvent(hostOf(event), event, System.currentTimeMillis());
  }

  @Override
  public List<RecordFleakData> flush(
      String callingUser, ExecutionContext context, boolean finalFlush) {
    return ((Ctx) context).manager.onTick(System.currentTimeMillis(), finalFlush);
  }

  private static String hostOf(RecordFleakData event) {
    FleakData host = event.getPayload().get("host");
    return host == null ? "unknown" : host.unwrap().toString();
  }

  private record Ctx(KeyedWindowManager<Long> manager) implements ExecutionContext {
    @Override
    public void close() {}
  }

  private static final class CountingFunction implements WindowFunction<Long> {
    @Override
    public Long init() {
      return 0L;
    }

    @Override
    public Long add(Long acc, RecordFleakData event) {
      return acc + 1;
    }

    @Override
    public List<RecordFleakData> emit(String key, Long acc) {
      return List.of((RecordFleakData) FleakData.wrap(Map.of("host", key, "count", acc)));
    }
  }
}

/**
 * Sink that records every event it receives and counts down a latch per record. Throws if written
 * after its context is closed, so a final flush that ran after command shutdown would be caught.
 */
class CapturingSinkCommand extends ScalarSinkCommand {

  final List<RecordFleakData> captured = new CopyOnWriteArrayList<>();
  private final CountDownLatch latch;

  CapturingSinkCommand(String nodeId, CountDownLatch latch) {
    super(nodeId, null, null, null);
    this.latch = latch;
  }

  @Override
  public String commandName() {
    return "capturingsink";
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    return new SinkCtx();
  }

  @Override
  public SinkResult writeToSink(
      List<RecordFleakData> events, String callingUser, ExecutionContext context) {
    if (((SinkCtx) context).closed) {
      throw new IllegalStateException("sink written after close");
    }
    List<RecordFleakData> accepted = new ArrayList<>(events);
    captured.addAll(accepted);
    if (latch != null) {
      accepted.forEach(e -> latch.countDown());
    }
    return new SinkResult(accepted.size(), accepted.size(), List.of());
  }

  private static final class SinkCtx implements ExecutionContext {
    private volatile boolean closed = false;

    @Override
    public void close() {
      closed = true;
    }
  }
}
