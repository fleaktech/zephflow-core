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

import static io.fleak.zephflow.lib.utils.JsonUtils.fromJsonString;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.noop.NoopConfigParser;
import io.fleak.zephflow.lib.commands.noop.NoopConfigValidator;
import io.fleak.zephflow.lib.commands.sink.ChronicleStoreForward;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.commands.sink.SinkStoreForward;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition.DagNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DrainedRunTest {

  @TempDir Path dir;

  @Test
  void aDrainRequestEndsTheRunAsDrained() throws Exception {
    Path ready = dir.resolve("ready");
    Path sinkFile = dir.resolve("sink.jsonl");
    Path statsFile = dir.resolve("stats.json");
    Path controlFile = dir.resolve("control");
    DagExecutor executor = executor(ready, sinkFile);

    CompletableFuture<Boolean> drained =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return executor.executeDagWithRunFiles(statsFile, controlFile);
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (!Files.exists(ready) && System.nanoTime() < deadline) {
      Thread.sleep(50);
    }
    assertTrue(Files.exists(ready), "the pipeline never consumed its input");
    assertEquals(true, fromJsonString(Files.readString(statsFile), Map.class).get("supportsDrain"));

    Files.writeString(controlFile, "drain");

    assertTrue(drained.get(30, TimeUnit.SECONDS));
    assertEquals(3, Files.readAllLines(sinkFile).size());
  }

  @Test
  void anIdleRunDoesNotDrainBeforeASinkBacklogFromAPreviousRunIsDelivered() throws Exception {
    Path store = Files.createDirectories(dir.resolve("store"));
    ChronicleStoreForward previousRun = storeForward(store);
    previousRun.start(
        records -> {
          throw new IOException("sink down");
        });
    previousRun.offer(List.of(event("a"), event("b"), event("c")));
    await(previousRun::isBuffering);
    previousRun.close();

    Path ready = dir.resolve("ready");
    Path controlFile = dir.resolve("control");
    Files.writeString(controlFile, "drain");
    CountDownLatch sinkUp = new CountDownLatch(1);
    List<RecordFleakData> replayed = new CopyOnWriteArrayList<>();
    DagExecutor executor =
        executor(
            SigtermFlushTest.factory(CommandType.SOURCE, id -> new IdleSource(id, ready)),
            SigtermFlushTest.factory(
                CommandType.SINK,
                id ->
                    new BacklogSink(
                        id,
                        store,
                        records -> {
                          sinkUp.await();
                          replayed.addAll(records);
                        })));

    CompletableFuture<Boolean> drained = runAsync(executor, null, controlFile);
    await(() -> Files.exists(ready));
    assertThrows(TimeoutException.class, () -> drained.get(5, TimeUnit.SECONDS));

    sinkUp.countDown();
    assertTrue(drained.get(30, TimeUnit.SECONDS));
    assertEquals(List.of("a", "b", "c"), replayed.stream().map(r -> r.unwrap().get("v")).toList());
  }

  @Test
  void aSinkThatCannotStartYetNeitherFailsTheRunNorLetsItDrain() throws Exception {
    Path ready = dir.resolve("ready");
    Path sinkFile = dir.resolve("sink.jsonl");
    Path statsFile = dir.resolve("stats.json");
    Path controlFile = dir.resolve("control");
    AtomicBoolean sinkReachable = new AtomicBoolean();
    Queue<RecordFleakData> input = new ConcurrentLinkedQueue<>();
    DagExecutor executor =
        executor(
            SigtermFlushTest.factory(CommandType.SOURCE, id -> new QueueSource(id, ready, input)),
            SigtermFlushTest.factory(
                CommandType.SINK,
                id ->
                    new SigtermFlushTest.FileSink(id, sinkFile) {
                      @Override
                      protected ExecutionContext createExecutionContext(
                          MetricClientProvider metricClientProvider,
                          JobContext jobContext,
                          CommandConfig commandConfig,
                          String nodeId) {
                        if (!sinkReachable.get()) {
                          throw new IllegalStateException("sink unreachable");
                        }
                        return super.createExecutionContext(
                            metricClientProvider, jobContext, commandConfig, nodeId);
                      }
                    }));

    CompletableFuture<Boolean> run = runAsync(executor, statsFile, controlFile);
    await(() -> Files.exists(ready) || run.isDone());
    assertFalse(run.isDone(), "the run ended at startup");
    assertEquals(
        "PENDING", fromJsonString(Files.readString(statsFile), Map.class).get("bufferState"));

    Files.writeString(controlFile, "drain");
    assertThrows(TimeoutException.class, () -> run.get(3, TimeUnit.SECONDS));

    Files.writeString(controlFile, "resume");
    sinkReachable.set(true);
    input.add(event("a"));
    await(() -> Files.exists(sinkFile));
    assertEquals(1, Files.readAllLines(sinkFile).size());

    Files.writeString(controlFile, "drain");
    assertTrue(run.get(30, TimeUnit.SECONDS));
  }

  private static CompletableFuture<Boolean> runAsync(
      DagExecutor executor, Path statsFile, Path controlFile) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            return executor.executeDagWithRunFiles(statsFile, controlFile);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });
  }

  private static void await(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (!condition.getAsBoolean()) {
      assertTrue(System.nanoTime() < deadline, "timed out");
      Thread.sleep(20);
    }
  }

  private static RecordFleakData event(String value) {
    return (RecordFleakData) FleakData.wrap(Map.of("v", value));
  }

  private static ChronicleStoreForward storeForward(Path store) {
    return new ChronicleStoreForward(
        new ChronicleStoreForward.Config(store, 1_000_000, 50, 10, "k"),
        t -> t instanceof IOException,
        null,
        null,
        null);
  }

  private static DagExecutor executor(Path ready, Path sinkFile) {
    return executor(
        SigtermFlushTest.factory(
            CommandType.SOURCE, id -> new SigtermFlushTest.EndlessSource(id, ready)),
        SigtermFlushTest.factory(
            CommandType.SINK, id -> new SigtermFlushTest.FileSink(id, sinkFile)));
  }

  private static DagExecutor executor(CommandFactory source, CommandFactory sink) {
    Map<String, CommandFactory> factories = Map.of("src", source, "sink", sink);
    AdjacencyListDagDefinition dag =
        AdjacencyListDagDefinition.builder()
            .jobContext(JobContext.builder().build())
            .dag(
                List.of(
                    DagNode.builder().id("s").commandName("src").outputs(List.of("k")).build(),
                    DagNode.builder().id("k").commandName("sink").outputs(List.of()).build()))
            .build();
    JobConfig jobConfig =
        JobConfig.builder()
            .dagDefinition(dag)
            .jobId("drain_job")
            .environment("test_env")
            .service("test_service")
            .build();
    return DagExecutor.createDagExecutor(
        jobConfig, factories, new MetricClientProvider.NoopMetricClientProvider());
  }

  /** Fetches nothing, ever. */
  static class IdleSource extends SimpleSourceCommand<RecordFleakData> {
    private final Path ready;

    IdleSource(String nodeId, Path ready) {
      super(
          nodeId, JobContext.builder().build(), new NoopConfigParser(), new NoopConfigValidator());
      this.ready = ready;
    }

    @Override
    public SourceType sourceType() {
      return SourceType.STREAMING;
    }

    @Override
    public String commandName() {
      return "src";
    }

    @Override
    protected SourceExecutionContext<RecordFleakData> createExecutionContext(
        MetricClientProvider metricClientProvider,
        JobContext jobContext,
        CommandConfig commandConfig,
        String nodeId) {
      Fetcher<RecordFleakData> fetcher =
          new Fetcher<>() {
            @Override
            public List<RecordFleakData> fetch() {
              try {
                Files.writeString(ready, "");
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
              return List.of();
            }

            @Override
            public void close() {}
          };
      return new SourceExecutionContext<>(
          fetcher,
          (sourceRecord, config) -> ConvertedResult.success(List.of(sourceRecord), sourceRecord),
          sourceRecord -> null,
          metricClientProvider.counter("input_event_size_count", Map.of()),
          metricClientProvider.counter("input_event_count", Map.of()),
          metricClientProvider.counter("input_deser_err_count", Map.of()),
          null);
    }
  }

  /** Emits whatever is queued in {@code input}, one record per fetch. */
  static class QueueSource extends SimpleSourceCommand<RecordFleakData> {
    private final Path ready;
    private final Queue<RecordFleakData> input;

    QueueSource(String nodeId, Path ready, Queue<RecordFleakData> input) {
      super(
          nodeId, JobContext.builder().build(), new NoopConfigParser(), new NoopConfigValidator());
      this.ready = ready;
      this.input = input;
    }

    @Override
    public SourceType sourceType() {
      return SourceType.STREAMING;
    }

    @Override
    public String commandName() {
      return "src";
    }

    @Override
    protected SourceExecutionContext<RecordFleakData> createExecutionContext(
        MetricClientProvider metricClientProvider,
        JobContext jobContext,
        CommandConfig commandConfig,
        String nodeId) {
      Fetcher<RecordFleakData> fetcher =
          new Fetcher<>() {
            @Override
            public List<RecordFleakData> fetch() {
              RecordFleakData next = input.poll();
              if (next != null) {
                return List.of(next);
              }
              try {
                Files.writeString(ready, "");
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
              return List.of();
            }

            @Override
            public void close() {}
          };
      return new SourceExecutionContext<>(
          fetcher,
          (sourceRecord, config) -> ConvertedResult.success(List.of(sourceRecord), sourceRecord),
          sourceRecord -> null,
          metricClientProvider.counter("input_event_size_count", Map.of()),
          metricClientProvider.counter("input_event_count", Map.of()),
          metricClientProvider.counter("input_deser_err_count", Map.of()),
          null);
    }
  }

  /**
   * A sink whose store-and-forward buffer resumes whatever an earlier run left in {@code store}.
   */
  static class BacklogSink extends ScalarSinkCommand {
    private final Path store;
    private final SinkStoreForward.ReplayTarget replay;

    BacklogSink(String nodeId, Path store, SinkStoreForward.ReplayTarget replay) {
      super(
          nodeId, JobContext.builder().build(), new NoopConfigParser(), new NoopConfigValidator());
      this.store = store;
      this.replay = replay;
    }

    @Override
    public String commandName() {
      return "sink";
    }

    @Override
    protected ExecutionContext createExecutionContext(
        MetricClientProvider metricClientProvider,
        JobContext jobContext,
        CommandConfig commandConfig,
        String nodeId) {
      ChronicleStoreForward storeForward = storeForward(store);
      storeForward.start(replay);
      return new SinkExecutionContext<>(null, null, storeForward, null, null, null, null, null);
    }

    @Override
    public SinkResult writeToSink(
        List<RecordFleakData> events, String callingUser, ExecutionContext context) {
      return new SinkResult(events.size(), events.size(), List.of());
    }
  }
}
