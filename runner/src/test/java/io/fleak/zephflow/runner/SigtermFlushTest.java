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
import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.noop.NoopConfigParser;
import io.fleak.zephflow.lib.commands.noop.NoopConfigValidator;
import io.fleak.zephflow.lib.commands.sample.SampleCommandFactory;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition.DagNode;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Stops a running pipeline the way Grid does, with SIGTERM to its JVM, and checks that the pending
 * output of a sample node (an incomplete group) still reaches the sink. A child JVM runs {@link
 * Pipeline}: the source emits one batch and then never ends, so only the shutdown flush can emit
 * the sampled record.
 */
class SigtermFlushTest {

  private static final Map<String, Object> SAMPLED = Map.of("host", "a", "__sampled__", 3);

  @TempDir Path dir;

  @ParameterizedTest
  @ValueSource(strings = {"stoppable", "unstoppable"})
  void sigterm_flushesPendingOutputToSink(String sourceKind) throws Exception {
    assumeFalse(System.getProperty("os.name").startsWith("Windows"), "needs SIGTERM");
    Path ready = dir.resolve("ready");
    Path sinkFile = dir.resolve("sink.jsonl");
    Path logFile = dir.resolve("child.log");
    Process child =
        new ProcessBuilder(
                ProcessHandle.current().info().command().orElse("java"),
                "-cp",
                System.getProperty("java.class.path"),
                Pipeline.class.getName(),
                sourceKind,
                ready.toString(),
                sinkFile.toString())
            .redirectErrorStream(true)
            .redirectOutput(logFile.toFile())
            .start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
      while (!Files.exists(ready) && child.isAlive() && System.nanoTime() < deadline) {
        Thread.sleep(50);
      }
      assertTrue(Files.exists(ready), "pipeline never consumed its input:\n" + log(logFile));

      child.destroy(); // SIGTERM

      assertTrue(child.waitFor(15, TimeUnit.SECONDS), "child did not exit:\n" + log(logFile));
      assertEquals(List.of(SAMPLED), records(sinkFile), log(logFile));
    } finally {
      child.destroyForcibly();
    }
  }

  private static List<Map<?, ?>> records(Path file) throws IOException {
    if (!Files.exists(file)) return List.of();
    return Files.readAllLines(file).stream()
        .<Map<?, ?>>map(line -> fromJsonString(line, Map.class))
        .toList();
  }

  private static String log(Path file) throws IOException {
    return Files.exists(file) ? Files.readString(file) : "";
  }

  /** Runs source -> sample (1 in 10) -> file sink until killed. */
  public static class Pipeline {
    public static void main(String[] args) throws Exception {
      String sourceKind = args[0];
      Path ready = Path.of(args[1]);
      Path sinkFile = Path.of(args[2]);
      Map<String, CommandFactory> factories =
          Map.of(
              "src",
              factory(
                  CommandType.SOURCE,
                  id ->
                      sourceKind.equals("stoppable")
                          ? new EndlessSource(id, ready)
                          : new UnstoppableSource(id, ready)),
              "sample",
              new SampleCommandFactory(),
              "sink",
              factory(CommandType.SINK, id -> new FileSink(id, sinkFile)));
      AdjacencyListDagDefinition dag =
          AdjacencyListDagDefinition.builder()
              .jobContext(JobContext.builder().build())
              .dag(
                  List.of(
                      DagNode.builder().id("s").commandName("src").outputs(List.of("x")).build(),
                      DagNode.builder()
                          .id("x")
                          .commandName("sample")
                          .config(Map.of("rules", List.of(Map.of("sampleRate", 10))))
                          .outputs(List.of("k"))
                          .build(),
                      DagNode.builder().id("k").commandName("sink").outputs(List.of()).build()))
              .build();
      JobConfig jobConfig =
          JobConfig.builder()
              .dagDefinition(dag)
              .jobId("sigterm_job")
              .environment("test_env")
              .service("test_service")
              .build();
      DagExecutor.createDagExecutor(
              jobConfig, factories, new MetricClientProvider.NoopMetricClientProvider())
          .executeDag();
    }
  }

  private static CommandFactory factory(CommandType type, Function<String, OperatorCommand> make) {
    return new CommandFactory() {
      @Override
      public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
        return make.apply(nodeId);
      }

      @Override
      public CommandType commandType() {
        return type;
      }
    };
  }

  private static List<RecordFleakData> threeEvents() {
    RecordFleakData event = (RecordFleakData) FleakData.wrap(Map.of("host", "a"));
    return List.of(event, event, event);
  }

  private static void touch(Path file) {
    try {
      Files.writeString(file, "");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /** Emits one batch, then fetches nothing forever; stops on request. */
  static class EndlessSource extends SimpleSourceCommand<RecordFleakData> {
    private final Path ready;

    EndlessSource(String nodeId, Path ready) {
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
            private int fetches;

            @Override
            public List<RecordFleakData> fetch() {
              if (fetches++ == 0) {
                return threeEvents();
              }
              // The first batch has been through the whole DAG by the second fetch.
              touch(ready);
              return List.of();
            }

            @Override
            public boolean isExhausted() {
              return false;
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

  /** Emits one batch, then blocks forever; cannot be asked to stop. */
  static class UnstoppableSource extends SourceCommand {
    private final Path ready;

    UnstoppableSource(String nodeId, Path ready) {
      super(
          nodeId, JobContext.builder().build(), new NoopConfigParser(), new NoopConfigValidator());
      this.ready = ready;
    }

    @Override
    public void execute(String callingUser, SourceEventAcceptor sourceEventAcceptor)
        throws Exception {
      sourceEventAcceptor.accept(threeEvents());
      touch(ready);
      Thread.sleep(Long.MAX_VALUE);
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
    protected ExecutionContext createExecutionContext(
        MetricClientProvider metricClientProvider,
        JobContext jobContext,
        CommandConfig commandConfig,
        String nodeId) {
      return () -> {};
    }
  }

  /** Appends each record to a file as a JSON line, so the parent can read what reached the sink. */
  static class FileSink extends ScalarSinkCommand {
    private final File file;

    FileSink(String nodeId, Path file) {
      super(
          nodeId, JobContext.builder().build(), new NoopConfigParser(), new NoopConfigValidator());
      this.file = file.toFile();
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
      return () -> {};
    }

    @Override
    public SinkResult writeToSink(
        List<RecordFleakData> events, String callingUser, ExecutionContext context) {
      try {
        for (RecordFleakData event : events) {
          Files.writeString(
              file.toPath(),
              toJsonString(event.unwrap()) + "\n",
              StandardOpenOption.CREATE,
              StandardOpenOption.APPEND);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return new SinkResult(events.size(), events.size(), List.of());
    }
  }
}
