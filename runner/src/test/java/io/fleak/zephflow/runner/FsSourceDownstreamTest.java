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

import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_TAG_ENV;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_TAG_SERVICE;
import static io.fleak.zephflow.runner.Constants.METRIC_NAME_PIPELINE_ERROR_EVENT;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ExecutionContext;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.api.ScalarCommand;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.NodeExecutionException;
import io.fleak.zephflow.lib.commands.fssource.FsSourceCommand;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendRegistry;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackend;
import io.fleak.zephflow.lib.commands.noop.NoopConfigParser;
import io.fleak.zephflow.lib.commands.noop.NoopConfigValidator;
import io.fleak.zephflow.runner.dag.Dag;
import io.fleak.zephflow.runner.dag.Edge;
import io.fleak.zephflow.runner.dag.Node;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * FsSource feeding a real {@link NoSourceDagRunner}, as {@link DagExecutor} wires them: a
 * downstream failure holds the file, which is read again next run, unless the runner drops it.
 */
class FsSourceDownstreamTest {

  private static final JobContext JOB_CONTEXT =
      JobContext.builder().metricTags(Map.of(METRIC_TAG_SERVICE, "t", METRIC_TAG_ENV, "t")).build();

  @TempDir Path root;
  private final List<Object> seen = new CopyOnWriteArrayList<>();
  private final AtomicLong errorEvents = new AtomicLong();

  @BeforeEach
  void registerBackend() throws Exception {
    FsBackendRegistry.unregister("file");
    FsBackendRegistry.register(new LocalFsBackend());
    Files.writeString(root.resolve("f_1.jsonl"), "{\"v\":\"a\"}\n{\"v\":\"b\"}\n");
    Files.writeString(root.resolve("f_2.jsonl"), "{\"v\":\"c\"}\n{\"v\":\"bad\"}\n");
  }

  @AfterEach
  void cleanup() {
    FsBackendRegistry.unregister("file");
  }

  @Test
  void aRecordFailingDownstreamWithADlqHoldsItsFileForTheNextRun() throws Exception {
    FsSourceCommand source = source();
    NoSourceDagRunner runner = runner(new Check("check", false), true);

    execute(source, runner);
    // Only the held file is left, and it fails again, so the run reads no file and fails.
    assertThrows(IllegalStateException.class, () -> execute(source, runner));

    assertEquals(List.of("a", "b", "c", "bad", "c", "bad"), seen);
  }

  @Test
  void aRecordFailingDownstreamWithoutADlqIsDroppedByTheRunnerAndTheFileIsDone() throws Exception {
    FsSourceCommand source = source();
    NoSourceDagRunner runner = runner(new Check("check", false), false);

    execute(source, runner);
    execute(source, runner);

    assertEquals(List.of("a", "b", "c", "bad"), seen);
    assertEquals(1L, errorEvents.get());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void aNodeThatFailsToInitializeHoldsTheFilesAndFailsTheRun(boolean useDlq) throws Exception {
    FsSourceCommand source = source();
    NoSourceDagRunner runner = runner(new Check("check", true), useDlq);

    assertThrows(IllegalStateException.class, () -> execute(source, runner));
    assertThrows(IllegalStateException.class, () -> execute(source, runner));
  }

  private FsSourceCommand source() {
    FsSourceCommand source = new FsSourceCommand("source", JOB_CONTEXT);
    source.parseAndValidateArg(
        Map.of(
            "backend", "file",
            "root", root.toUri().toString(),
            "fileNameRegex", "f_(?<ts>\\d+)\\.jsonl",
            "encodingType", "JSON_OBJECT_LINE"));
    source.initialize(new MetricClientProvider.NoopMetricClientProvider());
    return source;
  }

  private NoSourceDagRunner runner(Check check, boolean useDlq) {
    return new NoSourceDagRunner(
        List.of(Edge.builder().from("source").to("check").build()),
        new Dag<>(
            List.of(Node.<OperatorCommand>builder().id("check").nodeContent(check).build()),
            List.of()),
        new MetricClientProvider.NoopMetricClientProvider(),
        DagRunCounters.createPipelineCounters(new ErrorEventCounting(), Map.of()),
        useDlq);
  }

  /** Feeds the runner as {@link DagExecutor#executeDag} does. */
  private static void execute(FsSourceCommand source, NoSourceDagRunner runner) throws Exception {
    source.execute(
        "u",
        new SourceEventAcceptor() {
          @Override
          public void terminate() {}

          @Override
          public void accept(List<RecordFleakData> batch) {
            try {
              runner.run(batch, "u", new NoSourceDagRunner.DagRunConfig(true, false));
            } catch (NodeExecutionException e) {
              throw e;
            } catch (Exception e) {
              throw new NodeExecutionException("source", "unknown", e.getMessage(), e);
            }
          }
        });
  }

  /** Counts the runner's error events; every other metric is a no-op. */
  private final class ErrorEventCounting extends MetricClientProvider.NoopMetricClientProvider {
    @Override
    public FleakCounter counter(String name, Map<String, String> tags) {
      if (!METRIC_NAME_PIPELINE_ERROR_EVENT.equals(name)) {
        return super.counter(name, tags);
      }
      return new FleakCounter() {
        @Override
        public void increase(Map<String, String> additionalTags) {
          errorEvents.incrementAndGet();
        }

        @Override
        public void increase(long n, Map<String, String> additionalTags) {
          errorEvents.addAndGet(n);
        }
      };
    }
  }

  /** Fails each record whose {@code v} is "bad", or fails to initialize. */
  private final class Check extends ScalarCommand {
    private final boolean failInit;

    Check(String nodeId, boolean failInit) {
      super(nodeId, JOB_CONTEXT, new NoopConfigParser(), new NoopConfigValidator());
      this.failInit = failInit;
    }

    @Override
    public String commandName() {
      return "check";
    }

    @Override
    protected ExecutionContext createExecutionContext(
        MetricClientProvider metricClientProvider,
        JobContext jobContext,
        CommandConfig commandConfig,
        String nodeId) {
      if (failInit) {
        throw new IllegalStateException("credentials not found");
      }
      return () -> {};
    }

    @Override
    protected List<RecordFleakData> processOneEvent(
        RecordFleakData event, String callingUser, ExecutionContext context) {
      Object v = event.unwrap().get("v");
      seen.add(v);
      if ("bad".equals(v)) {
        throw new IllegalArgumentException("bad record");
      }
      return List.of(event);
    }
  }
}
