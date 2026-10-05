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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.delta.kernel.Operation;
import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.execution.*;
import io.fleak.zephflow.api.execution.ExecutionObserver.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.*;
import io.fleak.zephflow.lib.commands.deltalakesink.*;
import io.fleak.zephflow.lib.commands.sink.*;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition.DagNode;
import java.nio.file.Path;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

class BoundedDeltaCheckpointTest {
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void actualCheckpointHasBackgroundIdentityAndCannotChangeCommitReceipt(boolean failCheckpoint) {
    Engine engine = DefaultEngine.create(new Configuration());
    Table table = Table.forPath(engine, directory.toString());
    table
        .createTransactionBuilder(engine, "bounded test", Operation.CREATE_TABLE)
        .withSchema(engine, new StructType().add("value", DoubleType.DOUBLE, false))
        .withTableProperties(engine, Map.of("delta.checkpointInterval", "1"))
        .build(engine)
        .commit(engine, empty());
    CommandFactory factory =
        new CommandFactory() {
          public CommandType commandType() {
            return CommandType.SINK;
          }

          public OperatorCommand createCommand(String nodeId, JobContext job) {
            return new SimpleSinkCommand<Map<String, Object>>(
                nodeId, job, config -> new CommandConfig() {}, (config, node, context) -> {}) {
              protected int batchSize() {
                return 10;
              }

              public String commandName() {
                return "deltalakesink";
              }

              protected ExecutionContext createExecutionContext(
                  MetricClientProvider metrics,
                  JobContext context,
                  CommandConfig config,
                  String node) {
                var counter = new MetricClientProvider.NoopMetricClientProvider.NoopFleakCounter();
                var writer =
                    new DeltaLakeWriter(
                        DeltaLakeSinkDto.Config.builder()
                            .tablePath(directory.toString())
                            .batchSize(10)
                            .enableAutoCheckpoint(true)
                            .avroSchema(
                                Map.of(
                                    "type",
                                    "record",
                                    "name",
                                    "Record",
                                    "fields",
                                    List.of(Map.of("name", "value", "type", "double"))))
                            .build(),
                        context,
                        null,
                        counter,
                        counter,
                        counter,
                        node);
                writer.initialize();
                if (failCheckpoint) {
                  try {
                    var field = DeltaLakeWriter.class.getDeclaredField("engine");
                    field.setAccessible(true);
                    Engine instrumented = spy((Engine) field.get(writer));
                    doAnswer(
                            call -> {
                              if (Thread.currentThread().getName().contains("Checkpoint"))
                                throw new IllegalStateException("checkpoint-test-private-token");
                              return call.callRealMethod();
                            })
                        .when(instrumented)
                        .getParquetHandler();
                    field.set(writer, instrumented);
                  } catch (ReflectiveOperationException failure) {
                    throw new IllegalStateException(failure);
                  }
                }
                return new SinkExecutionContext<>(
                    writer,
                    new DeltaLakeMessageProcessor(),
                    counter,
                    counter,
                    counter,
                    counter,
                    counter);
              }
            };
          }
        };
    var service =
        new DagRunnerService(
            new DagCompiler(Map.of("deltalakesink", factory)),
            new MetricClientProvider.NoopMetricClientProvider());
    var runner =
        service.createForBoundedRun(
            List.of(new DagNode("sink", "deltalakesink", Map.of(), new ArrayList<>())),
            JobContext.builder().metricTags(Map.of("service", "test", "env", "test")).build(),
            new BoundedDefinition(Map.of(), Set.of("sink")));
    var observer = mock(ExecutionObserver.class);
    runner.runBounded(
        List.of(
            new BoundInput(
                "sink",
                "sink",
                Side.INPUT,
                List.of((RecordFleakData) FleakData.wrap(Map.of("value", 1))))),
        "user",
        observer,
        () -> {});
    runner.disposeBounded(CompletionDisposition.FINISHED);
    var invocations = ArgumentCaptor.forClass(Invocation.class);
    var outcomes = ArgumentCaptor.forClass(EffectOutcome.class);
    verify(observer, atLeastOnce())
        .effectFinished(anyLong(), invocations.capture(), anyLong(), outcomes.capture());
    int background = 0;
    for (int index = 0; index < invocations.getAllValues().size(); index++) {
      var invocation = invocations.getAllValues().get(index);
      var outcome = outcomes.getAllValues().get(index);
      if (invocation.phase() == Phase.PROCESS) {
        assertEquals(1L, outcome.acknowledgedCount());
        assertEquals(EffectOutcome.Delivery.ACKNOWLEDGED, outcome.delivery());
      } else if (invocation.phase() == Phase.BACKGROUND) {
        background++;
        assertNull(outcome.attemptedCount());
        assertEquals(
            failCheckpoint ? EffectOutcome.Delivery.UNKNOWN : EffectOutcome.Delivery.ACKNOWLEDGED,
            outcome.delivery());
      }
    }
    assertEquals(1, background);
    verify(observer, times(failCheckpoint ? 1 : 0))
        .invocationFailed(anyLong(), argThat(value -> value.phase() == Phase.BACKGROUND), any());
    assertEquals(1, table.getLatestSnapshot(engine).getVersion());
    assertEquals(
        !failCheckpoint,
        java.nio.file.Files.exists(directory.resolve("_delta_log/_last_checkpoint")));
  }

  private static <T> CloseableIterable<T> empty() {
    return new CloseableIterable<>() {
      public CloseableIterator<T> iterator() {
        return new CloseableIterator<>() {
          public boolean hasNext() {
            return false;
          }

          public T next() {
            throw new NoSuchElementException();
          }

          public void close() {}
        };
      }

      public void close() {}
    };
  }
}
