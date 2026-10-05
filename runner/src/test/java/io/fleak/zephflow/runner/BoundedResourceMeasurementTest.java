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

import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.execution.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.OperatorCommandRegistry;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition.DagNode;
import java.lang.management.ManagementFactory;
import java.util.*;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class BoundedResourceMeasurementTest {
  @Test
  void largeFiniteFanoutPreservesWholeSqlInputWithoutRetainingDebugOutputs() {
    int count = 25_000;
    var memory = ManagementFactory.getMemoryMXBean();
    var threads = ManagementFactory.getThreadMXBean();
    var allocation = threads instanceof com.sun.management.ThreadMXBean bean ? bean : null;
    long threadId = Thread.currentThread().threadId();
    long allocatedBefore = allocation == null ? -1 : allocation.getThreadAllocatedBytes(threadId);
    long heapBefore = memory.getHeapMemoryUsage().getUsed();
    long started = System.nanoTime();
    var records =
        IntStream.range(0, count)
            .mapToObj(
                index ->
                    (RecordFleakData)
                        FleakData.wrap(Map.of("id", index, "payload", "x".repeat(256))))
            .toList();
    var engine =
        new DagRunnerService(
            new DagCompiler(OperatorCommandRegistry.OPERATOR_COMMANDS),
            new MetricClientProvider.NoopMetricClientProvider());
    var graph =
        List.of(
            new DagNode("source", "noop", Map.of(), List.of("sql", "copy")),
            new DagNode(
                "sql", "sqleval", Map.of("sql", "SELECT count(*) AS total FROM events"), List.of()),
            new DagNode("copy", "noop", Map.of(), List.of()));
    var runner =
        engine.createForBoundedRun(
            graph,
            JobContext.builder()
                .metricTags(Map.of("service", "measurement", "env", "test"))
                .build(),
            new BoundedDefinition(
                Map.of("source", BoundedDefinition.BoundaryRole.SOURCE_OUTPUT), Set.of()));
    var observer = new CountingObserver();
    try {
      var result =
          runner.runBounded(
              List.of(
                  new BoundInput("source-data", "source", ExecutionObserver.Side.OUTPUT, records)),
              "user",
              observer,
              () -> {});
      assertEquals(0, result.failedInvocationCount());
      assertEquals(0, result.recordErrorCount());
      assertEquals(count, observer.counts.get("sql:INPUT"));
      assertEquals(count, observer.counts.get("copy:OUTPUT"));
      assertEquals(1, observer.sqlInvocations);
      assertEquals((double) count, observer.sqlTotal);
      runner.disposeBounded(CompletionDisposition.FINISHED);
    } finally {
      runner.disposeBounded(CompletionDisposition.ABORTED);
    }
    long duration = System.nanoTime() - started;
    long allocatedAfter = allocation == null ? -1 : allocation.getThreadAllocatedBytes(threadId);
    long allocated =
        allocatedBefore < 0 || allocatedAfter < 0 ? -1 : allocatedAfter - allocatedBefore;
    System.out.printf(
        Locale.ROOT,
        "FLE2774_RESOURCE_MEASUREMENT records=%d payloadCharactersPerRecord=256 durationMs=%.3f"
            + " heapBeforeBytes=%d heapAfterBytes=%d callerThreadAllocatedBytes=%d"
            + " maxHeapBytes=%d javaVersion=%s vm=%s%n",
        count,
        duration / 1_000_000.0,
        heapBefore,
        memory.getHeapMemoryUsage().getUsed(),
        allocated,
        Runtime.getRuntime().maxMemory(),
        System.getProperty("java.version"),
        System.getProperty("java.vm.name"));
  }

  private static final class CountingObserver implements ExecutionObserver {
    final Map<String, Integer> counts = new HashMap<>();
    int sqlInvocations;
    double sqlTotal;

    public void invocationStarted(long sequence, Invocation invocation) {
      if (invocation.nodeId().equals("sql") && invocation.phase() == Phase.PROCESS)
        sqlInvocations++;
    }

    public void records(
        long sequence, Invocation invocation, Side side, List<RecordFleakData> records) {
      counts.merge(invocation.nodeId() + ":" + side, records.size(), Integer::sum);
      if (invocation.nodeId().equals("sql") && side == Side.OUTPUT) {
        assertEquals(1, records.size());
        sqlTotal = ((Number) records.getFirst().unwrap().get("total")).doubleValue();
      }
    }

    public void recordErrors(long sequence, Invocation invocation, List<ErrorOutput> errors) {
      assertTrue(errors.isEmpty());
    }

    public void invocationFailed(long sequence, Invocation invocation, Throwable failure) {
      fail("Unexpected invocation failure", failure);
    }

    public void invocationFinished(
        long sequence, Invocation invocation, long input, long output, boolean complete) {
      assertTrue(complete);
    }

    public void effectStarted(long sequence, Invocation invocation, long effectId) {
      fail("Local graph must not have external effects");
    }

    public void effectFinished(
        long sequence, Invocation invocation, long effectId, EffectOutcome outcome) {
      fail("Local graph must not have external effects");
    }
  }
}
