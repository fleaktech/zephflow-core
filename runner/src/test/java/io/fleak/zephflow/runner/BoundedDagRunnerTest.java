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

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.execution.*;
import io.fleak.zephflow.api.execution.ExecutionObserver.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.OperatorCommandRegistry;
import io.fleak.zephflow.runner.dag.AdjacencyListDagDefinition.DagNode;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class BoundedDagRunnerTest {
  private final DagRunnerService service =
      new DagRunnerService(
          new DagCompiler(OperatorCommandRegistry.OPERATOR_COMMANDS),
          new MetricClientProvider.NoopMetricClientProvider());

  @Test
  void fullPopulationIsOneSqlBatchAndSourceIdentityIsObserved() {
    Capture capture = new Capture();
    var runner =
        runner(
            List.of(
                node("source", "noop", Map.of(), "sql"),
                node("sql", "sqleval", Map.of("sql", "SELECT count(*) AS total FROM events"))),
            Map.of("source", BoundedDefinition.BoundaryRole.SOURCE_OUTPUT));
    var summary =
        runner.runBounded(List.of(input("source", Side.OUTPUT, 350)), "user", capture, () -> {});
    runner.disposeBounded(CompletionDisposition.FINISHED);
    assertEquals(List.of(350.0), capture.values("sql", "total"));
    assertEquals(350, capture.records("source", Side.OUTPUT).size());
    assertEquals(0, summary.failedInvocationCount());
    assertFalse(
        capture.started.stream()
            .anyMatch(i -> i.nodeId().equals("source") && i.phase() == Phase.INIT));
    assertEquals(
        IntStream.rangeClosed(1, capture.sequences.size()).mapToObj(i -> (long) i).toList(),
        capture.sequences);
  }

  @Test
  void separateParentsRemainSeparateSqlInvocationsInAuthoredOrder() {
    Capture capture = new Capture();
    var runner =
        runner(
            List.of(
                node("a", "noop", Map.of(), "sql"),
                node("b", "noop", Map.of(), "sql"),
                node("sql", "sqleval", Map.of("sql", "SELECT count(*) AS total FROM events"))),
            Map.of(
                "a",
                BoundedDefinition.BoundaryRole.SOURCE_OUTPUT,
                "b",
                BoundedDefinition.BoundaryRole.SOURCE_OUTPUT));
    runner.runBounded(
        List.of(input("b", Side.OUTPUT, 3), input("a", Side.OUTPUT, 2)), "user", capture, () -> {});
    runner.disposeBounded(CompletionDisposition.FINISHED);
    assertEquals(List.of(2.0, 3.0), capture.values("sql", "total"));
    assertEquals(
        List.of("a", "b"),
        capture.started.stream()
            .filter(i -> i.nodeId().equals("sql") && i.phase() == Phase.PROCESS)
            .map(Invocation::upstreamNodeId)
            .toList());
  }

  @Test
  void bothBindingsShareSamplingStateAndDisposeDoesNotFlushAgain() {
    Capture capture = new Capture();
    var runner =
        runner(
            List.of(
                node("a", "noop", Map.of(), "sample"),
                node("b", "noop", Map.of(), "sample"),
                node("sample", "sample", Map.of("rules", List.of(Map.of("sampleRate", 3))))),
            Map.of(
                "a",
                BoundedDefinition.BoundaryRole.SOURCE_OUTPUT,
                "b",
                BoundedDefinition.BoundaryRole.SOURCE_OUTPUT));
    runner.runBounded(
        List.of(input("a", Side.OUTPUT, 2), input("b", Side.OUTPUT, 2)), "user", capture, () -> {});
    assertEquals(List.of(3.0, 1.0), capture.values("sample", "__sampled__"));
    int count = capture.records("sample", Side.OUTPUT).size();
    runner.disposeBounded(CompletionDisposition.FINISHED);
    runner.disposeBounded(CompletionDisposition.FINISHED);
    assertEquals(count, capture.records("sample", Side.OUTPUT).size());
    assertEquals(
        1,
        capture.started.stream()
            .filter(i -> i.nodeId().equals("sample") && i.phase() == Phase.END_OF_INPUT)
            .count());
    assertTrue(
        capture.started.stream()
            .filter(i -> i.phase() == Phase.END_OF_INPUT)
            .allMatch(i -> i.upstreamNodeId() == null));
  }

  @Test
  void chainedStatefulNodesFlushInTopologicalOrder() {
    Capture capture = new Capture();
    var runner =
        runner(
            List.of(
                node(
                    "first", "sample", Map.of("rules", List.of(Map.of("sampleRate", 3))), "second"),
                node("second", "sample", Map.of("rules", List.of(Map.of("sampleRate", 2))))),
            Map.of());
    runner.runBounded(List.of(input("first", Side.INPUT, 4)), "user", capture, () -> {});
    runner.disposeBounded(CompletionDisposition.FINISHED);
    assertEquals(List.of(2.0), capture.values("second", "__sampled__"));
  }

  @Test
  void inputOnlyBoundaryIsObservedWithoutInitializationOrFakeOutput() {
    Capture capture = new Capture();
    var runner =
        runner(
            List.of(
                node("source", "noop", Map.of(), "destination"),
                node("destination", "noop", Map.of())),
            Map.of(
                "source",
                BoundedDefinition.BoundaryRole.SOURCE_OUTPUT,
                "destination",
                BoundedDefinition.BoundaryRole.INPUT_ONLY));
    runner.runBounded(List.of(input("source", Side.OUTPUT, 2)), "user", capture, () -> {});
    runner.disposeBounded(CompletionDisposition.FINISHED);
    assertEquals(2, capture.records("destination", Side.INPUT).size());
    assertTrue(capture.records("destination", Side.OUTPUT).isEmpty());
    assertFalse(capture.started.stream().anyMatch(i -> i.phase() == Phase.INIT));
  }

  @Test
  void emptyBindingStillInvokesSql() {
    Capture capture = new Capture();
    var runner =
        runner(
            List.of(node("sql", "sqleval", Map.of("sql", "SELECT count(*) AS total FROM events"))),
            Map.of());
    runner.runBounded(List.of(input("sql", Side.INPUT, 0)), "user", capture, () -> {});
    runner.disposeBounded(CompletionDisposition.FINISHED);
    var existing =
        service.createForTestRun(
            List.of(node("sql", "sqleval", Map.of("sql", "SELECT count(*) AS total FROM events"))),
            JobContext.builder()
                .metricTags(Map.of("service", "bounded-test", "env", "test"))
                .build());
    try {
      var result =
          existing.run(List.of(), "user", new NoSourceDagRunner.DagRunConfig(true, true), true);
      assertEquals(result.getOutputEvents().get("sql"), capture.records("sql", Side.OUTPUT));
    } finally {
      existing.terminate();
    }
    assertTrue(capture.populations.containsKey("sql:INPUT"));
    assertTrue(capture.populations.containsKey("sql:OUTPUT"));
    assertTrue(capture.failures.isEmpty());
    assertEquals(
        1,
        capture.started.stream()
            .filter(i -> i.nodeId().equals("sql") && i.phase() == Phase.PROCESS)
            .count());
  }

  @Test
  void sqlBatchFailureOnEmptyInputStillHasAFatalDiagnostic() {
    Capture capture = new Capture();
    var runner =
        runner(
            List.of(
                node(
                    "sql",
                    "sqleval",
                    Map.of("sql", "SELECT count(*) AS total FROM missing_table"))),
            Map.of());
    var summary =
        runner.runBounded(List.of(input("sql", Side.INPUT, 0)), "user", capture, () -> {});
    runner.disposeBounded(CompletionDisposition.FINISHED);
    assertEquals(1, summary.failedInvocationCount());
    assertEquals(1, capture.failures.size());
    assertTrue(capture.records("sql", Side.OUTPUT).isEmpty());
  }

  @Test
  void persistenceFailureStopsBeforeDownstreamAndDoesNotBecomeARecordError() {
    Capture capture =
        new Capture() {
          @Override
          public void records(
              long sequence, Invocation invocation, Side side, List<RecordFleakData> records) {
            super.records(sequence, invocation, side, records);
            if (invocation.nodeId().equals("source") && side == Side.OUTPUT)
              throw new IllegalStateException("storage unavailable");
          }
        };
    var runner =
        runner(
            List.of(
                node("source", "noop", Map.of(), "sql"),
                node("sql", "sqleval", Map.of("sql", "SELECT count(*) AS total FROM events"))),
            Map.of("source", BoundedDefinition.BoundaryRole.SOURCE_OUTPUT));
    assertThrows(
        ExecutionStoppedException.class,
        () ->
            runner.runBounded(List.of(input("source", Side.OUTPUT, 3)), "user", capture, () -> {}));
    runner.disposeBounded(CompletionDisposition.ABORTED);
    assertFalse(
        capture.started.stream()
            .anyMatch(i -> i.nodeId().equals("sql") && i.phase() == Phase.PROCESS));
    assertTrue(
        capture.started.stream()
            .anyMatch(i -> i.nodeId().equals("sql") && i.phase() == Phase.DISPOSE));
    assertTrue(capture.errors.isEmpty());
  }

  @Test
  void cancellationDoesNotFlushPendingSample() {
    AtomicBoolean stop = new AtomicBoolean();
    Capture capture =
        new Capture() {
          @Override
          public void records(
              long sequence, Invocation invocation, Side side, List<RecordFleakData> records) {
            super.records(sequence, invocation, side, records);
            if (invocation.nodeId().equals("sample") && side == Side.OUTPUT) stop.set(true);
          }
        };
    var runner =
        runner(
            List.of(node("sample", "sample", Map.of("rules", List.of(Map.of("sampleRate", 10))))),
            Map.of());
    assertThrows(
        ExecutionStoppedException.class,
        () ->
            runner.runBounded(
                List.of(input("sample", Side.INPUT, 2)),
                "user",
                capture,
                () -> {
                  if (stop.get()) throw new ExecutionStoppedException("cancelled");
                }));
    runner.disposeBounded(CompletionDisposition.ABORTED);
    assertTrue(stop.get());
    assertTrue(capture.failures.isEmpty());
    assertTrue(capture.records("sample", Side.OUTPUT).isEmpty());
    assertFalse(capture.started.stream().anyMatch(i -> i.phase() == Phase.END_OF_INPUT));
  }

  @Test
  void rejectsDuplicateMissingAndNonEntryBindingsBeforeAnyObservation() {
    for (List<BoundInput> inputs :
        List.of(
            List.<BoundInput>of(),
            List.of(input("a", Side.INPUT, 1), input("a", Side.INPUT, 1)),
            List.of(input("b", Side.INPUT, 1)))) {
      Capture capture = new Capture();
      var runner =
          runner(List.of(node("a", "noop", Map.of(), "b"), node("b", "noop", Map.of())), Map.of());
      assertThrows(
          IllegalArgumentException.class,
          () -> runner.runBounded(inputs, "user", capture, () -> {}));
      assertTrue(capture.started.isEmpty());
      runner.disposeBounded(CompletionDisposition.ABORTED);
    }
  }

  @Test
  void perRecordControlIsNotSwallowedByScalarErrorCollection() {
    AtomicInteger processed = new AtomicInteger();
    AtomicBoolean stop = new AtomicBoolean();
    ScalarCommand command =
        new ScalarCommand(
            "test",
            JobContext.builder().build(),
            config -> new CommandConfig() {},
            (config, node, job) -> {}) {
          @Override
          public String commandName() {
            return "test";
          }

          @Override
          protected ExecutionContext createExecutionContext(
              MetricClientProvider metrics, JobContext job, CommandConfig config, String node) {
            return () -> {};
          }

          @Override
          protected List<RecordFleakData> processOneEvent(
              RecordFleakData event, String user, ExecutionContext context) {
            if (processed.incrementAndGet() == 2) stop.set(true);
            return List.of(event);
          }
        };
    command.setExecutionHooks(
        new ExecutionHooks(
            () -> {
              if (stop.get()) throw new ExecutionStoppedException("stop");
            },
            new ExecutionHooks.Effects() {
              public long started() {
                return 1;
              }

              public void finished(long id, EffectOutcome outcome) {}
            },
            Runnable::run));
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
    assertThrows(
        ExecutionStoppedException.class,
        () ->
            command.process(
                input("test", Side.INPUT, 5).records(), "user", command.getExecutionContext()));
    assertEquals(2, processed.get());
  }

  @Test
  void opaqueSuccessfulOperationsRemainAcknowledgedWithoutInventedCounts() {
    Capture capture = new Capture();
    var runner =
        service.createForBoundedRun(
            List.of(node("sql", "sqleval", Map.of("sql", "SELECT * FROM events"))),
            JobContext.builder()
                .metricTags(Map.of("service", "bounded-test", "env", "test"))
                .build(),
            new BoundedDefinition(Map.of(), Set.of("sql")));
    runner.runBounded(List.of(input("sql", Side.INPUT, 2)), "user", capture, () -> {});
    runner.disposeBounded(CompletionDisposition.FINISHED);
    assertEquals(2, capture.effects.size());
    for (EffectOutcome outcome : capture.effects) {
      assertEquals(EffectOutcome.Delivery.ACKNOWLEDGED, outcome.delivery());
      assertEquals("operation_returned", outcome.acknowledgementKind());
      assertNull(outcome.attemptedCount());
      assertNull(outcome.acknowledgedCount());
    }
  }

  @Test
  void opaqueProcessFailureDoesNotClaimSuccessfulOperation() {
    Capture capture = new Capture();
    var runner =
        service.createForBoundedRun(
            List.of(node("sql", "sqleval", Map.of("sql", "SELECT * FROM missing_table"))),
            JobContext.builder()
                .metricTags(Map.of("service", "bounded-test", "env", "test"))
                .build(),
            new BoundedDefinition(Map.of(), Set.of("sql")));
    runner.runBounded(List.of(input("sql", Side.INPUT, 2)), "user", capture, () -> {});
    runner.disposeBounded(CompletionDisposition.FINISHED);
    assertEquals(2, capture.effects.size());
    assertEquals(EffectOutcome.Delivery.ACKNOWLEDGED, capture.effects.getFirst().delivery());
    assertEquals(EffectOutcome.Delivery.UNKNOWN, capture.effects.getLast().delivery());
    assertFalse(capture.failures.isEmpty());
  }

  @Test
  void backgroundOperationOutcomeReflectsActualReturnAndFailure() {
    Capture capture = new Capture();
    var execution =
        new BoundedExecution(new BoundedDefinition(Map.of(), Set.of("lookup")), capture, () -> {});
    var background = execution.hooks("lookup").backgroundWork();
    background.run(() -> {});
    var failure = new IllegalStateException("response lost");
    assertSame(
        failure,
        assertThrows(
            IllegalStateException.class,
            () ->
                background.run(
                    () -> {
                      throw failure;
                    })));
    assertEquals(
        List.of(EffectOutcome.Delivery.ACKNOWLEDGED, EffectOutcome.Delivery.UNKNOWN),
        capture.effects.stream().map(EffectOutcome::delivery).toList());
    assertEquals(List.of(failure), capture.failures);
  }

  @Test
  void separateBranchFailuresAreAllObserved() {
    Capture capture = new Capture();
    var runner =
        runner(
            List.of(
                node("source", "noop", Map.of(), "bad-a", "bad-b"),
                node("bad-a", "sqleval", Map.of("sql", "SELECT * FROM missing_a")),
                node("bad-b", "sqleval", Map.of("sql", "SELECT * FROM missing_b"))),
            Map.of("source", BoundedDefinition.BoundaryRole.SOURCE_OUTPUT));
    var summary =
        runner.runBounded(List.of(input("source", Side.OUTPUT, 3)), "user", capture, () -> {});
    runner.disposeBounded(CompletionDisposition.FINISHED);
    assertEquals(2, summary.failedInvocationCount());
    assertEquals(2, capture.failures.size());
    assertEquals(3, capture.records("bad-a", Side.INPUT).size());
    assertEquals(3, capture.records("bad-b", Side.INPUT).size());
  }

  @Test
  void cancellationWaitsForOpaqueCallAndClosesBeforeReturning() throws Exception {
    var entered = new java.util.concurrent.CountDownLatch(1);
    var release = new java.util.concurrent.CountDownLatch(1);
    var stop = new AtomicBoolean();
    var closed = new AtomicBoolean();
    CommandFactory factory =
        new CommandFactory() {
          public CommandType commandType() {
            return CommandType.INTERMEDIATE_COMMAND;
          }

          public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
            return new ScalarCommand(
                nodeId, jobContext, config -> new CommandConfig() {}, (config, node, job) -> {}) {
              public String commandName() {
                return "opaque";
              }

              protected ExecutionContext createExecutionContext(
                  MetricClientProvider metrics, JobContext job, CommandConfig config, String node) {
                return () -> closed.set(true);
              }

              protected List<RecordFleakData> processOneEvent(
                  RecordFleakData record, String user, ExecutionContext context) throws Exception {
                entered.countDown();
                assertTrue(release.await(5, java.util.concurrent.TimeUnit.SECONDS));
                return List.of(record);
              }
            };
          }
        };
    var localService =
        new DagRunnerService(
            new DagCompiler(Map.of("opaque", factory)),
            new MetricClientProvider.NoopMetricClientProvider());
    var runner =
        localService.createForBoundedRun(
            List.of(node("op", "opaque", Map.of())),
            JobContext.builder()
                .metricTags(Map.of("service", "bounded-test", "env", "test"))
                .build(),
            new BoundedDefinition(Map.of(), Set.of("op")));
    Capture capture = new Capture();
    try (var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
      var future =
          executor.submit(
              () -> {
                try {
                  runner.runBounded(
                      List.of(input("op", Side.INPUT, 1)),
                      "user",
                      capture,
                      () -> {
                        if (stop.get()) throw new ExecutionStoppedException("cancelled");
                      });
                } finally {
                  runner.disposeBounded(CompletionDisposition.ABORTED);
                }
              });
      try {
        assertTrue(entered.await(5, java.util.concurrent.TimeUnit.SECONDS));
        stop.set(true);
        assertThrows(
            java.util.concurrent.TimeoutException.class,
            () -> future.get(100, java.util.concurrent.TimeUnit.MILLISECONDS));
        assertFalse(closed.get());
      } finally {
        release.countDown();
      }
      var failure =
          assertThrows(
              java.util.concurrent.ExecutionException.class,
              () -> future.get(5, java.util.concurrent.TimeUnit.SECONDS));
      assertInstanceOf(ExecutionStoppedException.class, failure.getCause());
      assertTrue(closed.get());
      assertEquals(input("op", Side.INPUT, 1).records(), capture.records("op", Side.OUTPUT));
      assertTrue(
          capture.incomplete.stream()
              .anyMatch(i -> i.nodeId().equals("op") && i.phase() == Phase.PROCESS));
    }
  }

  @Test
  void fatalInitializationStopsBeforeAnyBranchProcessesInputAndDisposesEarlierContexts() {
    AtomicInteger processed = new AtomicInteger();
    AtomicInteger closed = new AtomicInteger();
    CommandFactory factory =
        new CommandFactory() {
          @Override
          public CommandType commandType() {
            return CommandType.INTERMEDIATE_COMMAND;
          }

          @Override
          public OperatorCommand createCommand(String id, JobContext context) {
            return new ScalarCommand(
                id, context, config -> new CommandConfig() {}, (config, node, job) -> {}) {
              @Override
              public String commandName() {
                return "initialization-test";
              }

              @Override
              protected ExecutionContext createExecutionContext(
                  MetricClientProvider metrics, JobContext job, CommandConfig config, String node) {
                if (node.equals("failed")) throw new IllegalArgumentException("init rejected");
                return () -> closed.incrementAndGet();
              }

              @Override
              protected List<RecordFleakData> processOneEvent(
                  RecordFleakData event, String user, ExecutionContext executionContext) {
                processed.incrementAndGet();
                return List.of(event);
              }
            };
          }
        };
    var localService =
        new DagRunnerService(
            new DagCompiler(Map.of("initialization-test", factory)),
            new MetricClientProvider.NoopMetricClientProvider());
    var runner =
        localService.createForBoundedRun(
            List.of(
                node("ready", "initialization-test", Map.of()),
                node("failed", "initialization-test", Map.of())),
            JobContext.builder()
                .metricTags(Map.of("service", "bounded-test", "env", "test"))
                .build(),
            new BoundedDefinition(Map.of(), Set.of()));
    Capture capture = new Capture();
    try {
      assertThrows(
          ExecutionStoppedException.class,
          () ->
              runner.runBounded(
                  List.of(input("ready", Side.INPUT, 1), input("failed", Side.INPUT, 1)),
                  "user",
                  capture,
                  () -> {}));
      assertEquals(0, processed.get());
      assertEquals(1, capture.failures.size());
      assertTrue(capture.started.stream().noneMatch(i -> i.phase() == Phase.PROCESS));
    } finally {
      runner.disposeBounded(CompletionDisposition.ABORTED);
    }
    assertEquals(1, closed.get());
  }

  @Test
  void cancellationPublishesCompletedScalarPrefixAndErrorsWithoutRunningDownstream() {
    AtomicBoolean stop = new AtomicBoolean();
    AtomicInteger processed = new AtomicInteger();
    CommandFactory factory =
        new CommandFactory() {
          public CommandType commandType() {
            return CommandType.INTERMEDIATE_COMMAND;
          }

          public OperatorCommand createCommand(String id, JobContext context) {
            return new ScalarCommand(
                id, context, config -> new CommandConfig() {}, (config, node, job) -> {}) {
              public String commandName() {
                return "partial-test";
              }

              protected ExecutionContext createExecutionContext(
                  MetricClientProvider metrics, JobContext job, CommandConfig config, String node) {
                return () -> {};
              }

              protected List<RecordFleakData> processOneEvent(
                  RecordFleakData event, String user, ExecutionContext context) {
                if (id.equals("downstream")) fail("Incomplete parent must not route downstream");
                int ordinal = processed.incrementAndGet();
                if (ordinal == 2)
                  throw new IllegalArgumentException(
                      "bad second record",
                      new IllegalStateException("token=private-test-credential"));
                if (ordinal == 3) stop.set(true);
                return List.of(event);
              }
            };
          }
        };
    var localService =
        new DagRunnerService(
            new DagCompiler(Map.of("partial-test", factory)),
            new MetricClientProvider.NoopMetricClientProvider());
    var runner =
        localService.createForBoundedRun(
            List.of(
                node("first", "partial-test", Map.of(), "downstream"),
                node("downstream", "partial-test", Map.of())),
            JobContext.builder().metricTags(Map.of("service", "test", "env", "test")).build(),
            new BoundedDefinition(Map.of(), Set.of()));
    Capture capture = new Capture();
    List<org.apache.logging.log4j.core.LogEvent> logs = new ArrayList<>();
    var logger =
        (org.apache.logging.log4j.core.Logger)
            org.apache.logging.log4j.LogManager.getLogger(ScalarCommand.class);
    var previousLevel = logger.getLevel();
    var appender =
        new org.apache.logging.log4j.core.appender.AbstractAppender(
            "bounded-scalar-test",
            null,
            org.apache.logging.log4j.core.layout.PatternLayout.createDefaultLayout(),
            false,
            org.apache.logging.log4j.core.config.Property.EMPTY_ARRAY) {
          public void append(org.apache.logging.log4j.core.LogEvent event) {
            logs.add(event.toImmutable());
          }
        };
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(org.apache.logging.log4j.Level.DEBUG);
    try {
      assertThrows(
          ExecutionStoppedException.class,
          () ->
              runner.runBounded(
                  List.of(input("first", Side.INPUT, 5)),
                  "user",
                  capture,
                  () -> {
                    if (stop.get()) throw new ExecutionStoppedException("cancelled");
                  }));
      assertEquals(3, processed.get());
      assertEquals(List.of(0.0, 2.0), capture.values("first", "id"));
      assertEquals(
          List.of(input("first", Side.INPUT, 5).records().get(1)),
          capture.errors.stream().map(ErrorOutput::inputEvent).toList());
      assertEquals("bad second record", capture.errors.getFirst().errorMessage());
      assertTrue(capture.records("downstream", Side.INPUT).isEmpty());
      assertTrue(capture.failures.isEmpty());
      assertEquals(1, logs.size());
      assertFalse(logs.getFirst().getMessage().getFormattedMessage().contains("bad second record"));
      assertFalse(
          logs.getFirst().getMessage().getFormattedMessage().contains("private-test-credential"));
      assertNull(logs.getFirst().getThrown());
      assertTrue(
          capture.incomplete.stream()
              .anyMatch(i -> i.nodeId().equals("first") && i.phase() == Phase.PROCESS));
    } finally {
      runner.disposeBounded(CompletionDisposition.ABORTED);
      logger.setLevel(previousLevel);
      logger.removeAppender(appender);
      appender.stop();
    }
  }

  private NoSourceDagRunner runner(
      List<DagNode> nodes, Map<String, BoundedDefinition.BoundaryRole> boundaries) {
    return service.createForBoundedRun(
        nodes,
        JobContext.builder().metricTags(Map.of("service", "bounded-test", "env", "test")).build(),
        new BoundedDefinition(boundaries, Set.of()));
  }

  private static DagNode node(
      String id, String command, Map<String, Object> config, String... outputs) {
    return new DagNode(id, command, config, List.of(outputs));
  }

  private static BoundInput input(String node, Side side, int size) {
    return new BoundInput(
        "binding-" + node,
        node,
        side,
        IntStream.range(0, size)
            .mapToObj(i -> (RecordFleakData) FleakData.wrap(Map.of("id", i)))
            .toList());
  }

  private static class Capture implements ExecutionObserver {
    final List<Long> sequences = new ArrayList<>();
    final List<Invocation> started = new ArrayList<>();
    final List<Invocation> incomplete = new ArrayList<>();
    final Map<String, List<RecordFleakData>> populations = new HashMap<>();
    final List<ErrorOutput> errors = new ArrayList<>();
    final List<Throwable> failures = new ArrayList<>();
    final List<EffectOutcome> effects = new ArrayList<>();

    @Override
    public void invocationStarted(long sequence, Invocation invocation) {
      sequences.add(sequence);
      started.add(invocation);
    }

    @Override
    public void records(
        long sequence, Invocation invocation, Side side, List<RecordFleakData> records) {
      sequences.add(sequence);
      populations
          .computeIfAbsent(invocation.nodeId() + ":" + side, ignored -> new ArrayList<>())
          .addAll(records);
    }

    @Override
    public void recordErrors(long sequence, Invocation invocation, List<ErrorOutput> errors) {
      sequences.add(sequence);
      this.errors.addAll(errors);
    }

    @Override
    public void invocationFailed(long sequence, Invocation invocation, Throwable failure) {
      sequences.add(sequence);
      failures.add(failure);
    }

    @Override
    public void invocationFinished(
        long sequence, Invocation invocation, long input, long output, boolean complete) {
      sequences.add(sequence);
      if (!complete) incomplete.add(invocation);
    }

    @Override
    public void effectStarted(long sequence, Invocation invocation, long effectId) {
      sequences.add(sequence);
    }

    @Override
    public void effectFinished(
        long sequence, Invocation invocation, long effectId, EffectOutcome outcome) {
      sequences.add(sequence);
      effects.add(outcome);
    }

    List<RecordFleakData> records(String node, Side side) {
      return populations.getOrDefault(node + ":" + side, List.of());
    }

    List<Double> values(String node, String field) {
      return records(node, Side.OUTPUT).stream()
          .map(record -> ((Number) record.unwrap().get(field)).doubleValue())
          .toList();
    }
  }
}
