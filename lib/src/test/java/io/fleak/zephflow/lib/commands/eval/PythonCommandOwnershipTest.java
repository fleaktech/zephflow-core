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
package io.fleak.zephflow.lib.commands.eval;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.eval.python.PythonExecutor;
import io.fleak.zephflow.lib.commands.sample.SampleCommandFactory;
import io.fleak.zephflow.lib.commands.throttle.ThrottleCommandFactory;
import io.fleak.zephflow.lib.windowing.GroupKeyEvaluator;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class PythonCommandOwnershipTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void throttleValidationAndFinishOrAbortCloseSeparateExecutors(boolean abort) throws Exception {
    PythonExecutor validation = mock(), runtime = mock();
    try (var python = mockStatic(PythonExecutor.class)) {
      python
          .when(() -> PythonExecutor.createPythonExecutor(any(), anyBoolean()))
          .thenReturn(validation, runtime);
      var command = new ThrottleCommandFactory().createCommand("throttle", job());
      command.parseAndValidateArg(
          Map.of("keyExpression", "$.key", "numToAllow", 1, "periodSeconds", 60));
      verify(validation).close();
      command.initialize(new MetricClientProvider.NoopMetricClientProvider());
      verify(runtime, never()).close();
      if (abort) command.abort();
      else command.terminate();
      verify(runtime).close();
      command.terminate();
      verify(runtime, times(1)).close();
    }
  }

  @Test
  void groupKeyRetainsExecutorAfterEvaluationAndReleasesItOnClose() throws Exception {
    PythonExecutor executor = mock();
    try (var python = mockStatic(PythonExecutor.class)) {
      python
          .when(() -> PythonExecutor.createPythonExecutor(any(), anyBoolean()))
          .thenReturn(executor);
      var evaluator = GroupKeyEvaluator.compile("$.key", job());
      assertEquals(
          GroupKeyEvaluator.Kind.PRESENT,
          evaluator.evaluate((RecordFleakData) FleakData.wrap(Map.of("key", "one"))).kind());
      verify(executor, never()).close();
      assertInstanceOf(AutoCloseable.class, evaluator).close();
      verify(executor).close();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"eval", "group"})
  void compilerFailureReleasesAllocatedExecutorAndPreservesPrimary(String owner) throws Exception {
    PythonExecutor executor = mock();
    var cleanup = new IllegalStateException("cleanup");
    doThrow(cleanup).when(executor).close();
    try (var python = mockStatic(PythonExecutor.class)) {
      python
          .when(() -> PythonExecutor.createPythonExecutor(any(), anyBoolean()))
          .thenReturn(executor);
      var failure =
          assertThrows(
              IllegalArgumentException.class,
              () -> {
                if (owner.equals("eval"))
                  EvalCommand.createEvalExecutionContext(
                      new MetricClientProvider.NoopMetricClientProvider(),
                      job(),
                      new EvalCommandDto.Config("unknown_function($)", false),
                      "eval",
                      "eval");
                else GroupKeyEvaluator.compile("unknown_function($)", job());
              });
      assertTrue(failure.getMessage().contains("Unknown function"));
      assertTrue(contains(failure, cleanup));
      verify(executor).close();
    }
  }

  @Test
  void sampleLaterMetricFailureClosesAlreadyCompiledConditions() throws Exception {
    PythonExecutor validation = mock(), runtime = mock();
    var failure = new IllegalStateException("counter construction failed");
    MetricClientProvider metrics = mock();
    when(metrics.counter(anyString(), anyMap())).thenThrow(failure);
    try (var python = mockStatic(PythonExecutor.class)) {
      python
          .when(() -> PythonExecutor.createPythonExecutor(any(), anyBoolean()))
          .thenReturn(validation, runtime);
      var command = new SampleCommandFactory().createCommand("sample", job());
      command.parseAndValidateArg(
          Map.of("rules", List.of(Map.of("condition", "true", "sampleRate", 2))));
      assertSame(
          failure, assertThrows(IllegalStateException.class, () -> command.initialize(metrics)));
      verify(runtime).close();
    }
  }

  @Test
  void sampleDisposalAttemptsAllConditionsAndSurfacesCleanupFailure() throws Exception {
    PythonExecutor firstValidation = mock(),
        secondValidation = mock(),
        first = mock(),
        second = mock();
    var cleanup = new IllegalStateException("cleanup");
    doThrow(cleanup).when(first).close();
    when(first.boundedDiagnostics()).thenReturn(true);
    try (var python = mockStatic(PythonExecutor.class)) {
      python
          .when(() -> PythonExecutor.createPythonExecutor(any(), anyBoolean()))
          .thenReturn(firstValidation, secondValidation, first, second);
      var command = new SampleCommandFactory().createCommand("sample", job());
      command.parseAndValidateArg(
          Map.of(
              "rules",
              List.of(
                  Map.of("condition", "true", "sampleRate", 2),
                  Map.of("condition", "false", "sampleRate", 3))));
      command.initialize(new MetricClientProvider.NoopMetricClientProvider());
      var failure = assertThrows(Exception.class, command::abort);
      assertTrue(contains(failure, cleanup));
      verify(first).close();
      verify(second).close();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"eval", "group"})
  void boundedCleanupFailureDuringPythonInitCannotBeSwallowedByFallback(String owner) {
    PythonExecutor.CleanupFailure cleanup = mock();
    try (var python = mockStatic(PythonExecutor.class)) {
      python
          .when(() -> PythonExecutor.createPythonExecutor(any(), anyBoolean()))
          .thenThrow(cleanup);
      assertSame(
          cleanup,
          assertThrows(
              PythonExecutor.CleanupFailure.class,
              () -> {
                if (owner.equals("eval"))
                  EvalCommand.createEvalExecutionContext(
                      new MetricClientProvider.NoopMetricClientProvider(),
                      job(),
                      new EvalCommandDto.Config("$", false),
                      "eval",
                      "eval");
                else GroupKeyEvaluator.compile("$", job());
              }));
    }
  }

  private static boolean contains(Throwable failure, Throwable expected) {
    if (failure == expected) return true;
    if (failure.getCause() != null && contains(failure.getCause(), expected)) return true;
    for (Throwable suppressed : failure.getSuppressed())
      if (contains(suppressed, expected)) return true;
    return false;
  }

  private static JobContext job() {
    return JobContext.builder()
        .metricTags(Map.of("service", "ownership", "env", "test"))
        .executionHooks(
            new io.fleak.zephflow.api.execution.ExecutionHooks(() -> {}, mock(), Runnable::run))
        .build();
  }
}
