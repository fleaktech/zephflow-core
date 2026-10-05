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
package io.fleak.zephflow.lib.commands.sample;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.eval.python.PythonExecutor;
import io.fleak.zephflow.lib.commands.eval.python.PythonFunctionCollector;
import io.fleak.zephflow.lib.utils.BoundedLogCapture;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SampleConfigValidatorBoundedTest {
  private static final String SCRIPT_MARKER = "private_sample_condition";
  private static final String FAILURE_MARKER = "private_sample_provider_failure";
  private static final String CONDITION =
      "python('def " + SCRIPT_MARKER + "(value):\\n    return value', $.value)";

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void validationCompilationFailureClosesContextAndUsesCorrectDiagnosticPolicy(boolean bounded) {
    Context python = successfulContext();
    var providerFailure = new IllegalArgumentException(FAILURE_MARKER);
    when(python.eval(any(Source.class))).thenThrow(providerFailure);
    Context.Builder builder = mock(Context.Builder.class, RETURNS_SELF);
    when(builder.build()).thenReturn(python);
    try (var contexts = mockStatic(Context.class);
        var collectorLogs = new BoundedLogCapture(PythonFunctionCollector.class)) {
      contexts.when(() -> Context.newBuilder("python")).thenReturn(builder);

      var failure =
          assertThrows(IllegalArgumentException.class, () -> validateThroughCommand(bounded));

      assertTrue(failure.getMessage().contains("rules[0].condition"));
      assertTrue(contains(failure, providerFailure));
      verify(python).enter();
      verify(python).leave();
      verify(python).close(true);
      assertFalse(collectorLogs.events().isEmpty());
      String diagnostics = diagnostics(collectorLogs);
      assertEquals(!bounded, diagnostics.contains(SCRIPT_MARKER));
      assertEquals(!bounded, diagnostics.contains(FAILURE_MARKER));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void validationCleanupFailureIsVisibleOnlyWhereRequiredAndNeverLeaksBoundedCause(
      boolean bounded) {
    Context python = successfulContext();
    var cleanupFailure = new IllegalStateException(FAILURE_MARKER);
    doThrow(cleanupFailure).when(python).close(true);
    Context.Builder builder = mock(Context.Builder.class, RETURNS_SELF);
    when(builder.build()).thenReturn(python);
    try (var contexts = mockStatic(Context.class);
        var collectorLogs = new BoundedLogCapture(PythonFunctionCollector.class);
        var executorLogs = new BoundedLogCapture(PythonExecutor.class);
        var conditionLogs = new BoundedLogCapture(SampleCondition.class)) {
      contexts.when(() -> Context.newBuilder("python")).thenReturn(builder);

      if (bounded) {
        var failure = assertThrows(IllegalStateException.class, () -> validateThroughCommand(true));
        assertTrue(contains(failure, cleanupFailure));
        assertFalse(failure.getMessage().contains(FAILURE_MARKER));
      } else {
        assertDoesNotThrow(() -> validateThroughCommand(false));
      }

      verify(python).eval(any(Source.class));
      verify(python).leave();
      verify(python).close(true);
      assertFalse(executorLogs.events().isEmpty());
      String diagnostics = diagnostics(collectorLogs, executorLogs, conditionLogs);
      assertEquals(!bounded, diagnostics.contains(SCRIPT_MARKER));
      assertEquals(!bounded, diagnostics.contains(FAILURE_MARKER));
    }
  }

  private static void validateThroughCommand(boolean bounded) {
    // Compilation validates before the runner installs per-operator hooks.
    var job =
        JobContext.builder().otherProperties(Map.of(JobContext.FLAG_BOUNDED_MODE, bounded)).build();
    var command = new SampleCommandFactory().createCommand("sample", job);
    command.parseAndValidateArg(
        Map.of("rules", List.of(Map.of("condition", CONDITION, "sampleRate", 7))));
  }

  private static Context successfulContext() {
    Context python = mock();
    Value before = mock(), after = mock(), function = mock();
    when(before.getMemberKeys()).thenReturn(Set.of());
    when(after.getMemberKeys()).thenReturn(Set.of(SCRIPT_MARKER));
    when(after.getMember(SCRIPT_MARKER)).thenReturn(function);
    when(function.canExecute()).thenReturn(true);
    when(python.getBindings("python")).thenReturn(before, after);
    return python;
  }

  private static String diagnostics(BoundedLogCapture... captures) {
    StringWriter text = new StringWriter();
    PrintWriter writer = new PrintWriter(text);
    for (var capture : captures) {
      for (var event : capture.events()) {
        writer.println(event.getMessage().getFormattedMessage());
        if (event.getThrown() != null) event.getThrown().printStackTrace(writer);
      }
    }
    return text.toString();
  }

  private static boolean contains(Throwable failure, Throwable expected) {
    if (failure == expected) return true;
    if (failure.getCause() != null && contains(failure.getCause(), expected)) return true;
    for (Throwable suppressed : failure.getSuppressed()) {
      if (contains(suppressed, expected)) return true;
    }
    return false;
  }
}
