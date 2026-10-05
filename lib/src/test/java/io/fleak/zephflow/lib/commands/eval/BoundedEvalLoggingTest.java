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
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.python.CompiledPythonFunction;
import io.fleak.zephflow.lib.commands.eval.python.PythonExecutor;
import io.fleak.zephflow.lib.commands.eval.python.PythonFunctionCollector;
import io.fleak.zephflow.lib.utils.AntlrUtils;
import io.fleak.zephflow.lib.utils.BoundedLogCapture;
import io.fleak.zephflow.lib.windowing.GroupKeyEvaluator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.antlr.v4.runtime.ParserRuleContext;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class BoundedEvalLoggingTest {
  private static final String PRIVATE_RECORD = "PRIVATE_RECORD_72918";
  private static final String AUTHORED_SCRIPT = "AUTHORED_SCRIPT_92718";

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void arrayElementFailureKeepsResultsButBoundsRecordDiagnostics(boolean bounded) throws Exception {
    try (var capture = new BoundedLogCapture(ArrForEachFunction.class);
        var context = eval("dict(results=arr_foreach($.items, elem, parse_int(elem)))", bounded)) {
      var result =
          context
              .getCompiledExpression()
              .evaluate(FleakData.wrap(Map.of("items", List.of("1", PRIVATE_RECORD, "3"))));
      assertEquals(FleakData.wrap(Map.of("results", List.of(1, 3))), result);
      assertDiagnostics(capture, bounded, PRIVATE_RECORD);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void groupKeyFailureKeepsErrorClassificationButBoundsRecordDiagnostics(boolean bounded) {
    try (var capture = new BoundedLogCapture(GroupKeyEvaluator.class)) {
      var evaluator = GroupKeyEvaluator.compile("parse_int($.value)", job(bounded));
      assertEquals(
          GroupKeyEvaluator.Kind.ERROR,
          evaluator
              .evaluate((RecordFleakData) FleakData.wrap(Map.of("value", PRIVATE_RECORD)))
              .kind());
      assertDiagnostics(capture, bounded, PRIVATE_RECORD);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void pythonCompilationFailureDoesNotLogAuthoredScript(boolean bounded) {
    Context python = mock(Context.class);
    Value initial = mock(Value.class);
    when(initial.getMemberKeys()).thenReturn(Set.of());
    when(python.getBindings("python")).thenReturn(initial);
    when(python.eval(any(Source.class))).thenThrow(new IllegalArgumentException(PRIVATE_RECORD));
    Context.Builder builder = mock(Context.Builder.class, RETURNS_SELF);
    when(builder.build()).thenReturn(python);
    try (var contexts = mockStatic(Context.class);
        var capture = new BoundedLogCapture(PythonFunctionCollector.class)) {
      contexts.when(() -> Context.newBuilder("python")).thenReturn(builder);
      var error =
          assertThrows(
              IllegalArgumentException.class,
              () ->
                  PythonExecutor.createPythonExecutor(
                      parse("python('" + AUTHORED_SCRIPT + "', $)"), bounded));
      assertEquals(PRIVATE_RECORD, error.getMessage());
      assertDiagnostics(capture, bounded, AUTHORED_SCRIPT);
      assertDiagnostics(capture, bounded, PRIVATE_RECORD);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void successfulPythonDiscoveryDoesNotLogScriptOrFunctionName(boolean bounded) throws Exception {
    Context python = mock(Context.class);
    Value initial = mock(Value.class);
    Value after = mock(Value.class);
    Value function = mock(Value.class);
    when(initial.getMemberKeys()).thenReturn(Set.of());
    when(after.getMemberKeys()).thenReturn(Set.of(AUTHORED_SCRIPT));
    when(after.getMember(AUTHORED_SCRIPT)).thenReturn(function);
    when(function.canExecute()).thenReturn(true);
    when(python.getBindings("python")).thenReturn(initial, after);
    Context.Builder builder = mock(Context.Builder.class, RETURNS_SELF);
    when(builder.build()).thenReturn(python);
    try (var contexts = mockStatic(Context.class);
        var capture = new BoundedLogCapture(PythonFunctionCollector.class)) {
      contexts.when(() -> Context.newBuilder("python")).thenReturn(builder);
      try (var executor =
          PythonExecutor.createPythonExecutor(
              parse("python('" + AUTHORED_SCRIPT + "', $)"), bounded)) {
        assertEquals(1, executor.compiledPythonFunctions().size());
        assertDiagnostics(capture, bounded, AUTHORED_SCRIPT);
      }
      verify(python).close(true);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void evalInitializationDoesNotAttachRawPythonThrowable(boolean bounded) throws Exception {
    try (var python = mockStatic(PythonExecutor.class);
        var capture = new BoundedLogCapture(EvalCommand.class)) {
      python
          .when(() -> PythonExecutor.createPythonExecutor(any(), eq(bounded)))
          .thenThrow(new IllegalArgumentException(AUTHORED_SCRIPT));
      try (var context = eval("$", bounded)) {
        assertEquals(
            FleakData.wrap(Map.of("value", 1)),
            context.getCompiledExpression().evaluate(FleakData.wrap(Map.of("value", 1))));
        assertDiagnostics(capture, bounded, AUTHORED_SCRIPT);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void pythonCleanupDoesNotAttachRawThrowable(boolean bounded) throws Exception {
    Context python = mock(Context.class);
    doThrow(new IllegalStateException(PRIVATE_RECORD)).when(python).close(true);
    try (var capture = new BoundedLogCapture(PythonExecutor.class)) {
      var executor =
          new PythonExecutor(
              Map.of(
                  new ParserRuleContext(),
                  new CompiledPythonFunction("f", mock(Value.class), python)),
              bounded);
      if (bounded) assertThrows(PythonExecutor.CleanupFailure.class, executor::close);
      else executor.close();
      assertDiagnostics(capture, bounded, PRIVATE_RECORD);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void executionContextCleanupPreservesFailureWithoutRawLog(boolean bounded) throws Exception {
    PythonExecutor executor = mock(PythonExecutor.class);
    doThrow(new java.io.IOException(PRIVATE_RECORD)).when(executor).close();
    var expression =
        new io.fleak.zephflow.lib.commands.eval.compiled.CompiledExpression(
            ctx -> ctx.getRootData(), bounded);
    var context = new EvalExecutionContext(null, null, null, null, false, executor, expression);
    try (var capture = new BoundedLogCapture(EvalExecutionContext.class)) {
      assertEquals(
          PRIVATE_RECORD, assertThrows(java.io.IOException.class, context::close).getMessage());
      assertDiagnostics(capture, bounded, PRIVATE_RECORD);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void groupKeyInitializationDoesNotAttachRawPythonThrowable(boolean bounded) {
    try (var python = mockStatic(PythonExecutor.class);
        var capture = new BoundedLogCapture(GroupKeyEvaluator.class)) {
      python
          .when(() -> PythonExecutor.createPythonExecutor(any(), eq(bounded)))
          .thenThrow(new IllegalArgumentException(AUTHORED_SCRIPT));
      var evaluator = GroupKeyEvaluator.compile("$.value", job(bounded));
      assertEquals(
          GroupKeyEvaluator.Kind.PRESENT,
          evaluator.evaluate((RecordFleakData) FleakData.wrap(Map.of("value", 1))).kind());
      assertDiagnostics(capture, bounded, AUTHORED_SCRIPT);
    }
  }

  private static EvalExecutionContext eval(String expression, boolean bounded) {
    return EvalCommand.createEvalExecutionContext(
        new MetricClientProvider.NoopMetricClientProvider(),
        job(bounded),
        new EvalCommandDto.Config(expression, false),
        "eval",
        "eval");
  }

  private static JobContext job(boolean bounded) {
    return JobContext.builder()
        .metricTags(Map.of("service", "test", "env", "test"))
        .otherProperties(Map.of(JobContext.FLAG_BOUNDED_MODE, bounded))
        .build();
  }

  private static EvalExpressionParser.LanguageContext parse(String expression) {
    return ((EvalExpressionParser) AntlrUtils.parseInput(expression, AntlrUtils.GrammarType.EVAL))
        .language();
  }

  private static void assertDiagnostics(
      BoundedLogCapture capture, boolean bounded, String privateValue) {
    assertFalse(capture.events().isEmpty());
    if (bounded) {
      capture
          .events()
          .forEach(
              event -> {
                assertFalse(event.getMessage().getFormattedMessage().contains(privateValue));
                assertNull(event.getThrown());
              });
    } else {
      assertTrue(
          capture.events().stream()
              .anyMatch(
                  event ->
                      event.getMessage().getFormattedMessage().contains(privateValue)
                          || (event.getThrown() != null
                              && event.getThrown().getMessage().contains(privateValue))));
    }
  }
}
