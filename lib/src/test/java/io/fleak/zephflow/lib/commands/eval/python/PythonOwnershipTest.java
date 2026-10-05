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
package io.fleak.zephflow.lib.commands.eval.python;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.utils.AntlrUtils;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.antlr.v4.runtime.ParserRuleContext;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class PythonOwnershipTest {
  @ParameterizedTest
  @ValueSource(strings = {"build", "enter", "bindings", "eval", "discovery", "leave"})
  void failedLaterFunctionClosesCurrentAndEarlierContexts(String stage) {
    Context first = successfulContext();
    Context second = successfulContext();
    RuntimeException primary = new IllegalArgumentException("failed-" + stage);
    Context.Builder builder = mock(Context.Builder.class, RETURNS_SELF);
    when(builder.build()).thenReturn(first, second);
    switch (stage) {
      case "build" -> when(builder.build()).thenReturn(first).thenThrow(primary);
      case "enter" -> doThrow(primary).when(second).enter();
      case "bindings" -> when(second.getBindings("python")).thenThrow(primary);
      case "eval" -> when(second.eval(any(Source.class))).thenThrow(primary);
      case "discovery" -> {
        Value bindings = mock(Value.class);
        when(bindings.getMemberKeys()).thenReturn(Set.of());
        when(second.getBindings("python")).thenReturn(bindings);
      }
      case "leave" -> doThrow(primary).when(second).leave();
      default -> throw new AssertionError(stage);
    }
    try (var contexts = mockStatic(Context.class)) {
      contexts.when(() -> Context.newBuilder("python")).thenReturn(builder);
      assertThrows(
          RuntimeException.class, () -> PythonExecutor.createPythonExecutor(twoFunctions(), true));
      verify(first).close(true);
      if (!stage.equals("build")) verify(second).close(true);
      if (stage.equals("enter")) verify(second, never()).leave();
    }
  }

  @Test
  void successfulWalkTransfersBothContextsUntilExecutorClose() throws Exception {
    Context first = successfulContext(), second = successfulContext();
    Context.Builder builder = mock(Context.Builder.class, RETURNS_SELF);
    when(builder.build()).thenReturn(first, second);
    try (var contexts = mockStatic(Context.class)) {
      contexts.when(() -> Context.newBuilder("python")).thenReturn(builder);
      try (var executor = PythonExecutor.createPythonExecutor(twoFunctions(), true)) {
        assertEquals(2, executor.compiledPythonFunctions().size());
        verify(first, never()).close(anyBoolean());
        verify(second, never()).close(anyBoolean());
      }
      verify(first).close(true);
      verify(second).close(true);
    }
  }

  @Test
  void everyCloseIsAttemptedAndCleanupFailureIsObservable() {
    Context first = mock(), second = mock(), third = mock();
    RuntimeException firstFailure = new IllegalStateException("first-close"),
        secondFailure = new IllegalStateException("second-close");
    doThrow(firstFailure).when(first).close(true);
    doThrow(secondFailure).when(second).close(true);
    Map<ParserRuleContext, CompiledPythonFunction> functions = new LinkedHashMap<>();
    for (Context context : new Context[] {first, second, third})
      functions.put(
          new ParserRuleContext(), new CompiledPythonFunction("f", mock(Value.class), context));
    var failure = assertThrows(Exception.class, () -> new PythonExecutor(functions, true).close());
    assertTrue(contains(failure, firstFailure));
    assertTrue(contains(failure, secondFailure));
    verify(first).close(true);
    verify(second).close(true);
    verify(third).close(true);
  }

  @Test
  void compilationFailureKeepsPrimaryAndBothLeaveAndCloseFailures() {
    Context context = successfulContext();
    var primary = new IllegalArgumentException("script-failure");
    var leave = new IllegalStateException("leave-failure");
    var close = new IllegalStateException("close-failure");
    when(context.eval(any(Source.class))).thenThrow(primary);
    doThrow(leave).when(context).leave();
    doThrow(close).when(context).close(true);
    Context.Builder builder = mock(Context.Builder.class, RETURNS_SELF);
    when(builder.build()).thenReturn(context);
    try (var contexts = mockStatic(Context.class)) {
      contexts.when(() -> Context.newBuilder("python")).thenReturn(builder);
      var failure =
          assertThrows(
              RuntimeException.class,
              () -> PythonExecutor.createPythonExecutor(twoFunctions(), true));
      assertTrue(contains(failure, primary));
      assertTrue(contains(failure, leave));
      assertTrue(contains(failure, close));
      verify(context).close(true);
    }
  }

  @Test
  void actualPythonContextIsUsableUntilItsOwnerCloses() throws Exception {
    var language =
        ((EvalExpressionParser)
                AntlrUtils.parseInput(
                    "python('def identity(x):\\n    return x', $.value)",
                    AntlrUtils.GrammarType.EVAL))
            .language();
    Context context;
    try (var executor = PythonExecutor.createPythonExecutor(language, true)) {
      context = executor.compiledPythonFunctions().values().iterator().next().pythonContext();
      var compiled =
          io.fleak.zephflow.lib.commands.eval.compiled.ExpressionCompiler.compile(
              language, executor);
      assertEquals(
          7.0,
          compiled
              .evaluate(io.fleak.zephflow.api.structure.FleakData.wrap(Map.of("value", 7)))
              .getNumberValue());
    }
    var closed =
        assertThrows(
            org.graalvm.polyglot.PolyglotException.class, () -> context.getBindings("python"));
    assertTrue(closed.isCancelled(), "Forced owner close must cancel the actual Graal context");
  }

  static boolean contains(Throwable failure, Throwable expected) {
    if (failure == expected) return true;
    if (failure.getCause() != null && contains(failure.getCause(), expected)) return true;
    for (Throwable suppressed : failure.getSuppressed())
      if (contains(suppressed, expected)) return true;
    return false;
  }

  static Context successfulContext() {
    Context context = mock();
    Value before = mock(), after = mock(), function = mock();
    when(before.getMemberKeys()).thenReturn(Set.of());
    when(after.getMemberKeys()).thenReturn(Set.of("f"));
    when(after.getMember("f")).thenReturn(function);
    when(function.canExecute()).thenReturn(true);
    when(context.getBindings("python")).thenReturn(before, after);
    return context;
  }

  static EvalExpressionParser.LanguageContext twoFunctions() {
    return ((EvalExpressionParser)
            AntlrUtils.parseInput(
                "python('first', $) + python('second', $)", AntlrUtils.GrammarType.EVAL))
        .language();
  }
}
