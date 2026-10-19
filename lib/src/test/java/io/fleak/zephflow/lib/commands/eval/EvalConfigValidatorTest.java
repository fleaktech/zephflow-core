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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.eval.python.PythonExecutor;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Config-time validation of eval/filter/assertion expressions (FLE-2884). */
class EvalConfigValidatorTest {

  private final EvalConfigValidator validator = new EvalConfigValidator();

  @Test
  void rejectsUnboundVariable() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config("dafew"), "n1", null));
    assertEquals("expression: variable 'dafew' is not defined", e.getMessage());
  }

  @Test
  void acceptsBoundLoopVariable() {
    assertDoesNotThrow(
        () -> validator.validateConfig(config("arr_foreach($.items, x, x + 1)"), "n1", null));
  }

  @Test
  void pythonExpressionValidatesAndClosesTheExecutorItCreated() throws Exception {
    PythonExecutor executor = mock(PythonExecutor.class);
    try (var python = mockStatic(PythonExecutor.class)) {
      python
          .when(() -> PythonExecutor.createPythonExecutor(any(), anyBoolean()))
          .thenReturn(executor);
      assertDoesNotThrow(() -> validator.validateConfig(config("python('x', $)"), "n1", null));
    }
    verify(executor).close();
  }

  @Test
  void rejectsSyntaxErrorWithTheParameterLabel() {
    // parse fails before an executor is created: the error is still labeled and nothing leaks
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config("this is not valid"), "n1", null));
    assertTrue(e.getMessage().startsWith("expression: "), e.getMessage());
  }

  @Test
  void validatesUnderBoundedJobContext() {
    JobContext bounded =
        JobContext.builder().otherProperties(Map.of(JobContext.FLAG_BOUNDED_MODE, true)).build();
    assertDoesNotThrow(
        () -> validator.validateConfig(config("arr_foreach($.items, x, x + 1)"), "n1", bounded));
  }

  @Test
  void aFailureToCloseTheExecutorIsSwallowed() throws Exception {
    PythonExecutor executor = mock(PythonExecutor.class);
    doThrow(new RuntimeException("close failed")).when(executor).close();
    try (var python = mockStatic(PythonExecutor.class)) {
      python
          .when(() -> PythonExecutor.createPythonExecutor(any(), anyBoolean()))
          .thenReturn(executor);
      // a close() failure must not mask a successful validation
      assertDoesNotThrow(() -> validator.validateConfig(config("python('x', $)"), "n1", null));
    }
    verify(executor).close();
  }

  private static EvalCommandDto.Config config(String expression) {
    return new EvalCommandDto.Config(expression, false);
  }
}
