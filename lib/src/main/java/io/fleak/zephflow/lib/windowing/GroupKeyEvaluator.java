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
package io.fleak.zephflow.lib.windowing;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.compiled.CompiledExpression;
import io.fleak.zephflow.lib.commands.eval.compiled.ExpressionCompiler;
import io.fleak.zephflow.lib.commands.eval.python.PythonExecutor;
import io.fleak.zephflow.lib.utils.AntlrUtils;
import java.util.Collection;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * Shared group-key derivation for reduction commands: compiles an EEL expression once and evaluates
 * it per event into a classified {@link GroupKey}. This is the common "key machine"; the
 * missing-key POLICY (drop, pass-through, error) is left to each command.
 */
@Slf4j
public final class GroupKeyEvaluator implements AutoCloseable {

  public enum Kind {
    /** Result is a usable scalar; {@link GroupKey#value()} holds its string form. */
    PRESENT,
    /** Result is null or an empty string. */
    MISSING,
    /** Result is a map/array — not usable as a single key. */
    NON_SCALAR,
    /** Evaluation threw. */
    ERROR
  }

  public record GroupKey(Kind kind, String value) {
    static final GroupKey MISSING = new GroupKey(Kind.MISSING, null);
    static final GroupKey NON_SCALAR = new GroupKey(Kind.NON_SCALAR, null);
    static final GroupKey ERROR = new GroupKey(Kind.ERROR, null);

    static GroupKey present(String value) {
      return new GroupKey(Kind.PRESENT, value);
    }
  }

  private final CompiledExpression compiledExpression;
  private final PythonExecutor pythonExecutor;

  private GroupKeyEvaluator(CompiledExpression compiledExpression, PythonExecutor pythonExecutor) {
    this.compiledExpression = compiledExpression;
    this.pythonExecutor = pythonExecutor;
  }

  @Override
  public void close() {
    if (pythonExecutor == null) return;
    try {
      pythonExecutor.close();
    } catch (Exception cleanup) {
      throw new IllegalStateException("Failed to close group key Python executor", cleanup);
    }
  }

  /** Compiles the expression; throws if it does not parse/compile (use for config validation). */
  public static GroupKeyEvaluator compile(String expression) {
    return compile(expression, null);
  }

  public static GroupKeyEvaluator compile(String expression, JobContext jobContext) {
    boolean bounded = jobContext != null && jobContext.isBoundedExecution();
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(expression, AntlrUtils.GrammarType.EVAL);
    EvalExpressionParser.LanguageContext languageContext = parser.language();
    PythonExecutor pythonExecutor = null;
    try {
      pythonExecutor = PythonExecutor.createPythonExecutor(languageContext, bounded);
    } catch (PythonExecutor.CleanupFailure cleanup) {
      throw cleanup;
    } catch (Exception e) {
      if (bounded) {
        log.error("Python support init failed for group key expression; Python disabled.");
      } else {
        log.error("Python support init failed for group key expression; Python disabled.", e);
      }
    }
    try {
      return new GroupKeyEvaluator(
          ExpressionCompiler.compile(languageContext, pythonExecutor)
              .withBoundedDiagnostics(bounded),
          pythonExecutor);
    } catch (RuntimeException | Error primary) {
      if (pythonExecutor != null) {
        try {
          pythonExecutor.close();
        } catch (Exception cleanup) {
          primary.addSuppressed(cleanup);
        }
      }
      throw primary;
    }
  }

  public GroupKey evaluate(RecordFleakData event) {
    FleakData result;
    try {
      result = compiledExpression.evaluate(event);
    } catch (Exception e) {
      if (compiledExpression.boundedDiagnostics()) {
        log.debug("group key evaluation failed");
      } else {
        log.debug("group key evaluation failed", e);
      }
      return GroupKey.ERROR;
    }
    if (result == null) {
      return GroupKey.MISSING;
    }
    Object unwrapped = result.unwrap();
    if (unwrapped == null) {
      return GroupKey.MISSING;
    }
    if (unwrapped instanceof Map<?, ?> || unwrapped instanceof Collection<?>) {
      return GroupKey.NON_SCALAR;
    }
    if (unwrapped instanceof String s) {
      return s.isEmpty() ? GroupKey.MISSING : GroupKey.present(s);
    }
    if (unwrapped instanceof Number n) {
      double d = n.doubleValue();
      boolean integral = !Double.isInfinite(d) && !Double.isNaN(d) && d == Math.floor(d);
      return GroupKey.present(integral ? Long.toString((long) d) : Double.toString(d));
    }
    return GroupKey.present(String.valueOf(unwrapped));
  }
}
