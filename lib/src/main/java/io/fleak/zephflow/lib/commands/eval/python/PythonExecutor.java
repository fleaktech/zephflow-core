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

import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

/** Created by bolei on 4/22/25 */
@Slf4j
public record PythonExecutor(
    Map<ParserRuleContext, CompiledPythonFunction> compiledPythonFunctions,
    boolean boundedDiagnostics)
    implements AutoCloseable {

  public PythonExecutor(Map<ParserRuleContext, CompiledPythonFunction> compiledPythonFunctions) {
    this(compiledPythonFunctions, false);
  }

  /** A bounded cleanup failure that must not be mistaken for unavailable Python support. */
  public static final class CleanupFailure extends IllegalStateException {
    CleanupFailure(Throwable cause) {
      super("Failed to close Python contexts", cause);
    }
  }

  @Override
  public void close() throws Exception {
    CleanupFailure failure = null;
    for (CompiledPythonFunction compiledFunction : compiledPythonFunctions.values()) {
      try {
        log.debug("Closing Python Context via PythonExecutor.");
        compiledFunction.pythonContext().close(true);
      } catch (Exception cleanup) {
        if (boundedDiagnostics) {
          log.error("Error closing Python Context.");
          if (failure == null) failure = new CleanupFailure(cleanup);
          else failure.addSuppressed(cleanup);
        } else {
          log.error("Error closing Python Context.", cleanup);
        }
      }
    }
    if (failure != null) throw failure;
  }

  public static PythonExecutor createPythonExecutor(
      EvalExpressionParser.LanguageContext languageContext) {
    return createPythonExecutor(languageContext, false);
  }

  public static PythonExecutor createPythonExecutor(
      EvalExpressionParser.LanguageContext languageContext, boolean boundedDiagnostics) {

    // Attempt to create GraalVM resources
    PythonFunctionCollector collector =
        new PythonFunctionCollector(new LinkedHashMap<>(), boundedDiagnostics);
    ParseTreeWalker walker = new ParseTreeWalker();
    try {
      walker.walk(collector, languageContext);
    } catch (CleanupFailure primary) {
      try {
        new PythonExecutor(collector.getCompiledFunctions(), boundedDiagnostics).close();
      } catch (Exception cleanup) {
        primary.addSuppressed(cleanup);
      }
      throw primary;
    } catch (RuntimeException | Error primary) {
      try {
        new PythonExecutor(collector.getCompiledFunctions(), boundedDiagnostics).close();
      } catch (Exception cleanup) {
        CleanupFailure failure = new CleanupFailure(primary);
        failure.addSuppressed(cleanup);
        throw failure;
      }
      throw primary;
    }

    Map<ParserRuleContext, CompiledPythonFunction> compiledFunctions =
        collector.getCompiledFunctions();
    log.debug(
        "Python function pre-compilation complete. Found {} Python function nodes.",
        compiledFunctions.size());

    // Create the executor *only if* context creation and collection were successful.
    // We allow an empty compiledFunctions map here, assuming the executor might be needed
    // even if this particular expression has no python calls.
    log.info("PythonExecutor created.");
    return new PythonExecutor(compiledFunctions, boundedDiagnostics);
  }
}
