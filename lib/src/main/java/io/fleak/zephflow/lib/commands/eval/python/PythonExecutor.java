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
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

/** Created by bolei on 4/22/25 */
@Slf4j
public record PythonExecutor(Map<ParserRuleContext, CompiledPythonFunction> compiledPythonFunctions)
    implements AutoCloseable {

  @Override
  public void close() throws Exception {
    compiledPythonFunctions
        .values()
        .forEach(
            compiledFunc -> {
              try {
                log.debug("Closing Python Context via PythonExecutor.");
                compiledFunc.pythonContext().close(true); // Force close
              } catch (Exception e) {
                log.error("Error closing Python Context.", e);
              }
            });
  }

  public static PythonExecutor createPythonExecutor(
      EvalExpressionParser.LanguageContext languageContext) {

    // Attempt to create GraalVM resources
    PythonFunctionCollector collector = new PythonFunctionCollector(new HashMap<>());
    ParseTreeWalker walker = new ParseTreeWalker();
    walker.walk(collector, languageContext); // Walk the parsed tree

    Map<ParserRuleContext, CompiledPythonFunction> compiledFunctions =
        collector.getCompiledFunctions();
    log.debug(
        "Python function pre-compilation complete. Found {} Python function nodes.",
        compiledFunctions.size());

    // Create the executor *only if* context creation and collection were successful.
    // We allow an empty compiledFunctions map here, assuming the executor might be needed
    // even if this particular expression has no python calls.
    log.info("PythonExecutor created.");
    return new PythonExecutor(compiledFunctions);
  }
}
