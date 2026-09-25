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

import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.compiled.CompiledExpression;
import io.fleak.zephflow.lib.commands.eval.compiled.ExpressionCompiler;
import io.fleak.zephflow.lib.commands.eval.python.PythonExecutor;
import io.fleak.zephflow.lib.utils.AntlrUtils;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
record SampleCondition(CompiledExpression expression, PythonExecutor pythonExecutor) {

  static SampleCondition compile(String condition, int index) {
    PythonExecutor pythonExecutor = null;
    try {
      EvalExpressionParser.LanguageContext languageContext =
          ((EvalExpressionParser) AntlrUtils.parseInput(condition, AntlrUtils.GrammarType.EVAL))
              .language();
      pythonExecutor = PythonExecutor.createPythonExecutor(languageContext);
      return new SampleCondition(
          ExpressionCompiler.compile(languageContext, pythonExecutor), pythonExecutor);
    } catch (Exception e) {
      closeQuietly(pythonExecutor);
      throw new IllegalArgumentException(
          String.format("invalid 'rules[%s].condition' '%s': %s", index, condition, e.getMessage()),
          e);
    }
  }

  static void closeAll(List<SampleCondition> conditions) {
    conditions.forEach(c -> closeQuietly(c.pythonExecutor()));
  }

  private static void closeQuietly(PythonExecutor pythonExecutor) {
    if (pythonExecutor == null) {
      return;
    }
    try {
      pythonExecutor.close();
    } catch (Exception e) {
      log.error("failed to close python executor", e);
    }
  }
}
