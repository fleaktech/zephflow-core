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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.compiled.CompiledExpression;
import io.fleak.zephflow.lib.commands.eval.compiled.ExpressionCompiler;
import io.fleak.zephflow.lib.utils.AntlrUtils;

/** Shared helpers for FEEL function tests. */
abstract class FeelFunctionTestBase {

  protected void testFunctionExecution(
      FleakData testData, String expression, Object expectedValue) {
    try {
      FleakData result = evaluateExpression(expression, testData);
      Object actualValue = result != null ? result.unwrap() : null;
      assertEquals(expectedValue, actualValue, "Expression: " + expression);
    } catch (Exception e) {
      fail("Expression failed: " + expression + ". Error: " + e.getMessage());
    }
  }

  protected FleakData evaluateExpression(String expression, FleakData testData) {
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(expression, AntlrUtils.GrammarType.EVAL);
    var tree = parser.language();
    CompiledExpression compiled = ExpressionCompiler.compile(tree, null);
    return compiled.evaluate(testData);
  }
}
