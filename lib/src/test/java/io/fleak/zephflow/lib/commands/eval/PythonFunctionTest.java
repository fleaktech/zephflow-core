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

import io.fleak.zephflow.api.structure.*;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.compiled.CompiledExpression;
import io.fleak.zephflow.lib.commands.eval.compiled.ExpressionCompiler;
import io.fleak.zephflow.lib.commands.eval.python.PythonExecutor;
import io.fleak.zephflow.lib.utils.AntlrUtils;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PythonFunctionTest extends FeelFunctionTestBase {

  @Test
  public void testPythonFunction() {
    FleakData testData =
        FleakData.wrap(
            Map.of("value", 10.0, "factor", 5, "name", "alice", "roles", List.of("admin", "user")));

    String simpleScript =
        """
python(
  '
def add_one(x):
    return x + 1
',
  $["value"]
)""";

    try {
      EvalExpressionParser parser =
          (EvalExpressionParser) AntlrUtils.parseInput(simpleScript, AntlrUtils.GrammarType.EVAL);
      var languageContext = parser.language();

      try (PythonExecutor pythonExecutor = PythonExecutor.createPythonExecutor(languageContext)) {
        CompiledExpression compiled = ExpressionCompiler.compile(languageContext, pythonExecutor);
        FleakData result = compiled.evaluate(testData);
        assertNotNull(result);
        assertEquals(11.0, result.getNumberValue());
      }
    } catch (Exception e) {
      fail("Python test skipped (GraalVM Python not available): " + e.getMessage());
    }
  }

  @Test
  public void testPythonFunctionWithMapArg() {
    FleakData testData = FleakData.wrap(Map.of("record", Map.of("name", "alice", "age", 30)));

    String script =
        """
python(
  '
def listify(arg1):
    result = []
    for key, value in arg1.items():
        result.append(str(key) + "=" + str(value))
    result.sort()
    return result
',
  $["record"]
)""";

    try {
      EvalExpressionParser parser =
          (EvalExpressionParser) AntlrUtils.parseInput(script, AntlrUtils.GrammarType.EVAL);
      var languageContext = parser.language();

      try (PythonExecutor pythonExecutor = PythonExecutor.createPythonExecutor(languageContext)) {
        CompiledExpression compiled = ExpressionCompiler.compile(languageContext, pythonExecutor);
        FleakData result = compiled.evaluate(testData);
        assertNotNull(result);
        assertEquals(List.of("age=30", "name=alice"), result.unwrap());
      }
    } catch (Exception e) {
      fail("Python with Map arg failed: " + e.getMessage());
    }
  }

  @Test
  public void testPythonFunctionWithNestedStructures() {
    FleakData testData =
        FleakData.wrap(
            Map.of(
                "data", Map.of("users", List.of(Map.of("name", "alice"), Map.of("name", "bob")))));

    String script =
        """
python(
  '
def extract_names(data):
    return [u["name"] for u in data["users"]]
',
  $["data"]
)""";

    try {
      EvalExpressionParser parser =
          (EvalExpressionParser) AntlrUtils.parseInput(script, AntlrUtils.GrammarType.EVAL);
      var languageContext = parser.language();

      try (PythonExecutor pythonExecutor = PythonExecutor.createPythonExecutor(languageContext)) {
        CompiledExpression compiled = ExpressionCompiler.compile(languageContext, pythonExecutor);
        FleakData result = compiled.evaluate(testData);
        assertNotNull(result);
        assertEquals(List.of("alice", "bob"), result.unwrap());
      }
    } catch (Exception e) {
      fail("Python with nested structures failed: " + e.getMessage());
    }
  }

  @Test
  public void testPythonErrorHandling() {
    FleakData testData = new StringPrimitiveFleakData("test");

    String pythonScript = "python('def test(): return 1', 1)";

    try {
      evaluateExpression(pythonScript, testData);
      fail("Expected Python function to be unavailable");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Unknown function: python"));
    }
  }

  @Test
  public void testPythonFunctionSignatureInErrorMessages() {
    String badPythonExpr = "python()";

    try {
      EvalExpressionParser parser =
          (EvalExpressionParser) AntlrUtils.parseInput(badPythonExpr, AntlrUtils.GrammarType.EVAL);
      parser.language();
    } catch (Exception e) {
      assertTrue(
          e.getMessage().contains("python") || e.getMessage().toLowerCase().contains("argument"));
    }
  }
}
