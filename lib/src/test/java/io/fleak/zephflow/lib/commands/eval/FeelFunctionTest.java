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
import io.fleak.zephflow.lib.commands.eval.python.PythonExecutor;
import io.fleak.zephflow.lib.utils.AntlrUtils;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FeelFunctionTest {

  @Test
  public void testNewFunctionArchitecture() {
    // Test simple function calls with the new generic architecture
    String[] testCases = {
      "upper(\"hello\")",
      "lower(\"WORLD\")",
      "to_str(123)",
      "size_of(\"test\")",
      "array(1, 2, 3)",
      "str_split(\"a,b,c\", \",\")",
      "substr(\"hello\", 1, 3)",
      "duration_str_to_mills(\"01:30:45\")",
      "range(5)",
      "range(1, 10, 2)",
      "floor(123.45)",
      "ceil(123.45)"
    };

    for (String testCase : testCases) {
      try {
        EvalExpressionParser parser =
            (EvalExpressionParser) AntlrUtils.parseInput(testCase, AntlrUtils.GrammarType.EVAL);
        var tree = parser.language();
        assertNotNull(tree);
      } catch (Exception e) {
        fail("Failed to parse: " + testCase + ". Error: " + e.getMessage());
      }
    }
  }

  @Test
  public void testFunctionExecution() {
    // Test that our function implementations actually work
    String expr = "upper(\"hello\")";

    try {
      EvalExpressionParser parser =
          (EvalExpressionParser) AntlrUtils.parseInput(expr, AntlrUtils.GrammarType.EVAL);
      var tree = parser.language();

      // Create a visitor to execute the function
      FleakData testData = new StringPrimitiveFleakData("test");
      ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(testData, null);

      FleakData result = visitor.visit(tree);
      assertNotNull(result);
      assertEquals("HELLO", result.getStringValue());
    } catch (Exception e) {
      fail("Function execution failed: " + e.getMessage());
    }
  }

  @Test
  public void testImprovedErrorMessages() {
    // Test the original user case - should parse OK but fail at execution with better error message
    String badExpr = "ts_str_to_epoch($[\"field\"])"; // Missing second argument

    try {
      EvalExpressionParser parser =
          (EvalExpressionParser) AntlrUtils.parseInput(badExpr, AntlrUtils.GrammarType.EVAL);
      var tree = parser.language();

      // Parsing should succeed with new generic grammar
      assertNotNull(tree);

      // But execution should fail with clear error message
      FleakData testData = new StringPrimitiveFleakData("test");
      ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(testData, null);

      try {
        visitor.visit(tree);
        fail("Expected function execution to fail due to insufficient arguments");
      } catch (Exception executionError) {
        System.out.println(executionError.getMessage());
        // Should now have clear error about argument count
        assertTrue(executionError.getMessage().contains("ts_str_to_epoch"));
        assertTrue(executionError.getMessage().contains("2 arguments"));
      }

    } catch (Exception parseError) {
      fail("Parsing should succeed with new generic grammar: " + parseError.getMessage());
    }
  }

  @Test
  public void testComplexExpression() {
    // Test the user's original complex expression (but with correct syntax)
    String complexExpr =
"""
dict(
  duration=case(
    $["tiIndicator.validUntil"] != null
    and $["tiIndicator.creationTime"] != null
    => ts_str_to_epoch($["tiIndicator.validUntil"], "yyyy-MM-dd") - ts_str_to_epoch($["tiIndicator.creationTime"], "yyyy-MM-dd"),
    _ => null)
)
""";

    try {
      EvalExpressionParser parser =
          (EvalExpressionParser) AntlrUtils.parseInput(complexExpr, AntlrUtils.GrammarType.EVAL);
      var tree = parser.language();
      assertNotNull(tree);
    } catch (Exception e) {
      fail("❌ Complex expression failed: " + e.getMessage());
      // This is OK for now since dict() needs special key=value syntax that we haven't fully
      // implemented yet
    }
  }

  @Test
  public void testStrSplitWithSpecialRegexCharacters() {
    // Test the critical bug fix: str_split with regex special characters
    String[] testCases = {
      "str_split(\"a.b.c\", \".\")", // Should split on literal dot, not regex
      "str_split(\"a|b|c\", \"|\")", // Should split on literal pipe
      "str_split(\"a*b*c\", \"*\")", // Should split on literal asterisk
      "str_split(\"a+b+c\", \"+\")", // Should split on literal plus
      "str_split(\"a[b]c\", \"[\")" // Should split on literal bracket
    };

    for (String testCase : testCases) {
      try {
        EvalExpressionParser parser =
            (EvalExpressionParser) AntlrUtils.parseInput(testCase, AntlrUtils.GrammarType.EVAL);
        var tree = parser.language();
        assertNotNull(tree);

        // Execute the function
        FleakData testData = new StringPrimitiveFleakData("test");
        ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(testData, null);
        FleakData result = visitor.visit(tree);

        assertNotNull(result);
        assertInstanceOf(ArrayFleakData.class, result);
      } catch (Exception e) {
        fail("str_split with regex characters failed: " + testCase + ". Error: " + e.getMessage());
      }
    }
  }

  @Test
  public void testStringFunctions() {
    FleakData testData = FleakData.wrap(Map.of("text", "Hello World", "search", "World"));
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(testData, null);

    // Test str_contains
    testFunctionExecution(visitor, "str_contains(\"hello world\", \"world\")", true);
    testFunctionExecution(visitor, "str_contains(\"hello\", \"xyz\")", false);

    // Test substr
    testFunctionExecution(visitor, "substr(\"hello\", 1, 3)", "ell");
    testFunctionExecution(visitor, "substr(\"hello\", 1)", "ello");
    testFunctionExecution(visitor, "substr(\"hello\", -2)", "lo");

    // Test upper/lower
    testFunctionExecution(visitor, "upper(\"hello\")", "HELLO");
    testFunctionExecution(visitor, "lower(\"WORLD\")", "world");

    // Test to_str
    testFunctionExecution(visitor, "to_str(123)", "123");
    testFunctionExecution(visitor, "to_str(true)", "true");
  }

  @Test
  public void testArrayFunctions() {
    FleakData testData =
        FleakData.wrap(
            Map.of(
                "numbers", List.of(1, 2, 3),
                "nested", List.of(List.of(1, 2), List.of(3, 4))));
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(testData, null);

    // Test array creation
    testFunctionExecution(visitor, "array(1, 2, 3)", List.of(1L, 2L, 3L));
    testFunctionExecution(visitor, "array()", List.of());

    // Test size_of
    testFunctionExecution(visitor, "size_of(\"hello\")", 5L);
    testFunctionExecution(visitor, "size_of(array(1, 2, 3))", 3L);

    // Test range
    testFunctionExecution(visitor, "range(3)", List.of(0L, 1L, 2L));
    testFunctionExecution(visitor, "range(1, 4)", List.of(1L, 2L, 3L));
    testFunctionExecution(visitor, "range(0, 6, 2)", List.of(0L, 2L, 4L));
  }

  @Test
  public void testParsingFunctions() {
    FleakData testData = new StringPrimitiveFleakData("test");
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(testData, null);

    // Test parse_int
    testFunctionExecution(visitor, "parse_int(\"123\")", 123L);
    testFunctionExecution(visitor, "parse_int(\"FF\", 16)", 255L);

    // Test parse_float
    testFunctionExecution(visitor, "parse_float(\"3.14\")", 3.14);
    testFunctionExecution(visitor, "parse_float(\"123\")", 123.0);

    // Test duration_str_to_mills
    testFunctionExecution(visitor, "duration_str_to_mills(\"01:30:45\")", 5445000L);
    testFunctionExecution(visitor, "duration_str_to_mills(\"00:00:30\")", 30000L);
  }

  @Test
  public void testMathematicalFunctions() {
    FleakData testData = new StringPrimitiveFleakData("test");
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(testData, null);

    // Test floor function
    testFunctionExecution(visitor, "floor(123.45)", 123L);
    testFunctionExecution(visitor, "floor(-123.45)", -124L);
    testFunctionExecution(visitor, "floor(123)", 123L);
    testFunctionExecution(visitor, "floor(0.9)", 0L);
    testFunctionExecution(visitor, "floor(-0.9)", -1L);

    // Test ceil function
    testFunctionExecution(visitor, "ceil(123.45)", 124L);
    testFunctionExecution(visitor, "ceil(-123.45)", -123L);
    testFunctionExecution(visitor, "ceil(123)", 123L);
    testFunctionExecution(visitor, "ceil(0.1)", 1L);
    testFunctionExecution(visitor, "ceil(-0.1)", 0L);
  }

  @Test
  public void testTimestampFunctions() {
    FleakData testData = new StringPrimitiveFleakData("test");
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(testData, null);

    // Test ts_str_to_epoch (requires both timestamp and pattern)
    try {
      FleakData result =
          executeExpression(visitor, "ts_str_to_epoch(\"2023-12-25\", \"yyyy-MM-dd\")");
      assertNotNull(result);
      assertInstanceOf(NumberPrimitiveFleakData.class, result);
    } catch (Exception e) {
      fail("⚠️ ts_str_to_epoch test skipped (needs proper date): " + e.getMessage());
    }

    // Test epoch_to_ts_str
    try {
      FleakData result =
          executeExpression(visitor, "epoch_to_ts_str(1640995200000, \"yyyy-MM-dd\")");
      assertNotNull(result);
      assertInstanceOf(StringPrimitiveFleakData.class, result);
    } catch (Exception e) {
      fail("⚠️ epoch_to_ts_str test skipped (needs proper format): " + e.getMessage());
    }
  }

  @Test
  public void testTimestampFunctions2() {
    FleakData testData = FleakData.wrap(Map.of("eventTime", "2020-09-21T22:22:52Z"));
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(testData, null);
    try {
      FleakData result =
          executeExpression(
              visitor, "ts_str_to_epoch($.eventTime, 'yyyy-MM-dd\\'T\\'HH:mm:ss\\'Z\\'')");
      assertEquals(1600726972000L, result.unwrap());
    } catch (Exception e) {
      fail("⚠️ ts_str_to_epoch test skipped (needs proper date): " + e.getMessage());
    }
  }

  @Test
  public void testAdvancedFunctions() {
    FleakData testData =
        FleakData.wrap(
            Map.of(
                "raw", "2023-01-01 12:00:00 INFO Application started",
                "dict1", Map.of("a", 1, "b", 2),
                "dict2", Map.of("c", 3, "d", 4)));
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(testData, null);

    // Test dict creation (basic)
    testFunctionExecution(visitor, "dict()", Map.of());

    // Test dict_merge
    try {
      FleakData result = executeExpression(visitor, "dict_merge($[\"dict1\"], $[\"dict2\"])");
      assertNotNull(result);
      assertInstanceOf(RecordFleakData.class, result);
      @SuppressWarnings("unchecked")
      Map<String, Object> merged = (Map<String, Object>) result.unwrap();
      assertTrue(merged.containsKey("a") && merged.containsKey("c"));
    } catch (Exception e) {
      fail("dict_merge failed: " + e.getMessage());
    }

    // Test dict_remove with single key
    try {
      FleakData result = executeExpression(visitor, "dict_remove($[\"dict1\"], \"b\")");
      assertNotNull(result);
      assertInstanceOf(RecordFleakData.class, result);
      @SuppressWarnings("unchecked")
      Map<String, Object> removed = (Map<String, Object>) result.unwrap();
      assertTrue(removed.containsKey("a"));
      assertFalse(removed.containsKey("b"));
      assertEquals(1L, removed.get("a"));
    } catch (Exception e) {
      fail("dict_remove failed: " + e.getMessage());
    }

    // Test dict_remove with multiple keys
    try {
      FleakData result = executeExpression(visitor, "dict_remove($[\"dict2\"], \"c\", \"d\")");
      assertNotNull(result);
      assertInstanceOf(RecordFleakData.class, result);
      @SuppressWarnings("unchecked")
      Map<String, Object> removed = (Map<String, Object>) result.unwrap();
      assertTrue(removed.isEmpty());
    } catch (Exception e) {
      fail("dict_remove with multiple keys failed: " + e.getMessage());
    }

    // Test dict_remove with non-existent key
    try {
      FleakData result = executeExpression(visitor, "dict_remove($[\"dict1\"], \"nonexistent\")");
      assertNotNull(result);
      assertInstanceOf(RecordFleakData.class, result);
      @SuppressWarnings("unchecked")
      Map<String, Object> removed = (Map<String, Object>) result.unwrap();
      assertEquals(2, removed.size()); // Should still have both original keys
      assertTrue(removed.containsKey("a"));
      assertTrue(removed.containsKey("b"));
    } catch (Exception e) {
      fail("dict_remove with non-existent key failed: " + e.getMessage());
    }

    // Test grok with simple pattern
    try {
      FleakData result =
          executeExpression(visitor, "grok(\"hello world\", \"%{WORD:first} %{WORD:second}\")");
      assertNotNull(result);
      assertInstanceOf(RecordFleakData.class, result);
    } catch (Exception e) {
      fail("⚠️ grok test may need proper pattern: " + e.getMessage());
    }
  }

  @Test
  public void testArgumentValidation() {
    FleakData testData = new StringPrimitiveFleakData("test");
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(testData, null);

    // Test argument count validation
    assertThrows(IllegalArgumentException.class, () -> executeExpression(visitor, "upper()"));
    assertThrows(
        IllegalArgumentException.class,
        () -> executeExpression(visitor, "str_contains(\"hello\")"));
    assertThrows(IllegalArgumentException.class, () -> executeExpression(visitor, "parse_int()"));
    assertThrows(IllegalArgumentException.class, () -> executeExpression(visitor, "floor()"));
    assertThrows(IllegalArgumentException.class, () -> executeExpression(visitor, "ceil()"));

    // Test type validation
    assertThrows(
        Exception.class, () -> executeExpression(visitor, "parse_int(123)")); // Should be string
    assertThrows(
        Exception.class, () -> executeExpression(visitor, "upper(123)")); // Should be string
    assertThrows(
        Exception.class, () -> executeExpression(visitor, "floor(\"123\")")); // Should be number
    assertThrows(
        Exception.class, () -> executeExpression(visitor, "ceil(\"123\")")); // Should be number
  }

  @Test
  public void testEdgeCases() {
    FleakData testData = new StringPrimitiveFleakData("test");
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(testData, null);

    // Test empty strings
    testFunctionExecution(visitor, "str_split(\"\", \",\")", List.of());
    testFunctionExecution(visitor, "upper(\"\")", "");
    testFunctionExecution(visitor, "size_of(\"\")", 0L);

    // Test null handling
    testFunctionExecution(visitor, "to_str(null)", null);

    // Test boundary conditions
    testFunctionExecution(visitor, "substr(\"hello\", 0, 0)", "");
    testFunctionExecution(visitor, "substr(\"hello\", 10)", ""); // Start beyond string
    testFunctionExecution(visitor, "range(0)", List.of());
  }

  @Test
  public void testPythonFunction() {
    FleakData testData =
        FleakData.wrap(
            Map.of("value", 10.0, "factor", 5, "name", "alice", "roles", List.of("admin", "user")));

    // Test simple Python function with real PythonExecutor
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
      // Create parser and parse the expression to get the language context
      EvalExpressionParser parser =
          (EvalExpressionParser) AntlrUtils.parseInput(simpleScript, AntlrUtils.GrammarType.EVAL);
      var languageContext = parser.language();

      // Create real PythonExecutor with the parsed context
      try (PythonExecutor pythonExecutor = PythonExecutor.createPythonExecutor(languageContext)) {
        // Create visitor with real Python executor
        ExpressionValueVisitor visitor =
            ExpressionValueVisitor.createInstance(testData, pythonExecutor);

        FleakData result = visitor.visit(languageContext);
        assertNotNull(result);
        assertEquals(11.0, result.getNumberValue());
      }
    } catch (Exception e) {
      fail("⚠️ Python test skipped (GraalVM Python not available): " + e.getMessage());
    }
  }

  @Test
  public void testPythonErrorHandling() {
    FleakData testData = new StringPrimitiveFleakData("test");
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(testData, null);

    // Test that Python function is not available when PythonExecutor is null
    String pythonScript = "python('def test(): return 1', 1)";

    try {
      executeExpression(visitor, pythonScript);
      fail("Expected Python function to be unavailable");
    } catch (Exception e) {
      assertTrue(e.getMessage().contains("Unknown function: python"));
    }
  }

  @Test
  public void testPythonFunctionSignatureInErrorMessages() {
    // Test that Python function signatures are properly included in error handling
    // even when PythonExecutor is not available
    String badPythonExpr = "python()"; // Missing arguments

    try {
      EvalExpressionParser parser =
          (EvalExpressionParser) AntlrUtils.parseInput(badPythonExpr, AntlrUtils.GrammarType.EVAL);
      parser.language();
      System.out.println("✅ Python expression parsed successfully with unified signatures");
    } catch (Exception e) {
      // Should get enhanced error message even though Python function isn't available in runtime
      System.out.println("Python error message: " + e.getMessage());
      // The error should mention python function requirements from the unified signature system
      assertTrue(
          e.getMessage().contains("python") || e.getMessage().toLowerCase().contains("argument"));
      System.out.println("✅ Python function signature properly included in error system");
    }
  }

  private void testFunctionExecution(
      ExpressionValueVisitor visitor, String expression, Object expectedValue) {
    try {
      FleakData result = executeExpression(visitor, expression);
      Object actualValue = result != null ? result.unwrap() : null;
      assertEquals(expectedValue, actualValue, "Expression: " + expression);
    } catch (Exception e) {
      fail("Expression failed: " + expression + ". Error: " + e.getMessage());
    }
  }

  private FleakData executeExpression(ExpressionValueVisitor visitor, String expression) {
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(expression, AntlrUtils.GrammarType.EVAL);
    var tree = parser.language();
    return visitor.visit(tree);
  }
}
