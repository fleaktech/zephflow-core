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

class FeelFunctionTest {

  @Test
  public void testNewFunctionArchitecture() {
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
    String expr = "upper(\"hello\")";

    try {
      FleakData testData = new StringPrimitiveFleakData("test");
      FleakData result = evaluateExpression(expr, testData);
      assertNotNull(result);
      assertEquals("HELLO", result.getStringValue());
    } catch (Exception e) {
      fail("Function execution failed: " + e.getMessage());
    }
  }

  @Test
  public void testImprovedErrorMessages() {
    String badExpr = "ts_str_to_epoch($[\"field\"])";

    try {
      EvalExpressionParser parser =
          (EvalExpressionParser) AntlrUtils.parseInput(badExpr, AntlrUtils.GrammarType.EVAL);
      var tree = parser.language();

      assertNotNull(tree);

      FleakData testData = new StringPrimitiveFleakData("test");
      try {
        evaluateExpression(badExpr, testData);
        fail("Expected function execution to fail due to insufficient arguments");
      } catch (Exception executionError) {
        System.out.println(executionError.getMessage());
        assertTrue(executionError.getMessage().contains("ts_str_to_epoch"));
        assertTrue(executionError.getMessage().contains("2 arguments"));
      }

    } catch (Exception parseError) {
      fail("Parsing should succeed with new generic grammar: " + parseError.getMessage());
    }
  }

  @Test
  public void testComplexExpression() {
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
      fail("Complex expression failed: " + e.getMessage());
    }
  }

  @Test
  public void testStrSplitWithSpecialRegexCharacters() {
    String[] testCases = {
      "str_split(\"a.b.c\", \".\")",
      "str_split(\"a|b|c\", \"|\")",
      "str_split(\"a*b*c\", \"*\")",
      "str_split(\"a+b+c\", \"+\")",
      "str_split(\"a[b]c\", \"[\")"
    };

    FleakData testData = new StringPrimitiveFleakData("test");

    for (String testCase : testCases) {
      try {
        FleakData result = evaluateExpression(testCase, testData);
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

    testFunctionExecution(testData, "str_contains(\"hello world\", \"world\")", true);
    testFunctionExecution(testData, "str_contains(\"hello\", \"xyz\")", false);

    testFunctionExecution(testData, "substr(\"hello\", 1, 3)", "ell");
    testFunctionExecution(testData, "substr(\"hello\", 1)", "ello");
    testFunctionExecution(testData, "substr(\"hello\", -2)", "lo");

    testFunctionExecution(testData, "upper(\"hello\")", "HELLO");
    testFunctionExecution(testData, "lower(\"WORLD\")", "world");

    testFunctionExecution(testData, "to_str(123)", "123");
    testFunctionExecution(testData, "to_str(true)", "true");
  }

  @Test
  public void testStrContainsNullHandling() {
    FleakData testData = FleakData.wrap(Map.of("text", "Hello World"));

    testFunctionExecution(testData, "str_contains(null, null)", false);
    testFunctionExecution(testData, "str_contains(null, \"world\")", false);
    testFunctionExecution(testData, "str_contains(\"hello\", null)", false);
    testFunctionExecution(testData, "str_contains($.nonexistent_field, \"test\")", false);
    testFunctionExecution(testData, "str_contains(\"hello world\", \"world\")", true);
  }

  @Test
  public void testArrayFunctions() {
    FleakData testData =
        FleakData.wrap(
            Map.of(
                "numbers", List.of(1, 2, 3),
                "nested", List.of(List.of(1, 2), List.of(3, 4))));

    testFunctionExecution(testData, "array(1, 2, 3)", List.of(1L, 2L, 3L));
    testFunctionExecution(testData, "array()", List.of());

    testFunctionExecution(testData, "size_of(\"hello\")", 5L);
    testFunctionExecution(testData, "size_of(array(1, 2, 3))", 3L);

    testFunctionExecution(testData, "range(3)", List.of(0L, 1L, 2L));
    testFunctionExecution(testData, "range(1, 4)", List.of(1L, 2L, 3L));
    testFunctionExecution(testData, "range(0, 6, 2)", List.of(0L, 2L, 4L));
  }

  @Test
  public void testParsingFunctions() {
    FleakData testData = new StringPrimitiveFleakData("test");

    testFunctionExecution(testData, "parse_int(\"123\")", 123L);
    testFunctionExecution(testData, "parse_int(\"FF\", 16)", 255L);

    testFunctionExecution(testData, "parse_float(\"3.14\")", 3.14);
    testFunctionExecution(testData, "parse_float(\"123\")", 123.0);

    testFunctionExecution(testData, "duration_str_to_mills(\"01:30:45\")", 5445000L);
    testFunctionExecution(testData, "duration_str_to_mills(\"00:00:30\")", 30000L);
  }

  @Test
  public void testMathematicalFunctions() {
    FleakData testData = new StringPrimitiveFleakData("test");

    testFunctionExecution(testData, "floor(123.45)", 123L);
    testFunctionExecution(testData, "floor(-123.45)", -124L);
    testFunctionExecution(testData, "floor(123)", 123L);
    testFunctionExecution(testData, "floor(0.9)", 0L);
    testFunctionExecution(testData, "floor(-0.9)", -1L);

    testFunctionExecution(testData, "ceil(123.45)", 124L);
    testFunctionExecution(testData, "ceil(-123.45)", -123L);
    testFunctionExecution(testData, "ceil(123)", 123L);
    testFunctionExecution(testData, "ceil(0.1)", 1L);
    testFunctionExecution(testData, "ceil(-0.1)", 0L);
  }

  @Test
  public void testTimestampFunctions() {
    FleakData testData = new StringPrimitiveFleakData("test");

    try {
      FleakData result =
          evaluateExpression("ts_str_to_epoch(\"2023-12-25\", \"yyyy-MM-dd\")", testData);
      assertNotNull(result);
      assertInstanceOf(NumberPrimitiveFleakData.class, result);
    } catch (Exception e) {
      fail("ts_str_to_epoch test failed: " + e.getMessage());
    }

    try {
      FleakData result =
          evaluateExpression("epoch_to_ts_str(1640995200000, \"yyyy-MM-dd\")", testData);
      assertNotNull(result);
      assertInstanceOf(StringPrimitiveFleakData.class, result);
    } catch (Exception e) {
      fail("epoch_to_ts_str test failed: " + e.getMessage());
    }
  }

  @Test
  public void testTimestampFunctions2() {
    FleakData testData = FleakData.wrap(Map.of("eventTime", "2020-09-21T22:22:52Z"));
    try {
      FleakData result =
          evaluateExpression(
              "ts_str_to_epoch($.eventTime, 'yyyy-MM-dd\\'T\\'HH:mm:ss\\'Z\\'')", testData);
      assertEquals(1600726972000L, result.unwrap());
    } catch (Exception e) {
      fail("ts_str_to_epoch test failed: " + e.getMessage());
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

    testFunctionExecution(testData, "dict()", Map.of());

    try {
      FleakData result = evaluateExpression("dict_merge($[\"dict1\"], $[\"dict2\"])", testData);
      assertNotNull(result);
      assertInstanceOf(RecordFleakData.class, result);
      @SuppressWarnings("unchecked")
      Map<String, Object> merged = (Map<String, Object>) result.unwrap();
      assertTrue(merged.containsKey("a") && merged.containsKey("c"));
    } catch (Exception e) {
      fail("dict_merge failed: " + e.getMessage());
    }

    try {
      FleakData result = evaluateExpression("dict_remove($[\"dict1\"], \"b\")", testData);
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

    try {
      FleakData result = evaluateExpression("dict_remove($[\"dict2\"], \"c\", \"d\")", testData);
      assertNotNull(result);
      assertInstanceOf(RecordFleakData.class, result);
      @SuppressWarnings("unchecked")
      Map<String, Object> removed = (Map<String, Object>) result.unwrap();
      assertTrue(removed.isEmpty());
    } catch (Exception e) {
      fail("dict_remove with multiple keys failed: " + e.getMessage());
    }

    try {
      FleakData result = evaluateExpression("dict_remove($[\"dict1\"], \"nonexistent\")", testData);
      assertNotNull(result);
      assertInstanceOf(RecordFleakData.class, result);
      @SuppressWarnings("unchecked")
      Map<String, Object> removed = (Map<String, Object>) result.unwrap();
      assertEquals(2, removed.size());
      assertTrue(removed.containsKey("a"));
      assertTrue(removed.containsKey("b"));
    } catch (Exception e) {
      fail("dict_remove with non-existent key failed: " + e.getMessage());
    }

    try {
      FleakData result =
          evaluateExpression("grok(\"hello world\", \"%{WORD:first} %{WORD:second}\")", testData);
      assertNotNull(result);
      assertInstanceOf(RecordFleakData.class, result);
    } catch (Exception e) {
      fail("grok test failed: " + e.getMessage());
    }
  }

  @Test
  public void testArgumentValidation() {
    FleakData testData = new StringPrimitiveFleakData("test");

    assertThrows(IllegalArgumentException.class, () -> evaluateExpression("upper()", testData));
    assertThrows(
        IllegalArgumentException.class,
        () -> evaluateExpression("str_contains(\"hello\")", testData));
    assertThrows(IllegalArgumentException.class, () -> evaluateExpression("parse_int()", testData));
    assertThrows(IllegalArgumentException.class, () -> evaluateExpression("floor()", testData));
    assertThrows(IllegalArgumentException.class, () -> evaluateExpression("ceil()", testData));

    assertThrows(Exception.class, () -> evaluateExpression("parse_int(123)", testData));
    assertThrows(Exception.class, () -> evaluateExpression("upper(123)", testData));
    assertThrows(Exception.class, () -> evaluateExpression("floor(\"123\")", testData));
    assertThrows(Exception.class, () -> evaluateExpression("ceil(\"123\")", testData));
  }

  @Test
  public void testEdgeCases() {
    FleakData testData = new StringPrimitiveFleakData("test");

    testFunctionExecution(testData, "str_split(\"\", \",\")", List.of());
    testFunctionExecution(testData, "upper(\"\")", "");
    testFunctionExecution(testData, "size_of(\"\")", 0L);

    testFunctionExecution(testData, "to_str(null)", null);

    testFunctionExecution(testData, "substr(\"hello\", 0, 0)", "");
    testFunctionExecution(testData, "substr(\"hello\", 10)", "");
    testFunctionExecution(testData, "range(0)", List.of());
  }

  @Test
  public void testNullHandlingAcrossFunctions() {
    FleakData testData = FleakData.wrap(Map.of("text", "Hello World"));

    testFunctionExecution(testData, "upper(null)", null);
    testFunctionExecution(testData, "upper($.nonexistent)", null);
    testFunctionExecution(testData, "lower(null)", null);
    testFunctionExecution(testData, "lower($.nonexistent)", null);
    testFunctionExecution(testData, "size_of(null)", null);
    testFunctionExecution(testData, "size_of($.nonexistent)", null);
    testFunctionExecution(testData, "substr(null, 1, 3)", null);
    testFunctionExecution(testData, "substr($.nonexistent, 1)", null);
  }

  @Test
  public void testStrSplitNullHandling() {
    FleakData testData = FleakData.wrap(Map.of("text", "a,b,c"));

    testFunctionExecution(testData, "str_split(null, \",\")", null);
    testFunctionExecution(testData, "str_split($.nonexistent, \",\")", null);
    testFunctionExecution(testData, "str_split(\"a,b,c\", null)", List.of("a,b,c"));
    testFunctionExecution(testData, "str_split(null, null)", null);
    testFunctionExecution(testData, "str_split($.text, \",\")", List.of("a", "b", "c"));
    testFunctionExecution(
        testData,
        """
        str_split("a,b,,", ",")
        """,
        List.of("a", "b", "", ""));
    testFunctionExecution(
        testData,
        """
        str_split(",a,b", ",")
        """,
        List.of("", "a", "b"));
    testFunctionExecution(
        testData,
        """
        str_split(",,", ",")
        """,
        List.of("", "", ""));
    testFunctionExecution(
        testData,
        """
        str_split("a\\"b\\"c", "\\"")
        """,
        List.of("a", "b", "c"));
  }

  @Test
  public void testDictRemoveNullHandling() {
    FleakData testData =
        FleakData.wrap(Map.of("dict1", Map.of("a", 1, "b", 2, "c", 3), "key", "b"));

    testFunctionExecution(testData, "dict_remove(null, \"a\")", null);
    testFunctionExecution(testData, "dict_remove($.nonexistent, \"a\")", null);
    testFunctionExecution(
        testData, "dict_remove($.dict1, null)", Map.of("a", 1L, "b", 2L, "c", 3L));
    testFunctionExecution(testData, "dict_remove($.dict1, \"b\", null, \"c\")", Map.of("a", 1L));
    testFunctionExecution(testData, "dict_remove($.dict1, $.key)", Map.of("a", 1L, "c", 3L));
  }

  @Test
  public void testNowFunction() {
    FleakData testData = new RecordFleakData();

    FleakData result = evaluateExpression("now()", testData);
    assertNotNull(result);
    assertInstanceOf(NumberPrimitiveFleakData.class, result);

    long currentTime = System.currentTimeMillis();
    long resultTime = (long) result.getNumberValue();

    assertTrue(resultTime > 0);
    assertTrue(Math.abs(currentTime - resultTime) < 1000);
  }

  @Test
  public void testRandomLongFunction() {
    FleakData testData = new RecordFleakData();

    FleakData result1 = evaluateExpression("random_long()", testData);
    assertNotNull(result1);
    assertInstanceOf(NumberPrimitiveFleakData.class, result1);

    FleakData result2 = evaluateExpression("random_long()", testData);
    assertNotNull(result2);
    assertInstanceOf(NumberPrimitiveFleakData.class, result2);

    long randomValue1 = (long) result1.getNumberValue();
    long randomValue2 = (long) result2.getNumberValue();

    assertNotEquals(randomValue1, randomValue2);
  }

  @Test
  public void testRandomLongFunctionNoArguments() {
    FleakData testData = new RecordFleakData();

    assertThrows(
        IllegalArgumentException.class, () -> evaluateExpression("random_long(123)", testData));
  }

  @Test
  public void testArrFindFunction() {
    FleakData testData =
        FleakData.wrap(
            Map.of(
                "users",
                    List.of(
                        Map.of("name", "Alice", "id", "100"),
                        Map.of("name", "Bob", "id", "200"),
                        Map.of("name", "Charlie", "id", "300")),
                "products",
                    List.of(
                        Map.of("price", 100, "category", "A"),
                        Map.of("price", 200, "category", "B"),
                        Map.of("price", 150, "category", "A"))));

    FleakData result1 = evaluateExpression("arr_find($.users, user, user.id == \"100\")", testData);
    assertEquals(FleakData.wrap(Map.of("name", "Alice", "id", "100")), result1);

    FleakData result2 =
        evaluateExpression("arr_find($.products, item, item.price > 100)", testData);
    assertEquals(FleakData.wrap(Map.of("category", "B", "price", 200)), result2);

    FleakData result3 =
        evaluateExpression("arr_find($.products, item, item.category == \"A\")", testData);
    assertEquals(FleakData.wrap(Map.of("price", 100, "category", "A")), result3);
  }

  @Test
  public void testArrFindNoMatch() {
    FleakData testData =
        FleakData.wrap(
            Map.of(
                "items",
                List.of(
                    Map.of("price", 100, "category", "A"), Map.of("price", 200, "category", "B"))));

    FleakData result =
        evaluateExpression("arr_find($.items, item, item.category == \"C\")", testData);
    assertNull(result);
  }

  @Test
  public void testArrFindEmptyArray() {
    FleakData testData = FleakData.wrap(Map.of("items", List.of()));

    FleakData result = evaluateExpression("arr_find($.items, item, item.price > 0)", testData);
    assertNull(result);
  }

  @Test
  public void testArrFindWithNullSafety() {
    FleakData testData =
        FleakData.wrap(
            Map.of(
                "users",
                List.of(Map.of("name", "Alice", "id", "100"), Map.of("name", "Bob", "id", "200"))));

    String expr = "arr_find($.users, user, user.id == '100').name";

    FleakData result = evaluateExpression(expr, testData);
    assertNotNull(result);
    assertEquals("Alice", result.getStringValue());

    String expr2 = "arr_find($.users, user, user.id == \"999\").name";

    FleakData result2 = evaluateExpression(expr2, testData);
    assertNull(result2);

    String expr3 = "arr_find($.bad_field, user, user.id == '100').name";
    FleakData result3 = evaluateExpression(expr3, testData);
    assertNull(result3);
  }

  @Test
  public void testArrFilterFunction() {
    FleakData testData =
        FleakData.wrap(
            Map.of(
                "items",
                    List.of(
                        Map.of("price", 100, "category", "A"),
                        Map.of("price", 200, "category", "B"),
                        Map.of("price", 150, "category", "A"),
                        Map.of("price", 300, "category", "C")),
                "numbers", List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));

    FleakData result1 =
        evaluateExpression("arr_filter($.items, item, item.category == \"A\")", testData);
    assertEquals(
        FleakData.wrap(
            List.of(Map.of("price", 100, "category", "A"), Map.of("price", 150, "category", "A"))),
        result1);

    FleakData result2 =
        evaluateExpression("arr_filter($.items, item, item.price >= 150)", testData);
    assertEquals(
        FleakData.wrap(
            List.of(
                Map.of("price", 200, "category", "B"),
                Map.of("price", 150, "category", "A"),
                Map.of("price", 300, "category", "C"))),
        result2);

    FleakData result3 = evaluateExpression("arr_filter($.numbers, n, n % 2 == 0)", testData);
    assertEquals(FleakData.wrap(List.of(2, 4, 6, 8, 10)), result3);
  }

  @Test
  public void testArrFilterNoMatch() {
    FleakData testData =
        FleakData.wrap(
            Map.of(
                "items",
                List.of(
                    Map.of("price", 100, "category", "A"), Map.of("price", 200, "category", "B"))));

    FleakData result =
        evaluateExpression("arr_filter($.items, item, item.category == \"C\")", testData);
    assertEquals(FleakData.wrap(List.of()), result);

    FleakData result2 =
        evaluateExpression("arr_filter($.bad_array, item, item.category == \"C\")", testData);
    assertEquals(FleakData.wrap(List.of()), result2);
  }

  @Test
  public void testArrFilterEmptyArray() {
    FleakData testData = FleakData.wrap(Map.of("items", List.of()));

    FleakData result = evaluateExpression("arr_filter($.items, item, item.price > 0)", testData);
    assertEquals(FleakData.wrap(List.of()), result);
  }

  @Test
  public void testArrFilterWithIndexAccess() {
    FleakData testData =
        FleakData.wrap(
            Map.of(
                "items",
                List.of(
                    Map.of("price", 100, "category", "A"), Map.of("price", 150, "category", "A"))));

    String expr = "arr_filter($.items, item, item.category == \"A\")[0].price";
    FleakData result = evaluateExpression(expr, testData);
    assertNotNull(result);
    assertEquals(100L, result.unwrap());
  }

  @Test
  public void testArrFindAndFilterArgumentValidation() {
    FleakData testData = FleakData.wrap(Map.of("items", List.of(1, 2, 3)));

    assertThrows(
        IllegalArgumentException.class,
        () -> evaluateExpression("arr_find($.items, item)", testData));

    assertThrows(
        IllegalArgumentException.class, () -> evaluateExpression("arr_filter($.items)", testData));

    FleakData findResult = evaluateExpression("arr_find(\"not_array\", x, x > 0)", testData);
    assertNull(findResult);

    FleakData filterResult = evaluateExpression("arr_filter(\"not_array\", x, x > 0)", testData);
    assertEquals(List.of(), filterResult.unwrap());
  }

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
      System.out.println("Python expression parsed successfully with unified signatures");
    } catch (Exception e) {
      System.out.println("Python error message: " + e.getMessage());
      assertTrue(
          e.getMessage().contains("python") || e.getMessage().toLowerCase().contains("argument"));
      System.out.println("Python function signature properly included in error system");
    }
  }

  @Test
  public void testArrForeach_firstArgNull() {
    FleakData testData = FleakData.wrap(Map.of());

    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                evaluateExpression("arr_foreach($.items, item, item.category == \"C\")", testData));
    assertEquals(
        "arr_foreach: first argument should be an array or object but found: null", e.getMessage());
  }

  private void testFunctionExecution(FleakData testData, String expression, Object expectedValue) {
    try {
      FleakData result = evaluateExpression(expression, testData);
      Object actualValue = result != null ? result.unwrap() : null;
      assertEquals(expectedValue, actualValue, "Expression: " + expression);
    } catch (Exception e) {
      fail("Expression failed: " + expression + ". Error: " + e.getMessage());
    }
  }

  private FleakData evaluateExpression(String expression, FleakData testData) {
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(expression, AntlrUtils.GrammarType.EVAL);
    var tree = parser.language();
    CompiledExpression compiled = ExpressionCompiler.compile(tree, null);
    return compiled.evaluate(testData);
  }
}
