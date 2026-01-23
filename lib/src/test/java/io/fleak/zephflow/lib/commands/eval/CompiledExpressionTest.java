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

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.compiled.CompiledExpression;
import io.fleak.zephflow.lib.commands.eval.compiled.ExpressionCompiler;
import io.fleak.zephflow.lib.commands.eval.python.PythonExecutor;
import io.fleak.zephflow.lib.utils.AntlrUtils;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Tests for CompiledExpression evaluation. Migrated from ExpressionValueVisitorTest.java to ensure
 * comprehensive test coverage after removing the visitor pattern.
 */
class CompiledExpressionTest {

  @Test
  void testLenientMode_runtimeFailure() {
    String evalExpr = "dict(x=dict(k=parse_int($.abc)))";
    FleakData output = evaluate(evalExpr, FleakData.wrap(Map.of()), true);
    Map<String, Object> expected =
        Map.of(
            "x",
            Map.of(
                "k",
                ">>> Failed to evaluate expression: parse_int($.abc). Reason: parse_int: argument to be parsed is not a string: null"));
    assertEquals(expected, output.unwrap());
  }

  @Test
  void testLenientMode_runtimeErrorInArrayForeach() {
    String evalExpr =
        """
dict(
   resources=arr_foreach(
      $.devices,
      d,
      dict(
         r_id = parse_int(d.id),
         r_type = d.type
      )
   )
)
""";

    FleakData input =
        FleakData.wrap(
            Map.of(
                "devices",
                List.of(Map.of("id", "abc", "type", "foo"), Map.of("id", "def", "type", "bar"))));

    FleakData output = evaluate(evalExpr, input, true);
    Map<String, Object> expected =
        Map.of(
            "resources",
            List.of(
                Map.of(
                    "r_id",
                    ">>> Failed to evaluate expression: parse_int(d.id). Reason: parse_int: failed to parse int string: abc with radix: 10",
                    "r_type",
                    "foo"),
                Map.of(
                    "r_id",
                    ">>> Failed to evaluate expression: parse_int(d.id). Reason: parse_int: failed to parse int string: def with radix: 10",
                    "r_type",
                    "bar")));
    assertEquals(expected, output.unwrap());
  }

  @Test
  void testGoodExpression() {
    String evalExpr =
        """
dict(
  version= 'TLSv1.3',
  fingerprints= array(
    dict(
      value= $.tlsDetails.cipherSuite,
      algorithm= 'SHA-256',
      algorithm_id= 3
    )
  ),
  resources=arr_foreach(
    $.devices,
    d,
    dict(
       r_id = parse_int(d.id),
       r_type = d.type
    )
 ),
 k1 = ($.x+$.y),
 k2= ($.a or $.b),
 k3 = ($.x>$.y),
 type_uid= case($.eventName == 'AssumeRole' => 300201, _ => 300299)
)
""";

    Map<String, Object> inputPayload =
        Map.of(
            "eventName",
            "AssumeRole",
            "tlsDetails",
            Map.of("cipherSuite", "v1"),
            "devices",
            List.of(Map.of("id", "1", "type", "foo"), Map.of("id", "2", "type", "bar")),
            "x",
            1,
            "y",
            2,
            "a",
            false,
            "b",
            false);

    FleakData output = evaluatePrimary(evalExpr, FleakData.wrap(inputPayload));
    Map<String, Object> expected =
        Map.of(
            "version",
            "TLSv1.3",
            "fingerprints",
            List.of(Map.of("value", "v1", "algorithm", "SHA-256", "algorithm_id", 3L)),
            "resources",
            List.of(Map.of("r_id", 1L, "r_type", "foo"), Map.of("r_id", 2L, "r_type", "bar")),
            "k1",
            3L,
            "k2",
            false,
            "k3",
            false,
            "type_uid",
            300201L);

    assertEquals(FleakData.wrap(expected), output);
  }

  @Test
  void testDictKvValueIsExpression() {
    String evalExpr = "dict(x=$.a and $.b)";
    FleakData output = evaluate(evalExpr, FleakData.wrap(Map.of("a", true, "b", true)));
    Map<String, Object> expected = Map.of("x", true);
    assertEquals(expected, output.unwrap());
  }

  @Test
  void testStringConcatenation() {
    String evalExpr = "$.a + '_'+ $.b";
    FleakData output = evaluate(evalExpr, FleakData.wrap(Map.of("a", "a", "b", "b")));
    assertEquals("a_b", output.unwrap());
  }

  @Test
  void testAdditionTyping() {
    assertEquals(15L, evaluate("10 + 5").unwrap());
    assertEquals(15.5, evaluate("10 + 5.5").unwrap());
    assertEquals(15.5, evaluate("10.5 + 5").unwrap());
    assertEquals(16.0, evaluate("10.5 + 5.5").unwrap());
  }

  @Test
  void testSubtractionTyping() {
    assertEquals(5L, evaluate("10 - 5").unwrap());
    assertEquals(4.5, evaluate("10 - 5.5").unwrap());
    assertEquals(5.5, evaluate("10.5 - 5").unwrap());
    assertEquals(5.0, evaluate("10.5 - 5.5").unwrap());
  }

  @Test
  void testMultiplicationTyping() {
    assertEquals(50L, evaluate("10 * 5").unwrap());
    assertEquals(55.0, evaluate("10 * 5.5").unwrap());
    assertEquals(21.0, evaluate("10.5 * 2").unwrap());
    assertEquals(21.0, evaluate("10.5 * 2.0").unwrap());
  }

  @Test
  void testDivisionTyping() {
    assertEquals(2.5, evaluate("10 / 4").unwrap());
    assertEquals(4.0, evaluate("10 / 2.5").unwrap());
    assertEquals(2.5, evaluate("12.5 / 5").unwrap());
    assertEquals(5.0, evaluate("12.5 / 2.5").unwrap());

    assertThrows(IllegalArgumentException.class, () -> evaluate("10 / 0").unwrap());
    assertThrows(IllegalArgumentException.class, () -> evaluate("10 / 0.0").unwrap());
  }

  @Test
  void testModuloTyping() {
    assertEquals(1L, evaluate("10 % 3").unwrap());
    assertEquals(3.0, evaluate("10 % 3.5").unwrap());
    assertEquals(1.5, evaluate("10.5 % 3").unwrap());
    assertEquals(0.0, evaluate("10.5 % 3.5").unwrap());

    assertThrows(IllegalArgumentException.class, () -> evaluate("10 % 0").unwrap());
    assertThrows(IllegalArgumentException.class, () -> evaluate("10 % 0.0").unwrap());
  }

  @Test
  void testNegativeNumbers() {
    FleakData output;
    FleakData inputEvent = FleakData.wrap(Map.of());

    output = evaluate("5-1", inputEvent);
    assertEquals(4L, output.unwrap());

    output = evaluate("-42", inputEvent);
    assertEquals(-42L, output.unwrap());

    output = evaluate("-3.14", inputEvent);
    assertEquals(-3.14d, output.unwrap());

    output = evaluate("10 - -5", inputEvent);
    assertEquals(15L, output.unwrap());

    output = evaluate("-(5+3)", inputEvent);
    assertEquals(-8L, output.unwrap());
  }

  @Test
  void testExpressionWithoutSpaces() {
    String evalExpr = "dict(a= 1-1)";
    FleakData inputEvent = FleakData.wrap(Map.of());
    FleakData actual = evaluate(evalExpr, inputEvent);
    assertInstanceOf(RecordFleakData.class, actual);
    assertEquals(Map.of("a", 0L), actual.unwrap());
  }

  @Test
  void testRangeFunc() {
    FleakData output;
    FleakData inputEvent = FleakData.wrap(Map.of());

    output = evaluate("range(5)", inputEvent);
    assertEquals(FleakData.wrap(List.of(0, 1, 2, 3, 4)), output);

    output = evaluate("range(-2)", inputEvent);
    assertEquals(List.of(), output.unwrap());

    output = evaluate("range(2, 5)", inputEvent);
    assertEquals(FleakData.wrap(List.of(2, 3, 4)), output);

    output = evaluate("range(0, 10, 2)", inputEvent);
    assertEquals(FleakData.wrap(List.of(0, 2, 4, 6, 8)), output);

    output = evaluate("range(10, 0, -2)", inputEvent);
    assertEquals(FleakData.wrap(List.of(10, 8, 6, 4, 2)), output);

    output = evaluate("range(5, 0, -1)", inputEvent);
    assertEquals(FleakData.wrap(List.of(5, 4, 3, 2, 1)), output);

    output = evaluate("range(0, 5, -1)", inputEvent);
    assertEquals(List.of(), output.unwrap());

    output = evaluate("range(0, -5, -1)", inputEvent);
    assertEquals(FleakData.wrap(List.of(0, -1, -2, -3, -4)), output);
  }

  @Test
  void testSubStr() {
    FleakData output;
    FleakData inputEvent = FleakData.wrap("hello");

    output = evaluate("substr(\"hello\", 1)", inputEvent);
    assertEquals("ello", output.unwrap());

    output = evaluate("substr(\"hello\", -2)", inputEvent);
    assertEquals("lo", output.unwrap());

    output = evaluate("substr(\"hello\", 1, 2)", inputEvent);
    assertEquals("el", output.unwrap());

    output = evaluate("substr(\"hello\", -3, 2)", inputEvent);
    assertEquals("ll", output.unwrap());

    output = evaluate("substr(\"hello\", 10)", inputEvent);
    assertEquals("", output.unwrap());

    output = evaluate("substr(\"hello\", -10)", inputEvent);
    assertEquals("hello", output.unwrap());

    output = evaluate("substr(\"hello\", 2, 100)", inputEvent);
    assertEquals("llo", output.unwrap());
  }

  @Test
  void testBooleanExprWithNull() {
    FleakData output;
    FleakData inputEvent = FleakData.wrap(Map.of("foo", 100));

    output = evaluate("$.a == null", inputEvent);
    assertTrue((Boolean) output.unwrap());
  }

  @Test
  void testDictWithSpecialKeys() {
    String evalExpr =
        """
dict(
  "key with spaces"="value1",
  "key.with.dots"="value2",
  "key-with-dashes"="value3",
  "key with.special ^&* characters"="value4",
  "key with \\"escaped quotes\\""="value5",
  "key with 'single quotes'"="value6",
  "123numeric_start"="value7",
  "special@#$%^&*()chars"="value8",
  normalKey=100,
  "user.profile"=$.user,
  "config::db"="localhost"
)
""";
    FleakData inputEvent = FleakData.wrap(Map.of("user", "test_user"));
    FleakData outputEvent = evaluate(evalExpr, inputEvent);

    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) outputEvent.unwrap();

    assertEquals("value1", result.get("key with spaces"));
    assertEquals("value2", result.get("key.with.dots"));
    assertEquals("value3", result.get("key-with-dashes"));
    assertEquals("value4", result.get("key with.special ^&* characters"));
    assertEquals("value5", result.get("key with \"escaped quotes\""));
    assertEquals("value6", result.get("key with 'single quotes'"));
    assertEquals("value7", result.get("123numeric_start"));
    assertEquals("value8", result.get("special@#$%^&*()chars"));
    assertEquals(100L, result.get("normalKey"));
    assertEquals("test_user", result.get("user.profile"));
    assertEquals("localhost", result.get("config::db"));
  }

  @Test
  void testProblematicPython() {
    String evalExpr =
        """
dict(
  pyData=python(
    'invalid python script',
    $.k
  )
)
""";

    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(evalExpr, AntlrUtils.GrammarType.EVAL);
    EvalExpressionParser.LanguageContext languageContext = parser.language();
    Exception exception =
        assertThrows(
            Exception.class,
            () -> {
              try (PythonExecutor ignored = PythonExecutor.createPythonExecutor(languageContext)) {
                fail("should not reach here");
              }
            });
    assertEquals(
        "SyntaxError: invalid syntax (compileTimeScript.py, line 1)", exception.getMessage());
  }

  @Test
  void testBadFunction() {
    String evalExpr =
        """
dict(
 status=case(
    size($.responseElements.directConnectGateways) == 0 => 'Failed',
    _ => 'Success'
  )
)
""";
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(evalExpr, AntlrUtils.GrammarType.EVAL);
    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> ExpressionCompiler.compile(parser.language(), null));
    assertEquals("Unknown function: size", exception.getMessage());
  }

  @Test
  void testBadFunction_lenientCompileMode() {
    String evalExpr =
        """
dict(
 status=case(
    size($.responseElements.directConnectGateways) == 0 => 'Failed',
    _ => 'Success'
  )
)
""";
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(evalExpr, AntlrUtils.GrammarType.EVAL);
    CompiledExpression compiled = ExpressionCompiler.compile(parser.language(), null, true);
    assertNotNull(compiled);

    FleakData input =
        FleakData.wrap(Map.of("responseElements", Map.of("directConnectGateways", List.of())));
    Exception exception =
        assertThrows(IllegalArgumentException.class, () -> compiled.evaluate(input, false));
    assertEquals("Unknown function: size", exception.getMessage());
  }

  @Test
  void testTyping() {
    FleakData output = evaluate("parse_int(\"1\") * 10", FleakData.wrap(Map.of()));
    assertEquals(10L, output.unwrap());
  }

  @Test
  void testArrayNull() {
    String evalExpr = "array(null, 5)";
    FleakData inputEvent = FleakData.wrap(Map.of());
    FleakData actual = evaluate(evalExpr, inputEvent);
    List<Object> expected = new java.util.ArrayList<>();
    expected.add(null);
    expected.add(5L);
    assertEquals(expected, actual.unwrap());
  }

  @Test
  void testEmptyDict() {
    String evalExpr = "dict()";
    FleakData inputEvent = FleakData.wrap(Map.of());
    FleakData actual = evaluate(evalExpr, inputEvent);
    assertInstanceOf(RecordFleakData.class, actual);
    assertTrue(((Map<?, ?>) actual.unwrap()).isEmpty());
  }

  @Test
  void testEquality() {
    String evalExpr = "$.type == 'even'";
    FleakData inputEvent = FleakData.wrap(Map.of("type", "odd", "num", 1));
    FleakData actual = evaluate(evalExpr, inputEvent);
    assertFalse((Boolean) actual.unwrap());

    inputEvent = FleakData.wrap(Map.of("type", "even", "num", 2));
    actual = evaluate(evalExpr, inputEvent);
    assertTrue((Boolean) actual.unwrap());
  }

  @Test
  void testFunctionResultPath1() {
    String evalExpr =
        """
grok($.proc_name, "(?<parent_folder>.*?)\\\\\\\\(?<name>[^\\\\\\\\]+)$").parent_folder
""";
    String inputEventJsonStr =
        """
{
  "proc_name": "C:\\\\Windows\\\\System32\\\\services.exe"
}
""";
    FleakData inputEvent = JsonUtils.loadFleakDataFromJsonString(inputEventJsonStr);
    FleakData actual = evaluate(evalExpr, inputEvent);
    assertEquals("C:\\Windows\\System32", actual.unwrap());
  }

  @Test
  void testFunctionResultPath2() {
    String evalExpr =
        """
arr_foreach(
    $,
    elem,
    dict(
        osVersion=elem.operation_system,
        source=$.integration,
        pdf_attachment=$.attachments.snmp_pdf
    )
)[0].osVersion""";
    FleakData inputEvent =
        FleakData.wrap(
            Map.of(
                "integration", "snmp",
                "attachments", Map.of("snmp_pdf", "s3://a.pdf", "f1", "s3://b.pdf"),
                "operation_system", "windows"));
    FleakData actual = evaluate(evalExpr, inputEvent);
    assertEquals("windows", actual.unwrap());
  }

  @Test
  void testArrForeach_ObjectAsArray() {
    String evalExpr =
        """
arr_foreach(
    $,
    elem,
    dict(
        osVersion=elem.operation_system,
        source=$.integration,
        pdf_attachment=$.attachments.snmp_pdf
    )
)""";
    FleakData inputEvent =
        FleakData.wrap(
            Map.of(
                "integration", "snmp",
                "attachments", Map.of("snmp_pdf", "s3://a.pdf"),
                "operation_system", "windows"));
    FleakData actual = evaluate(evalExpr, inputEvent);
    @SuppressWarnings("unchecked")
    List<Map<String, Object>> result = (List<Map<String, Object>>) actual.unwrap();
    assertEquals(1, result.size());
    assertEquals("windows", result.get(0).get("osVersion"));
    assertEquals("snmp", result.get(0).get("source"));
    assertEquals("s3://a.pdf", result.get(0).get("pdf_attachment"));
  }

  private FleakData evaluate(String expression) {
    return evaluate(expression, FleakData.wrap(Map.of()), false);
  }

  private FleakData evaluate(String expression, FleakData input) {
    return evaluate(expression, input, false);
  }

  private FleakData evaluate(String expression, FleakData input, boolean lenient) {
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(expression, AntlrUtils.GrammarType.EVAL);
    CompiledExpression compiled = ExpressionCompiler.compile(parser.language(), null);
    return compiled.evaluate(input, lenient);
  }

  private FleakData evaluatePrimary(String expression, FleakData input) {
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(expression, AntlrUtils.GrammarType.EVAL);
    CompiledExpression compiled = ExpressionCompiler.compile(parser.language(), null);
    return compiled.evaluate(input);
  }
}
