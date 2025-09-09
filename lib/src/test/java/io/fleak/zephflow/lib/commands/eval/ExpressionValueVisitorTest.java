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

import static io.fleak.zephflow.lib.utils.JsonUtils.*;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.BooleanPrimitiveFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.python.PythonExecutor;
import io.fleak.zephflow.lib.utils.AntlrUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

/** Created by bolei on 1/27/25 */
class ExpressionValueVisitorTest {
  @Test
  public void testEvaluateExpression_runtimeFailure() {
    String evalExpr = "dict(x=dict(k=parse_int($.abc)))";
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(evalExpr, AntlrUtils.GrammarType.EVAL);
    ExpressionValueVisitor visitor =
        ExpressionValueVisitor.createInstance(FleakData.wrap(Map.of()), true, null);

    // input event doesn't have $.abc, parse_int will fail at runtime
    FleakData output = visitor.visit(parser.language());
    Map<String, Object> expected =
        Map.of(
            "x",
            Map.of(
                "k",
                ">>> Failed to evaluate expression: parse_int($.abc). Reason: parse_int: argument to be parsed is not a string: null"));
    assertEquals(expected, output.unwrap());
  }

  @Test
  public void testEvaluateExpression_runtimeErrorInArrayForeach() {
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
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(evalExpr, AntlrUtils.GrammarType.EVAL);

    ExpressionValueVisitor visitor =
        ExpressionValueVisitor.createInstance(
            FleakData.wrap(
                Map.of(
                    "devices",
                    List.of(
                        Map.of("id", "abc", "type", "foo"), Map.of("id", "def", "type", "bar")))),
            true,
            null);

    // input data d.id is a string, parse_int will fail
    FleakData output = visitor.visit(parser.language());
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
  public void test_goodExpression() {
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
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(evalExpr, AntlrUtils.GrammarType.EVAL);

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

    System.out.println(toJsonString(inputPayload));

    ExpressionValueVisitor visitor =
        ExpressionValueVisitor.createInstance(FleakData.wrap(inputPayload), true, null);
    FleakData output = visitor.visit(parser.primary());
    Map<String, Object> expected =
        Map.of(
            "version",
            "TLSv1.3",
            "fingerprints",
            List.of(
                Map.of(
                    "value", "v1",
                    "algorithm", "SHA-256",
                    "algorithm_id", 3)),
            "resources",
            List.of(Map.of("r_id", 1, "r_type", "foo"), Map.of("r_id", 2, "r_type", "bar")),
            "k1",
            3,
            "k2",
            false,
            "k3",
            false,
            "type_uid",
            300201);

    System.out.println(toJsonString(expected));

    assertEquals(FleakData.wrap(expected), output);
  }

  @Test
  public void test_dictKvValueIsExpression() {
    String evalExpr = "dict(x=$.a and $.b)";
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(evalExpr, AntlrUtils.GrammarType.EVAL);
    ExpressionValueVisitor visitor =
        ExpressionValueVisitor.createInstance(
            FleakData.wrap(Map.of("a", true, "b", true)), true, null);

    // input event doesn't have $.abc, parse_int will fail at runtime
    FleakData output = visitor.visit(parser.language());
    Map<String, Object> expected = Map.of("x", true);
    assertEquals(expected, output.unwrap());
  }

  @Test
  public void testStringConcatenation() {
    String evalExpr = "$.a + '_'+ $.b";
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(evalExpr, AntlrUtils.GrammarType.EVAL);
    ExpressionValueVisitor visitor =
        ExpressionValueVisitor.createInstance(FleakData.wrap(Map.of("a", "a", "b", "b")), null);

    FleakData output = visitor.visit(parser.language());
    String expected = "a_b";
    assertEquals(expected, output.unwrap());
  }

  @Test
  public void testParseFloat() {
    String evalExpr;
    FleakData inputEvent = FleakData.wrap(Map.of());
    FleakData outputEvent;

    evalExpr = "parse_float('-3.14')";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(-3.14d, outputEvent.unwrap());

    evalExpr = "parse_float('1,997.99')";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(1997.99d, outputEvent.unwrap());

    evalExpr = "parse_float('1,999.00')";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(1999.00d, outputEvent.unwrap());

    evalExpr = "parse_float('3,498.00')";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(3498.00d, outputEvent.unwrap());
  }

  @Test
  public void testParseInt() {
    String evalExpr;
    FleakData inputEvent = FleakData.wrap(Map.of());
    FleakData outputEvent;

    evalExpr = "parse_int('123')";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(123L, outputEvent.unwrap());

    evalExpr = "parse_int('-456')";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(-456L, outputEvent.unwrap());

    evalExpr = "parse_int('0')";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(0L, outputEvent.unwrap());

    evalExpr = "parse_int('789', 10)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(789L, outputEvent.unwrap());

    evalExpr = "parse_int('1010', 2)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(10L, outputEvent.unwrap());

    evalExpr = "parse_int('77', 8)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(63L, outputEvent.unwrap());

    evalExpr = "parse_int('FF', 16)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(255L, outputEvent.unwrap());

    evalExpr = "parse_int('abc', 16)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(2748L, outputEvent.unwrap());

    evalExpr = "parse_int('ZZ', 36)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(1295L, outputEvent.unwrap());
  }

  @Test
  public void testArrForeach_ObjectAsArray() {
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
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(evalExpr, AntlrUtils.GrammarType.EVAL);
    String inputEventJsonStr =
"""
{
  "integration": "snmp",
  "attachments": {
    "snmp_pdf": "s3://a.pdf",
    "f1": "s3://b.pdf"
  },
  "operation_system": "windows"
}""";

    FleakData inputEvent = loadFleakDataFromJsonString(inputEventJsonStr);
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(inputEvent, null);
    FleakData actual = visitor.visit(parser.language());

    String outputEventJsonStr =
"""
[
  {
    "source": "snmp",
    "osVersion": "windows",
    "pdf_attachment": "s3://a.pdf"
  }
]""";
    FleakData expected = loadFleakDataFromJsonString(outputEventJsonStr);
    assertEquals(expected, actual);
  }

  @Test
  public void testEquality() {
    String evalExpr = "$.type == 'even'";
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(evalExpr, AntlrUtils.GrammarType.EVAL);

    String inputEventJsonStr =
"""
{
 "type": "odd",
 "num": 1
}""";
    FleakData inputEvent = loadFleakDataFromJsonString(inputEventJsonStr);
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(inputEvent, null);
    FleakData actual = visitor.visit(parser.language());
    assertInstanceOf(BooleanPrimitiveFleakData.class, actual);
    assertFalse(actual.isTrueValue());
  }

  @Test
  public void testFunctionResultPath1() {
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
    FleakData inputEvent = loadFleakDataFromJsonString(inputEventJsonStr);
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(inputEvent, null);
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(evalExpr, AntlrUtils.GrammarType.EVAL);
    FleakData actual = visitor.visit(parser.language());
    assertEquals("C:\\Windows\\System32", actual.getStringValue());
  }

  @Test
  public void testFunctionResultPath2() {
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
    String inputEventJsonStr =
"""
{
  "integration": "snmp",
  "attachments": {
    "snmp_pdf": "s3://a.pdf",
    "f1": "s3://b.pdf"
  },
  "operation_system": "windows"
}""";
    FleakData inputEvent = loadFleakDataFromJsonString(inputEventJsonStr);
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(inputEvent, null);
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(evalExpr, AntlrUtils.GrammarType.EVAL);
    FleakData actual = visitor.visit(parser.language());
    assertEquals("windows", actual.getStringValue());
  }

  @Test
  public void testStrSplit() {
    String evalExpr =
"""
str_split("a,b,c", ",")
""";
    FleakData inputEvent = FleakData.wrap(Map.of());
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(inputEvent, null);
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(evalExpr, AntlrUtils.GrammarType.EVAL);
    FleakData actual = visitor.visit(parser.language());
    assertEquals(FleakData.wrap(List.of("a", "b", "c")), actual);
  }

  @Test
  public void testArrayNull() {
    String evalExpr = "array(null, 5)";
    FleakData inputEvent = FleakData.wrap(Map.of());
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(inputEvent, null);
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(evalExpr, AntlrUtils.GrammarType.EVAL);
    FleakData actual = visitor.visit(parser.language());
    ArrayList<FleakData> arrPayload = new ArrayList<>();
    arrPayload.add(null);
    arrPayload.add(FleakData.wrap(5));
    assertEquals(FleakData.wrap(arrPayload), actual);
  }

  @Test
  public void testEmptyDict() {
    String evalExpr = "dict()";
    FleakData inputEvent = FleakData.wrap(Map.of());
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(inputEvent, null);
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(evalExpr, AntlrUtils.GrammarType.EVAL);
    FleakData actual = visitor.visit(parser.language());
    assertInstanceOf(RecordFleakData.class, actual);
    assertTrue(actual.getPayload().isEmpty());
  }

  @Test
  public void testDictKeyWithDot() {
    String evalExpr = "dict(a.b=5)";
    FleakData inputEvent = FleakData.wrap(Map.of());
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(inputEvent, null);
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(evalExpr, AntlrUtils.GrammarType.EVAL);
    FleakData actual = visitor.visit(parser.language());
    assertInstanceOf(RecordFleakData.class, actual);
    assertEquals(Map.of("a.b", 5L), actual.unwrap());
  }

  @Test
  public void testDictKeyWithDoubleColons() {
    String evalExpr = "dict(a::b=5)";
    FleakData inputEvent = FleakData.wrap(Map.of());
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(inputEvent, null);
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(evalExpr, AntlrUtils.GrammarType.EVAL);
    FleakData actual = visitor.visit(parser.language());
    assertInstanceOf(RecordFleakData.class, actual);
    assertEquals(Map.of("a::b", 5L), actual.unwrap());
  }

  @Test
  public void testDictKeyWithSpecialChars() {
    String evalExpr =
"""
  dict(
      "key with spaces" = "value1",
      "key.with.dots" = "value2",
      "key-with-dashes" = "value3",
      "key with.special ^&* characters" = "value4",
      "key with \\"escaped quotes\\"" = "value5",
      "key with 'single quotes'" = "value6",
      "123numeric_start" = "value7",
      "special@#$%^&*()chars" = "value8",
      normalKey = 100,
      user.profile = $.user,
      config::db = "localhost"
  )
""";
    FleakData inputEvent = FleakData.wrap(Map.of("user", "test_user"));

    FleakData outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(
        ImmutableMap.builder()
            .put("\"key with spaces\"", "value1")
            .put("\"key.with.dots\"", "value2")
            .put("\"key-with-dashes\"", "value3")
            .put("\"key with.special ^&* characters\"", "value4")
            .put("\"key with \\\"escaped quotes\\\"\"", "value5")
            .put("\"key with 'single quotes'\"", "value6")
            .put("\"123numeric_start\"", "value7")
            .put("\"special@#$%^&*()chars\"", "value8")
            .put("normalKey", 100L)
            .put("user.profile", "test_user")
            .put("config::db", "localhost")
            .build(),
        outputEvent.unwrap());
  }

  @Test
  public void testProblematicPython() {
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
  public void testRangeFunc() {
    String evalExpr;
    FleakData outputEvent;
    FleakData inputEvent = FleakData.wrap(Map.of());

    evalExpr = "range(5)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(FleakData.wrap(List.of(0, 1, 2, 3, 4)), outputEvent);

    evalExpr = "range(-2)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(List.of(), outputEvent.unwrap());

    evalExpr = "range(2, 5)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(FleakData.wrap(List.of(2, 3, 4)), outputEvent);

    evalExpr = "range(0, 10, 2)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(FleakData.wrap(List.of(0, 2, 4, 6, 8)), outputEvent);

    evalExpr = "range(10, 0, -2)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(FleakData.wrap(List.of(10, 8, 6, 4, 2)), outputEvent);

    evalExpr = "range(5, 0, -1)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(FleakData.wrap(List.of(5, 4, 3, 2, 1)), outputEvent);

    evalExpr = "range(0, 5, -1)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(List.of(), outputEvent.unwrap());

    evalExpr = "range(0, -5, -1)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(FleakData.wrap(List.of(0, -1, -2, -3, -4)), outputEvent);
  }

  @Test
  public void testExpressionWithoutSpaces() {
    // 1-1 should be a valid expression
    String evalExpr =
"""
dict(
  a= 1-1
)
""";
    FleakData inputEvent = FleakData.wrap(Map.of());
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(inputEvent, null);
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(evalExpr, AntlrUtils.GrammarType.EVAL);
    FleakData actual = visitor.visit(parser.language());
    assertInstanceOf(RecordFleakData.class, actual);
    assertEquals(Map.of("a", 0L), actual.unwrap());

    FleakData outputEvent;
    evalExpr = "5-1";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(4L, outputEvent.unwrap());

    evalExpr = "-42";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(-42L, outputEvent.unwrap());

    evalExpr = "-3.14";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(-3.14d, outputEvent.unwrap());

    evalExpr = "10 - -5";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(15L, outputEvent.unwrap());

    evalExpr = "-(5+3)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(-8L, outputEvent.unwrap());
  }

  @Test
  public void testSubStr() {
    String evalExpr;
    FleakData outputEvent;
    FleakData inputEvent = FleakData.wrap("hello");

    evalExpr = "substr(\"hello\", 1)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals("ello", outputEvent.unwrap());
    evalExpr = "substr(\"hello\", -2)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals("lo", outputEvent.unwrap());
    evalExpr = "substr(\"hello\", 1, 2)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals("el", outputEvent.unwrap());
    evalExpr = "substr(\"hello\", -3, 2)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals("ll", outputEvent.unwrap());
    evalExpr = "substr(\"hello\", 10)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals("", outputEvent.unwrap());
    evalExpr = "substr(\"hello\", -10)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals("hello", outputEvent.unwrap());
    evalExpr = "substr(\"hello\", 2, 100)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals("llo", outputEvent.unwrap());
  }

  @Test
  public void testBooleanExpr() {
    String evalExpr;
    FleakData outputEvent;
    FleakData inputEvent = FleakData.wrap(Map.of("foo", 100));

    evalExpr = "$.a == null";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertTrue((Boolean) outputEvent.unwrap());
  }

  @Test
  public void testBadFunction() {

    String evalExpr;
    FleakData outputEvent;
    FleakData inputEvent =
        FleakData.wrap(Map.of("responseElements", Map.of("directConnectGateways", List.of())));

    evalExpr =
"""
dict(
 status=case(
    size($.responseElements.directConnectGateways) == 0 => 'Failed',
    _ => 'Success'
  )
)
""";
    outputEvent = evaluate(evalExpr, inputEvent);

    assertEquals(
        ">>> Failed to evaluate expression: case(size($.responseElements.directConnectGateways)==0=>'Failed',_=>'Success'). Reason: Unknown function: size",
        ((Map) outputEvent.unwrap()).get("status"));
  }

  @Test
  public void testTyping() {
    String evalExpr;
    FleakData outputEvent;
    FleakData inputEvent = FleakData.wrap(Map.of());

    evalExpr = "parse_int(\"1\") * 10";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(10L, outputEvent.unwrap());
  }

  @Test
  public void testAdditionTyping() {
    // LONG + LONG -> LONG
    assertEquals(15L, evaluate("10 + 5").unwrap());

    // LONG + DOUBLE -> DOUBLE
    assertEquals(15.5, evaluate("10 + 5.5").unwrap());

    // DOUBLE + LONG -> DOUBLE
    assertEquals(15.5, evaluate("10.5 + 5").unwrap());

    // DOUBLE + DOUBLE -> DOUBLE
    assertEquals(16.0, evaluate("10.5 + 5.5").unwrap());
  }

  @Test
  public void testSubtractionTyping() {
    // LONG - LONG -> LONG
    assertEquals(5L, evaluate("10 - 5").unwrap());

    // LONG - DOUBLE -> DOUBLE
    assertEquals(4.5, evaluate("10 - 5.5").unwrap());

    // DOUBLE - LONG -> DOUBLE
    assertEquals(5.5, evaluate("10.5 - 5").unwrap());

    // DOUBLE - DOUBLE -> DOUBLE
    assertEquals(5.0, evaluate("10.5 - 5.5").unwrap());
  }

  @Test
  public void testMultiplicationTyping() {
    // LONG * LONG -> LONG
    assertEquals(50L, evaluate("10 * 5").unwrap());

    // LONG * DOUBLE -> DOUBLE
    assertEquals(55.0, evaluate("10 * 5.5").unwrap());

    // DOUBLE * LONG -> DOUBLE
    assertEquals(21.0, evaluate("10.5 * 2").unwrap());

    // DOUBLE * DOUBLE -> DOUBLE
    assertEquals(21.0, evaluate("10.5 * 2.0").unwrap());
  }

  @Test
  public void testDivisionTyping() {
    // Division should always promote to DOUBLE to preserve precision.
    // LONG / LONG -> DOUBLE
    assertEquals(2.5, evaluate("10 / 4").unwrap());

    // LONG / DOUBLE -> DOUBLE
    assertEquals(4.0, evaluate("10 / 2.5").unwrap());

    // DOUBLE / LONG -> DOUBLE
    assertEquals(2.5, evaluate("12.5 / 5").unwrap());

    // DOUBLE / DOUBLE -> DOUBLE
    assertEquals(5.0, evaluate("12.5 / 2.5").unwrap());

    // Test division by zero
    assertThrows(IllegalArgumentException.class, () -> evaluate("10 / 0").unwrap());
    assertThrows(IllegalArgumentException.class, () -> evaluate("10 / 0.0").unwrap());
  }

  @Test
  public void testModuloTyping() {
    // LONG % LONG -> LONG
    assertEquals(1L, evaluate("10 % 3").unwrap());

    // LONG % DOUBLE -> DOUBLE
    assertEquals(3.0, evaluate("10 % 3.5").unwrap()); // 10.0 = 2 * 3.5 + 3.0

    // DOUBLE % LONG -> DOUBLE
    assertEquals(1.5, evaluate("10.5 % 3").unwrap()); // 10.5 = 3 * 3.0 + 1.5

    // DOUBLE % DOUBLE -> DOUBLE
    assertEquals(0.0, evaluate("10.5 % 3.5").unwrap());

    // Test modulo by zero
    assertThrows(IllegalArgumentException.class, () -> evaluate("10 % 0").unwrap());
    assertThrows(IllegalArgumentException.class, () -> evaluate("10 % 0.0").unwrap());
  }

  private FleakData evaluate(String evalExpr, FleakData inputEvent) {
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(inputEvent, true, null);
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(evalExpr, AntlrUtils.GrammarType.EVAL);
    return visitor.visit(parser.language());
  }

  private FleakData evaluate(String evalExpr) {
    ExpressionValueVisitor visitor =
        ExpressionValueVisitor.createInstance(FleakData.wrap(Map.of()), true, null);
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(evalExpr, AntlrUtils.GrammarType.EVAL);
    return visitor.visit(parser.language());
  }
}
