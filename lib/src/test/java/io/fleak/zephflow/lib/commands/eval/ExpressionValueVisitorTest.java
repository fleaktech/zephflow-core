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
                ">>> Evaluation error: parse_int: argument to be parsed is not a string: null"));
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
                    ">>> Evaluation error: parse_int: failed to parse int string: abc",
                    "r_type",
                    "foo"),
                Map.of(
                    "r_id",
                    ">>> Evaluation error: parse_int: failed to parse int string: def",
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
    String floatStr = "parse_float('-3.14')";
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(floatStr, AntlrUtils.GrammarType.EVAL);
    ExpressionValueVisitor visitor =
        ExpressionValueVisitor.createInstance(FleakData.wrap(Map.of()), null);

    FleakData output = visitor.visit(parser.language());
    double expected = -3.14d;
    assertEquals(expected, output.unwrap());
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
    assertEquals(List.of(0, 1, 2, 3, 4), outputEvent.unwrap());

    evalExpr = "range(-2)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(List.of(), outputEvent.unwrap());

    evalExpr = "range(2, 5)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(List.of(2, 3, 4), outputEvent.unwrap());

    evalExpr = "range(0, 10, 2)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(List.of(0, 2, 4, 6, 8), outputEvent.unwrap());

    evalExpr = "range(10, 0, -2)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(List.of(10, 8, 6, 4, 2), outputEvent.unwrap());

    evalExpr = "range(5, 0, -1)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(List.of(5, 4, 3, 2, 1), outputEvent.unwrap());

    evalExpr = "range(0, 5, -1)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(List.of(), outputEvent.unwrap());

    evalExpr = "range(0, -5, -1)";
    outputEvent = evaluate(evalExpr, inputEvent);
    assertEquals(List.of(0, -1, -2, -3, -4), outputEvent.unwrap());
  }

  private FleakData evaluate(String evalExpr, FleakData inputEvent) {
    ExpressionValueVisitor visitor = ExpressionValueVisitor.createInstance(inputEvent, null);
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(evalExpr, AntlrUtils.GrammarType.EVAL);
    return visitor.visit(parser.language());
  }
}
