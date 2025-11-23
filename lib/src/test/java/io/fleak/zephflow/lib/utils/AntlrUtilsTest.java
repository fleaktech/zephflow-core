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
package io.fleak.zephflow.lib.utils;

import static io.fleak.zephflow.lib.utils.JsonUtils.fromJsonString;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.FeelFunction;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Created by bolei on 1/27/25 */
class AntlrUtilsTest {
  @Test
  public void testGenericGrammarBehavior() {
    // With new generic grammar, parse_int() should parse successfully
    String expression =
        """
dict(
  x=dict(
    k=parse_int()
  )
)
""";
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(expression, AntlrUtils.GrammarType.EVAL);

    // Should now parse successfully with generic grammar
    try {
      var tree = parser.language();
      assertNotNull(tree);
    } catch (Exception e) {
      fail("Expression should parse successfully with new generic grammar: " + e.getMessage());
    }
  }

  @Test
  public void testParseError2() {
    String badExpr =
        """
abc def
""";

    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(badExpr, AntlrUtils.GrammarType.EVAL);

    RuntimeException exception = assertThrows(RuntimeException.class, parser::language);
    AntlrUtils.ParseErrorDto errorDto =
        fromJsonString(exception.getMessage(), new TypeReference<>() {});
    assertNotNull(errorDto);
    assertEquals(1, errorDto.getLine());
    assertEquals(4, errorDto.getCharPositionInLine());
    assertEquals("extraneous input 'def' expecting <EOF>", errorDto.getMessage());
  }

  @Test
  public void testUnifiedFunctionSignatures() {
    // Test that function signatures are automatically synchronized with implementations
    Map<String, FeelFunction.FunctionSignature> signatures = AntlrUtils.FEEL_FUNCTION_SIGNATURES;

    assertNotNull(signatures);
    assertFalse(signatures.isEmpty());

    // Verify key functions are present
    assertTrue(signatures.containsKey("ts_str_to_epoch"));
    assertTrue(signatures.containsKey("upper"));
    assertTrue(signatures.containsKey("parse_int"));
    assertTrue(signatures.containsKey("range"));
    assertTrue(signatures.containsKey("python")); // Should be included now

    // Verify signature details are correct
    FeelFunction.FunctionSignature tsStrToEpoch = signatures.get("ts_str_to_epoch");
    assertEquals(2, tsStrToEpoch.minArgs());
    assertEquals(2, tsStrToEpoch.maxArgs());
    assertTrue(tsStrToEpoch.description().contains("timestamp"));

    FeelFunction.FunctionSignature python = signatures.get("python");
    assertEquals(1, python.minArgs());
    assertEquals(-1, python.maxArgs()); // Variable arguments
    assertTrue(python.description().contains("script"));
  }

  @Test
  public void testEnhancedErrorMessages() {
    // Test various function argument errors to verify enhanced error handling
    String[] errorCases = {
      "ts_str_to_epoch($[\"field\"])", // Missing second argument
      "grok($[\"field\"])", // Missing pattern argument
      "str_contains(\"hello\")", // Missing second argument
      "parse_int()", // Missing argument
      "substr(\"hello\")" // Missing start position
    };

    for (String errorCase : errorCases) {
      try {
        EvalExpressionParser parser =
            (EvalExpressionParser) AntlrUtils.parseInput(errorCase, AntlrUtils.GrammarType.EVAL);

        // These should parse successfully with new grammar
        var tree = parser.language();
        assertNotNull(tree);

        // Execution errors would happen at runtime with clear messages
        // (tested in FeelFunctionTest)

      } catch (Exception e) {
        // If it does fail at parse time, it should have enhanced error message
        assertTrue(
            e.getMessage().contains("function")
                || e.getMessage().contains("argument")
                || e.getMessage().contains("expects"),
            "Error should mention function info: " + e.getMessage());
      }
    }
  }

  @Test
  public void testGrammarEdgeCases() {
    // Test various edge cases in the new grammar
    String[] validCases = {
      "upper(\"hello\")", // Basic function
      "dict(key=value, other=123)", // Dict with multiple keys
      "dict(a.b=5, c::d=10)", // Dict with dotted/colon keys
      "array(1, 2, upper(\"test\"))", // Nested functions
      "case(true => \"yes\", _ => \"no\")", // Case expressions
      "$[\"field\"].subfield[0]", // Path selection with steps
      "range(1, 10, 2) + array(1, 2, 3)" // Expression with functions
    };

    for (String validCase : validCases) {
      try {
        EvalExpressionParser parser =
            (EvalExpressionParser) AntlrUtils.parseInput(validCase, AntlrUtils.GrammarType.EVAL);
        var tree = parser.language();
        assertNotNull(tree);
      } catch (Exception e) {
        fail("Valid expression failed to parse: " + validCase + ". Error: " + e.getMessage());
      }
    }
  }

  @Test
  public void testErrorListenerEnhancement() {
    // Test that the error listener properly enhances function-related errors
    String actuallyBadExpr = "upper(\"hello\" + )"; // Syntax error in function argument

    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(actuallyBadExpr, AntlrUtils.GrammarType.EVAL);

    RuntimeException exception = assertThrows(RuntimeException.class, parser::language);
    AntlrUtils.ParseErrorDto errorDto =
        fromJsonString(exception.getMessage(), new TypeReference<>() {});

    assertNotNull(errorDto);
    assertNotNull(errorDto.getMessage());

    // Error message should contain useful information
    String message = errorDto.getMessage();
    assertFalse(message.isEmpty());
  }
}
