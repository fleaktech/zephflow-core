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
import org.junit.jupiter.api.Test;

/** Created by bolei on 1/27/25 */
class AntlrUtilsTest {
  @Test
  public void testParseError() {
    String badExpr =
        """
dict(
  x=dict(
    k=parse_int()
  )
)
""";
    EvalExpressionParser parser =
        (EvalExpressionParser) AntlrUtils.parseInput(badExpr, AntlrUtils.GrammarType.EVAL);
    RuntimeException exception = assertThrows(RuntimeException.class, parser::language);
    AntlrUtils.ParseErrorDto errorDto =
        fromJsonString(exception.getMessage(), new TypeReference<>() {});
    assertNotNull(errorDto);
    assertEquals(3, errorDto.getLine());
    assertEquals(16, errorDto.getCharPositionInLine());
    assertTrue(errorDto.getMessage().startsWith("mismatched input ')'"));
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
}
