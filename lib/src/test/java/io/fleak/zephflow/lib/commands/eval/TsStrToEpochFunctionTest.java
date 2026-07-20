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
import io.fleak.zephflow.lib.utils.AntlrUtils;
import java.util.Map;
import org.junit.jupiter.api.Test;

class TsStrToEpochFunctionTest extends FeelFunctionTestBase {

  @Test
  public void testTsStrToEpoch() {
    FleakData testData = new StringPrimitiveFleakData("test");

    try {
      FleakData result =
          evaluateExpression("ts_str_to_epoch(\"2023-12-25\", \"yyyy-MM-dd\")", testData);
      assertNotNull(result);
      assertInstanceOf(NumberPrimitiveFleakData.class, result);
    } catch (Exception e) {
      fail("ts_str_to_epoch test failed: " + e.getMessage());
    }
  }

  @Test
  public void testTsStrToEpochWithTimezonePattern() {
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
        assertTrue(executionError.getMessage().contains("ts_str_to_epoch"));
        assertTrue(executionError.getMessage().contains("2 arguments"));
      }

    } catch (Exception parseError) {
      fail("Parsing should succeed with new generic grammar: " + parseError.getMessage());
    }
  }
}
