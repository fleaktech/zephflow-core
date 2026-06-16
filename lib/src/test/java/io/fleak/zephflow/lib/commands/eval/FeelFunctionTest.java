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
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Cross-cutting tests for FEEL functions. Per-function behavior lives in the dedicated {@code
 * *FunctionTest} classes next to this one.
 */
class FeelFunctionTest extends FeelFunctionTestBase {

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
  public void testDictLiteral() {
    FleakData testData = new RecordFleakData();

    testFunctionExecution(testData, "dict()", Map.of());
  }

  @Test
  public void testSingstatBopTransformation() {
    String inputJson =
        """
        {
          "Data": {
            "row": [
              {
                "rowText": "Financial Account (Net)",
                "columns": [
                  {"key": "2024 Q3", "value": "24500.2"},
                  {"key": "2024 Q2", "value": "18900.5"}
                ]
              },
              {
                "rowText": "   Direct Investment",
                "columns": [{"key": "2024 Q3", "value": "15200.1"}]
              },
              {
                "rowText": "      Direct Investment Assets",
                "columns": [{"key": "2024 Q3", "value": "8500.0"}]
              },
              {
                "rowText": "      Direct Investment Liabilities",
                "columns": [{"key": "2024 Q3", "value": "6700.1"}]
              }
            ]
          }
        }
        """;

    String expr =
        """
dict(
  header = dict(
    id = "SG_BOP_SUBMISSION",
    test = false,
    prepared = "2026-02-03T12:00:00Z",
    sender = dict(id = "SINGSTAT"),
    structure = dict(structureID = "IMF_BOP_BPM6")
  ),
  dataSets = array(
    dict(
      action = "Replace",
      structureRef = "IMF_BOP_BPM6",
      observations = arr_to_dict(
        arr_flatten(
          arr_foreach($.Data.row, row,
            arr_foreach(row.columns, elem,
              dict(
                k = "Q:SG:FA:" +
                  case(
                    str_contains(row.rowText, "Direct Investment") => "DI",
                    str_contains(row.rowText, "Portfolio Investment") => "PI",
                    str_contains(row.rowText, "Financial Derivatives") => "FD",
                    str_contains(row.rowText, "Other Investment") => "OI",
                    str_contains(row.rowText, "Reserve Assets") => "RA",
                    _ => "_T"
                  ) + ":" +
                  case(
                    str_contains(row.rowText, "Assets") => "A",
                    str_contains(row.rowText, "Liabilities") => "L",
                    _ => "NET"
                  ) + ":" + str_replace(elem.key, " ", "-"),
                v = array(parse_float(elem.value), 0, "A")
              )
            )
          )
        ),
        entry,
        entry.k,
        entry.v
      )
    )
  )
)
""";

    String expectedJson =
        """
        {
          "header": {
            "id": "SG_BOP_SUBMISSION",
            "test": false,
            "prepared": "2026-02-03T12:00:00Z",
            "sender": {"id": "SINGSTAT"},
            "structure": {"structureID": "IMF_BOP_BPM6"}
          },
          "dataSets": [
            {
              "action": "Replace",
              "structureRef": "IMF_BOP_BPM6",
              "observations": {
                "Q:SG:FA:_T:NET:2024-Q3": [24500.2, 0, "A"],
                "Q:SG:FA:_T:NET:2024-Q2": [18900.5, 0, "A"],
                "Q:SG:FA:DI:NET:2024-Q3": [15200.1, 0, "A"],
                "Q:SG:FA:DI:A:2024-Q3": [8500.0, 0, "A"],
                "Q:SG:FA:DI:L:2024-Q3": [6700.1, 0, "A"]
              }
            }
          ]
        }
        """;

    FleakData inputData = JsonUtils.loadFleakDataFromJsonString(inputJson);
    FleakData result = evaluateExpression(expr, inputData);
    assertNotNull(result);

    FleakData expected = JsonUtils.loadFleakDataFromJsonString(expectedJson);
    assertEquals(expected, result);
  }
}
