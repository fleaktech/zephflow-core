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

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.fleak.zephflow.api.structure.*;
import io.fleak.zephflow.lib.utils.JsonUtils;
import org.junit.jupiter.api.Test;

class NumCompareAnyFunctionTest extends FeelFunctionTestBase {

  private static final FleakData DUMMY = new StringPrimitiveFleakData("x");

  @Test
  public void allFourOperators() {
    testFunctionExecution(DUMMY, "num_compare_any(4444, \"gt\", 1024)", true);
    testFunctionExecution(DUMMY, "num_compare_any(80, \"gt\", 1024)", false);
    testFunctionExecution(DUMMY, "num_compare_any(1024, \"gte\", 1024)", true);
    testFunctionExecution(DUMMY, "num_compare_any(1023, \"gte\", 1024)", false);
    testFunctionExecution(DUMMY, "num_compare_any(80, \"lt\", 1024)", true);
    testFunctionExecution(DUMMY, "num_compare_any(1024, \"lt\", 1024)", false);
    testFunctionExecution(DUMMY, "num_compare_any(1024, \"lte\", 1024)", true);
    testFunctionExecution(DUMMY, "num_compare_any(1025, \"lte\", 1024)", false);
  }

  @Test
  public void handlesDoublesAndNegatives() {
    testFunctionExecution(DUMMY, "num_compare_any(3.5, \"lt\", 4)", true);
    testFunctionExecution(DUMMY, "num_compare_any(\"-5\", \"lt\", 0)", true);
    testFunctionExecution(DUMMY, "num_compare_any(-5, \"gt\", 0)", false);
  }

  @Test
  public void nonNumericValuesDoNotSatisfy() {
    // missing field, a record, and a non-numeric string all yield false rather than throwing
    testFunctionExecution(ev("{\"g\":1}"), "num_compare_any($.port, \"gt\", 10)", false);
    testFunctionExecution(ev("{\"port\":{\"n\":1}}"), "num_compare_any($.port, \"gt\", 10)", false);
    testFunctionExecution(DUMMY, "num_compare_any(\"abc\", \"gt\", 10)", false);
  }

  @Test
  public void unknownOperatorThrows() {
    assertThrows(
        Exception.class, () -> evaluateExpression("num_compare_any(5, \"between\", 10)", DUMMY));
  }

  private static FleakData ev(String json) {
    return JsonUtils.loadFleakDataFromJsonString(json);
  }

  @Test
  public void stringValueIsCoerced() {
    testFunctionExecution(DUMMY, "num_compare_any(\"5000\", \"gt\", 1024)", true);
    testFunctionExecution(DUMMY, "num_compare_any(\"50\", \"gt\", 1024)", false);
    testFunctionExecution(DUMMY, "num_compare_any(\"not-a-number\", \"lt\", 10)", false);
  }

  @Test
  public void arrayMatchesAnyElement() {
    FleakData ev = JsonUtils.loadFleakDataFromJsonString("{\"ports\":[80,443,4444]}");
    testFunctionExecution(ev, "num_compare_any($.ports, \"gt\", 1024)", true);
    FleakData low = JsonUtils.loadFleakDataFromJsonString("{\"ports\":[80,443]}");
    testFunctionExecution(low, "num_compare_any($.ports, \"gt\", 1024)", false);
  }
}
