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

import io.fleak.zephflow.api.structure.*;
import io.fleak.zephflow.lib.utils.JsonUtils;
import org.junit.jupiter.api.Test;

class DateComponentEqAnyFunctionTest extends FeelFunctionTestBase {

  private static final FleakData DUMMY = new StringPrimitiveFleakData("x");
  private static final String TS = "\"2023-03-15T14:30:00Z\""; // Wed 2023-03-15 14:30 UTC, week 11

  private static FleakData ev(String json) {
    return JsonUtils.loadFleakDataFromJsonString(json);
  }

  @Test
  public void extractsEveryUnitFromIsoString() {
    testFunctionExecution(DUMMY, "date_component_eq_any(" + TS + ", \"minute\", 30)", true);
    testFunctionExecution(DUMMY, "date_component_eq_any(" + TS + ", \"hour\", 14)", true);
    testFunctionExecution(DUMMY, "date_component_eq_any(" + TS + ", \"day\", 15)", true);
    testFunctionExecution(DUMMY, "date_component_eq_any(" + TS + ", \"week\", 11)", true);
    testFunctionExecution(DUMMY, "date_component_eq_any(" + TS + ", \"month\", 3)", true);
    testFunctionExecution(DUMMY, "date_component_eq_any(" + TS + ", \"year\", 2023)", true);
  }

  @Test
  public void nonMatchingComponentIsFalse() {
    testFunctionExecution(DUMMY, "date_component_eq_any(" + TS + ", \"hour\", 9)", false);
    testFunctionExecution(DUMMY, "date_component_eq_any(" + TS + ", \"month\", 12)", false);
  }

  @Test
  public void unitIsCaseInsensitive() {
    testFunctionExecution(DUMMY, "date_component_eq_any(" + TS + ", \"HOUR\", 14)", true);
  }

  @Test
  public void extractsFromEpochMillis() {
    // 1678890600000 == 2023-03-15T14:30:00Z
    testFunctionExecution(DUMMY, "date_component_eq_any(1678890600000, \"hour\", 14)", true);
    testFunctionExecution(DUMMY, "date_component_eq_any(1678890600000, \"minute\", 30)", true);
  }

  @Test
  public void unknownUnitOrUnparseableValueOrMissingIsFalse() {
    testFunctionExecution(DUMMY, "date_component_eq_any(" + TS + ", \"fortnight\", 1)", false);
    testFunctionExecution(DUMMY, "date_component_eq_any(\"not-a-date\", \"hour\", 14)", false);
    testFunctionExecution(ev("{\"g\":1}"), "date_component_eq_any($.ts, \"hour\", 14)", false);
  }

  @Test
  public void arrayMatchesAnyElement() {
    FleakData hit = ev("{\"ts\":[\"2023-03-15T09:00:00Z\",\"2023-03-15T14:00:00Z\"]}");
    testFunctionExecution(hit, "date_component_eq_any($.ts, \"hour\", 14)", true);
    FleakData miss = ev("{\"ts\":[\"2023-03-15T09:00:00Z\"]}");
    testFunctionExecution(miss, "date_component_eq_any($.ts, \"hour\", 14)", false);
  }
}
