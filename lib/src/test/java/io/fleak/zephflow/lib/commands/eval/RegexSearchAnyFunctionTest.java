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

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.lib.utils.JsonUtils;
import org.junit.jupiter.api.Test;

class RegexSearchAnyFunctionTest extends FeelFunctionTestBase {

  private static FleakData ev(String json) {
    return JsonUtils.loadFleakDataFromJsonString(json);
  }

  @Test
  public void findsUnanchoredUnlikeFullMatch() {
    // regex_match_any is a full match and would be false here; regex_search_any (find) is true
    testFunctionExecution(
        ev("{\"f\":\"prefix net view suffix\"}"), "regex_match_any($.f, \"net view\")", false);
    testFunctionExecution(
        ev("{\"f\":\"prefix net view suffix\"}"), "regex_search_any($.f, \"net view\")", true);
  }

  @Test
  public void searchesAnyArrayElement() {
    testFunctionExecution(
        ev("{\"f\":[\"x\",\"has net view here\"]}"), "regex_search_any($.f, \"net view\")", true);
    testFunctionExecution(
        ev("{\"f\":[\"x\",\"y\"]}"), "regex_search_any($.f, \"net view\")", false);
  }

  @Test
  public void missingFieldIsFalse() {
    testFunctionExecution(ev("{\"g\":1}"), "regex_search_any($.f, \".*\")", false);
  }

  @Test
  public void coercesNonStringScalar() {
    testFunctionExecution(ev("{\"f\":4444}"), "regex_search_any($.f, \"44\")", true);
  }

  @Test
  public void recordValueIsNeverMatched() {
    testFunctionExecution(ev("{\"f\":{\"n\":1}}"), "regex_search_any($.f, \".*\")", false);
  }
}
