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

import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class RegexMatchAnyFunctionTest extends FeelFunctionTestBase {

  private static FleakData ev(String json) {
    return JsonUtils.loadFleakDataFromJsonString(json);
  }

  @Test
  public void scalarFullMatch() {
    testFunctionExecution(ev("{\"f\":\"cmd.exe\"}"), "regex_match_any($.f, \"cmd\\\\.exe\")", true);
    testFunctionExecution(
        ev("{\"f\":\"xcmd.exe\"}"), "regex_match_any($.f, \"cmd\\\\.exe\")", false);
  }

  @Test
  public void arrayMatchesAnyElement() {
    testFunctionExecution(
        ev("{\"f\":[\"a\",\"cmd.exe\"]}"), "regex_match_any($.f, \"cmd\\\\.exe\")", true);
    testFunctionExecution(
        ev("{\"f\":[\"a\",\"b\"]}"), "regex_match_any($.f, \"cmd\\\\.exe\")", false);
  }

  @Test
  public void missingFieldAndNumbers() {
    testFunctionExecution(ev("{\"g\":1}"), "regex_match_any($.f, \".*\")", false);
    testFunctionExecution(ev("{\"f\":4444}"), "regex_match_any($.f, \"4444\")", true);
  }

  @Test
  public void recordValueIsNeverMatched() {
    // a nested object is not string-matched, even by ".*"
    testFunctionExecution(ev("{\"f\":{\"n\":1}}"), "regex_match_any($.f, \".*\")", false);
  }

  @Test
  public void catastrophicRegexIsBudgetedNotHung() {
    // (a+)+$ on many a's + trailing X is exponential backtracking; the match budget must abort it
    // quickly and return false instead of hanging.
    String evil = "{\"f\":\"" + "a".repeat(40) + "X\"}";
    assertTimeoutPreemptively(
        Duration.ofSeconds(5),
        () -> testFunctionExecution(ev(evil), "regex_match_any($.f, \"(a+)+$\")", false));
  }
}
