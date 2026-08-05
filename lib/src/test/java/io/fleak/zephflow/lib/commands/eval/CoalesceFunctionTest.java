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
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.utils.JsonUtils;
import org.junit.jupiter.api.Test;

class CoalesceFunctionTest extends FeelFunctionTestBase {

  private static final FleakData DUMMY = new StringPrimitiveFleakData("x");

  private static FleakData ev(String json) {
    return JsonUtils.loadFleakDataFromJsonString(json);
  }

  @Test
  public void returnsFirstNonNull() {
    testFunctionExecution(ev("{\"a\":\"first\",\"b\":\"second\"}"), "coalesce($.a, $.b)", "first");
  }

  @Test
  public void skipsMissingToLaterArg() {
    testFunctionExecution(ev("{\"b\":\"second\"}"), "coalesce($.a, $.b)", "second");
  }

  @Test
  public void allNullYieldsNull() {
    testFunctionExecution(ev("{\"g\":1}"), "coalesce($.a, $.b)", null);
  }

  @Test
  public void explicitNullLiteralIsSkipped() {
    testFunctionExecution(DUMMY, "coalesce(null, \"fallback\")", "fallback");
  }

  @Test
  public void preservesNonStringType() {
    // the chosen argument keeps its type; it is not stringified
    testFunctionExecution(ev("{\"flag\":true}"), "coalesce($.missing, $.flag)", true);
    testFunctionExecution(ev("{\"n\":42}"), "coalesce($.missing, $.n)", 42L);
  }

  @Test
  public void nonNullButFalsyValuesAreReturnedNotSkipped() {
    // "first non-null", not "first truthy": empty string / 0 / false are values and must be
    // returned
    testFunctionExecution(ev("{\"a\":\"\",\"b\":\"fallback\"}"), "coalesce($.a, $.b)", "");
    testFunctionExecution(ev("{\"a\":0,\"b\":\"fallback\"}"), "coalesce($.a, $.b)", 0L);
    testFunctionExecution(ev("{\"a\":false,\"b\":\"fallback\"}"), "coalesce($.a, $.b)", false);
  }

  @Test
  public void explicitJsonNullValueIsSkipped() {
    // a present-but-null field is skipped just like an absent one
    testFunctionExecution(ev("{\"a\":null,\"b\":\"present\"}"), "coalesce($.a, $.b)", "present");
  }

  @Test
  public void skipsMultipleNullsToFirstPresent() {
    testFunctionExecution(ev("{\"c\":\"third\"}"), "coalesce($.a, $.b, $.c)", "third");
  }

  @Test
  public void singleArgumentReturnsItself() {
    testFunctionExecution(ev("{\"a\":\"only\"}"), "coalesce($.a)", "only");
  }
}
