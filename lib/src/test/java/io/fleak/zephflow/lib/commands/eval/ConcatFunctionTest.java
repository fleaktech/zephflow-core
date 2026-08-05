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

class ConcatFunctionTest extends FeelFunctionTestBase {

  private static final FleakData DUMMY = new StringPrimitiveFleakData("x");

  private static FleakData ev(String json) {
    return JsonUtils.loadFleakDataFromJsonString(json);
  }

  @Test
  public void joinsStringParts() {
    // rebuild a full path from a directory and a name, separated by a backslash
    testFunctionExecution(
        ev("{\"dir\":\"C:\\\\Win\",\"name\":\"cmd.exe\"}"),
        "concat($.dir, \"\\\\\", $.name)",
        "C:\\Win\\cmd.exe");
  }

  @Test
  public void nullArgumentIsTreatedAsEmpty() {
    // dir is missing -> treated as "" instead of throwing like the + operator would
    testFunctionExecution(
        ev("{\"name\":\"cmd.exe\"}"), "concat($.dir, \"\\\\\", $.name)", "\\cmd.exe");
  }

  @Test
  public void coercesNonStringArgument() {
    testFunctionExecution(DUMMY, "concat(\"port-\", 4444)", "port-4444");
  }

  @Test
  public void integerFieldConcatenatesWithoutDecimalPoint() {
    // a whole-number field must join as "4444", not "4444.0"
    testFunctionExecution(ev("{\"n\":4444}"), "concat(\"port-\", $.n)", "port-4444");
  }

  @Test
  public void allNullYieldsNull() {
    testFunctionExecution(ev("{\"g\":1}"), "concat($.a, $.b)", null);
  }

  @Test
  public void presentEmptyStringsYieldEmptyNotNull() {
    // present-but-empty is different from all-null: result is "" (non-null)
    testFunctionExecution(ev("{\"a\":\"\",\"b\":\"\"}"), "concat($.a, $.b)", "");
  }

  @Test
  public void explicitJsonNullValueIsTreatedAsEmpty() {
    testFunctionExecution(ev("{\"a\":null,\"b\":\"x\"}"), "concat($.a, $.b)", "x");
  }

  @Test
  public void singleArgumentIsStringified() {
    testFunctionExecution(ev("{\"a\":\"x\"}"), "concat($.a)", "x");
  }
}
