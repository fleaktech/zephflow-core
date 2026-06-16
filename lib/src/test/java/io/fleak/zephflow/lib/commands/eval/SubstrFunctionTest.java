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
import java.util.Map;
import org.junit.jupiter.api.Test;

class SubstrFunctionTest extends FeelFunctionTestBase {

  @Test
  public void testSubstr() {
    FleakData testData = FleakData.wrap(Map.of("text", "Hello World"));

    testFunctionExecution(testData, "substr(\"hello\", 1, 3)", "ell");
    testFunctionExecution(testData, "substr(\"hello\", 1)", "ello");
    testFunctionExecution(testData, "substr(\"hello\", -2)", "lo");
  }

  @Test
  public void testSubstrEdgeCases() {
    FleakData testData = new StringPrimitiveFleakData("test");

    testFunctionExecution(testData, "substr(\"hello\", 0, 0)", "");
    testFunctionExecution(testData, "substr(\"hello\", 10)", "");
  }

  @Test
  public void testSubstrNullHandling() {
    FleakData testData = FleakData.wrap(Map.of("text", "Hello World"));

    testFunctionExecution(testData, "substr(null, 1, 3)", null);
    testFunctionExecution(testData, "substr($.nonexistent, 1)", null);
  }
}
