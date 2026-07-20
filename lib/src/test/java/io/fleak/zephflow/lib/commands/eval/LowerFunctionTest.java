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

class LowerFunctionTest extends FeelFunctionTestBase {

  @Test
  public void testLower() {
    FleakData testData = FleakData.wrap(Map.of("text", "Hello World"));

    testFunctionExecution(testData, "lower(\"WORLD\")", "world");
  }

  @Test
  public void testLowerNullHandling() {
    FleakData testData = FleakData.wrap(Map.of("text", "Hello World"));

    testFunctionExecution(testData, "lower(null)", null);
    testFunctionExecution(testData, "lower($.nonexistent)", null);
  }
}
