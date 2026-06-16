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
import java.util.Map;
import org.junit.jupiter.api.Test;

class UpperFunctionTest extends FeelFunctionTestBase {

  @Test
  public void testUpper() {
    FleakData testData = FleakData.wrap(Map.of("text", "Hello World"));

    testFunctionExecution(testData, "upper(\"hello\")", "HELLO");
    testFunctionExecution(testData, "upper(\"\")", "");
  }

  @Test
  public void testUpperExecution() {
    FleakData testData = new StringPrimitiveFleakData("test");
    FleakData result = evaluateExpression("upper(\"hello\")", testData);
    assertNotNull(result);
    assertEquals("HELLO", result.getStringValue());
  }

  @Test
  public void testUpperNullHandling() {
    FleakData testData = FleakData.wrap(Map.of("text", "Hello World"));

    testFunctionExecution(testData, "upper(null)", null);
    testFunctionExecution(testData, "upper($.nonexistent)", null);
  }

  @Test
  public void testUpperArgumentValidation() {
    FleakData testData = new StringPrimitiveFleakData("test");

    assertThrows(IllegalArgumentException.class, () -> evaluateExpression("upper()", testData));
    assertThrows(Exception.class, () -> evaluateExpression("upper(123)", testData));
  }
}
