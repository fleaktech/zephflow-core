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
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class InFunctionTest extends FeelFunctionTestBase {

  @Test
  public void testInFunction() {
    FleakData testData =
        FleakData.wrap(
            Map.of(
                "nums", List.of(1, 2, 3),
                "items", List.of(Map.of("name", "Alice"), Map.of("name", "Bob"))));

    // primitive found
    testFunctionExecution(testData, "in(2, $.nums)", true);
    // primitive not found
    testFunctionExecution(testData, "in(5, $.nums)", false);
    // empty array
    testFunctionExecution(testData, "in(1, array())", false);
    // null value
    testFunctionExecution(testData, "in(null, $.nums)", false);
    // null array
    testFunctionExecution(testData, "in(1, $.nonexistent)", false);
    // object in array
    testFunctionExecution(testData, "in(dict(name=\"Alice\"), $.items)", true);
    testFunctionExecution(testData, "in(dict(name=\"Charlie\"), $.items)", false);
    // non-array second arg
    assertThrows(
        IllegalArgumentException.class,
        () -> evaluateExpression("in(1, \"not_an_array\")", testData));
  }
}
