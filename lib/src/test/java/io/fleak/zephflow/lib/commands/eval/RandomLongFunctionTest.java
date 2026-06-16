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
import org.junit.jupiter.api.Test;

class RandomLongFunctionTest extends FeelFunctionTestBase {

  @Test
  public void testRandomLongFunction() {
    FleakData testData = new RecordFleakData();

    FleakData result1 = evaluateExpression("random_long()", testData);
    assertNotNull(result1);
    assertInstanceOf(NumberPrimitiveFleakData.class, result1);

    FleakData result2 = evaluateExpression("random_long()", testData);
    assertNotNull(result2);
    assertInstanceOf(NumberPrimitiveFleakData.class, result2);

    long randomValue1 = (long) result1.getNumberValue();
    long randomValue2 = (long) result2.getNumberValue();

    assertNotEquals(randomValue1, randomValue2);
  }

  @Test
  public void testRandomLongFunctionNoArguments() {
    FleakData testData = new RecordFleakData();

    assertThrows(
        IllegalArgumentException.class, () -> evaluateExpression("random_long(123)", testData));
  }
}
