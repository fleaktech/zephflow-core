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

class ArrFilterFunctionTest extends FeelFunctionTestBase {

  @Test
  public void testArrFilterFunction() {
    FleakData testData =
        FleakData.wrap(
            Map.of(
                "items",
                    List.of(
                        Map.of("price", 100, "category", "A"),
                        Map.of("price", 200, "category", "B"),
                        Map.of("price", 150, "category", "A"),
                        Map.of("price", 300, "category", "C")),
                "numbers", List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));

    FleakData result1 =
        evaluateExpression("arr_filter($.items, item, item.category == \"A\")", testData);
    assertEquals(
        FleakData.wrap(
            List.of(Map.of("price", 100, "category", "A"), Map.of("price", 150, "category", "A"))),
        result1);

    FleakData result2 =
        evaluateExpression("arr_filter($.items, item, item.price >= 150)", testData);
    assertEquals(
        FleakData.wrap(
            List.of(
                Map.of("price", 200, "category", "B"),
                Map.of("price", 150, "category", "A"),
                Map.of("price", 300, "category", "C"))),
        result2);

    FleakData result3 = evaluateExpression("arr_filter($.numbers, n, n % 2 == 0)", testData);
    assertEquals(FleakData.wrap(List.of(2, 4, 6, 8, 10)), result3);
  }

  @Test
  public void testArrFilterNoMatch() {
    FleakData testData =
        FleakData.wrap(
            Map.of(
                "items",
                List.of(
                    Map.of("price", 100, "category", "A"), Map.of("price", 200, "category", "B"))));

    FleakData result =
        evaluateExpression("arr_filter($.items, item, item.category == \"C\")", testData);
    assertEquals(FleakData.wrap(List.of()), result);

    FleakData result2 =
        evaluateExpression("arr_filter($.bad_array, item, item.category == \"C\")", testData);
    assertEquals(FleakData.wrap(List.of()), result2);
  }

  @Test
  public void testArrFilterEmptyArray() {
    FleakData testData = FleakData.wrap(Map.of("items", List.of()));

    FleakData result = evaluateExpression("arr_filter($.items, item, item.price > 0)", testData);
    assertEquals(FleakData.wrap(List.of()), result);
  }

  @Test
  public void testArrFilterWithIndexAccess() {
    FleakData testData =
        FleakData.wrap(
            Map.of(
                "items",
                List.of(
                    Map.of("price", 100, "category", "A"), Map.of("price", 150, "category", "A"))));

    String expr = "arr_filter($.items, item, item.category == \"A\")[0].price";
    FleakData result = evaluateExpression(expr, testData);
    assertNotNull(result);
    assertEquals(100L, result.unwrap());
  }

  @Test
  public void testArrFilterArgumentValidation() {
    FleakData testData = FleakData.wrap(Map.of("items", List.of(1, 2, 3)));

    assertThrows(
        IllegalArgumentException.class, () -> evaluateExpression("arr_filter($.items)", testData));

    FleakData filterResult = evaluateExpression("arr_filter(\"not_array\", x, x > 0)", testData);
    assertEquals(List.of(), filterResult.unwrap());
  }
}
