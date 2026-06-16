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

class ArrFindFunctionTest extends FeelFunctionTestBase {

  @Test
  public void testArrFindFunction() {
    FleakData testData =
        FleakData.wrap(
            Map.of(
                "users",
                    List.of(
                        Map.of("name", "Alice", "id", "100"),
                        Map.of("name", "Bob", "id", "200"),
                        Map.of("name", "Charlie", "id", "300")),
                "products",
                    List.of(
                        Map.of("price", 100, "category", "A"),
                        Map.of("price", 200, "category", "B"),
                        Map.of("price", 150, "category", "A"))));

    FleakData result1 = evaluateExpression("arr_find($.users, user, user.id == \"100\")", testData);
    assertEquals(FleakData.wrap(Map.of("name", "Alice", "id", "100")), result1);

    FleakData result2 =
        evaluateExpression("arr_find($.products, item, item.price > 100)", testData);
    assertEquals(FleakData.wrap(Map.of("category", "B", "price", 200)), result2);

    FleakData result3 =
        evaluateExpression("arr_find($.products, item, item.category == \"A\")", testData);
    assertEquals(FleakData.wrap(Map.of("price", 100, "category", "A")), result3);
  }

  @Test
  public void testArrFindNoMatch() {
    FleakData testData =
        FleakData.wrap(
            Map.of(
                "items",
                List.of(
                    Map.of("price", 100, "category", "A"), Map.of("price", 200, "category", "B"))));

    FleakData result =
        evaluateExpression("arr_find($.items, item, item.category == \"C\")", testData);
    assertNull(result);
  }

  @Test
  public void testArrFindEmptyArray() {
    FleakData testData = FleakData.wrap(Map.of("items", List.of()));

    FleakData result = evaluateExpression("arr_find($.items, item, item.price > 0)", testData);
    assertNull(result);
  }

  @Test
  public void testArrFindWithNullSafety() {
    FleakData testData =
        FleakData.wrap(
            Map.of(
                "users",
                List.of(Map.of("name", "Alice", "id", "100"), Map.of("name", "Bob", "id", "200"))));

    String expr = "arr_find($.users, user, user.id == '100').name";

    FleakData result = evaluateExpression(expr, testData);
    assertNotNull(result);
    assertEquals("Alice", result.getStringValue());

    String expr2 = "arr_find($.users, user, user.id == \"999\").name";

    FleakData result2 = evaluateExpression(expr2, testData);
    assertNull(result2);

    String expr3 = "arr_find($.bad_field, user, user.id == '100').name";
    FleakData result3 = evaluateExpression(expr3, testData);
    assertNull(result3);
  }

  @Test
  public void testArrFindArgumentValidation() {
    FleakData testData = FleakData.wrap(Map.of("items", List.of(1, 2, 3)));

    assertThrows(
        IllegalArgumentException.class,
        () -> evaluateExpression("arr_find($.items, item)", testData));

    FleakData findResult = evaluateExpression("arr_find(\"not_array\", x, x > 0)", testData);
    assertNull(findResult);
  }
}
