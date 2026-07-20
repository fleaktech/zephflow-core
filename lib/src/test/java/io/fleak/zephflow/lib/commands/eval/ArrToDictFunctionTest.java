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

class ArrToDictFunctionTest extends FeelFunctionTestBase {

  @Test
  public void testArrToDictBasic() {
    FleakData testData =
        FleakData.wrap(
            Map.of(
                "columns",
                List.of(
                    Map.of("key", "cpu", "value", "0.85"), Map.of("key", "mem", "value", "0.62"))));

    FleakData result =
        evaluateExpression(
            "arr_to_dict($.columns, elem, elem.key, parse_float(elem.value))", testData);
    assertEquals(FleakData.wrap(Map.of("cpu", 0.85, "mem", 0.62)), result);
  }

  @Test
  public void testArrToDictWithArrayValue() {
    FleakData testData =
        FleakData.wrap(
            Map.of(
                "columns",
                List.of(
                    Map.of("key", "cpu", "value", "0.85"), Map.of("key", "mem", "value", "0.62"))));

    FleakData result =
        evaluateExpression(
            "arr_to_dict($.columns, elem, elem.key, array(parse_float(elem.value)))", testData);
    assertEquals(FleakData.wrap(Map.of("cpu", List.of(0.85), "mem", List.of(0.62))), result);
  }

  @Test
  public void testArrToDictDuplicateKeys() {
    FleakData testData =
        FleakData.wrap(
            Map.of("items", List.of(Map.of("k", "a", "v", 1), Map.of("k", "a", "v", 2))));

    FleakData result = evaluateExpression("arr_to_dict($.items, e, e.k, e.v)", testData);
    assertEquals(FleakData.wrap(Map.of("a", 2)), result);
  }

  @Test
  public void testArrToDictNullInput() {
    FleakData testData = FleakData.wrap(Map.of());

    FleakData result = evaluateExpression("arr_to_dict($.missing, e, e.k, e.v)", testData);
    assertNull(result);
  }

  @Test
  public void testArrToDictObjectInput() {
    FleakData testData = FleakData.wrap(Map.of("item", Map.of("k", "key1", "v", 42)));

    FleakData result = evaluateExpression("arr_to_dict($.item, e, e.k, e.v)", testData);
    assertEquals(FleakData.wrap(Map.of("key1", 42)), result);
  }

  @Test
  public void testArrToDictEmptyArray() {
    FleakData testData = FleakData.wrap(Map.of("items", List.of()));

    FleakData result = evaluateExpression("arr_to_dict($.items, e, e.k, e.v)", testData);
    assertEquals(FleakData.wrap(Map.of()), result);
  }

  @Test
  public void testArrToDictNonStringKey() {
    FleakData testData = FleakData.wrap(Map.of("items", List.of(Map.of("k", 123, "v", "val"))));

    assertThrows(
        IllegalArgumentException.class,
        () -> evaluateExpression("arr_to_dict($.items, e, e.k, e.v)", testData));
  }
}
