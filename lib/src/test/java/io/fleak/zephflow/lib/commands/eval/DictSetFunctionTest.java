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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DictSetFunctionTest extends FeelFunctionTestBase {

  @Test
  public void testDictSet() {
    FleakData testData =
        FleakData.wrap(
            Map.of(
                "d", Map.of("a", 1, "b", 2),
                "keyName", "dynamic",
                "nums", Map.of("x", 3, "y", 4)));

    // add a new field
    testFunctionExecution(testData, "dict_set($.d, \"c\", 3)", Map.of("a", 1L, "b", 2L, "c", 3L));
    // multiple pairs in one call
    testFunctionExecution(
        testData,
        "dict_set($.d, \"c\", 3, \"e\", \"hi\")",
        Map.of("a", 1L, "b", 2L, "c", 3L, "e", "hi"));
    // overwrite existing key
    testFunctionExecution(testData, "dict_set($.d, \"a\", 9)", Map.of("a", 9L, "b", 2L));
    // duplicate key in same call: last wins
    testFunctionExecution(
        testData, "dict_set($.d, \"c\", 1, \"c\", 2)", Map.of("a", 1L, "b", 2L, "c", 2L));
    // dynamic key from the event
    testFunctionExecution(
        testData, "dict_set($.d, $.keyName, 1)", Map.of("a", 1L, "b", 2L, "dynamic", 1L));
    // computed value expression
    testFunctionExecution(
        testData,
        "dict_set($.d, \"sum\", $.nums.x + $.nums.y)",
        Map.of("a", 1L, "b", 2L, "sum", 7L));
    // dict/array values
    testFunctionExecution(
        testData,
        "dict_set($.d, \"tags\", array(1, 2), \"meta\", dict(k=\"v\"))",
        Map.of("a", 1L, "b", 2L, "tags", List.of(1L, 2L), "meta", Map.of("k", "v")));

    // overwrite changes the value type
    testFunctionExecution(
        testData, "dict_set($.d, \"a\", dict(inner=1))", Map.of("a", Map.of("inner", 1L), "b", 2L));
    // empty string is a valid key
    testFunctionExecution(testData, "dict_set($.d, \"\", 1)", Map.of("a", 1L, "b", 2L, "", 1L));
    // build on an empty dict literal
    testFunctionExecution(testData, "dict_set(dict(), \"a\", 1)", Map.of("a", 1L));
    // chained calls compose
    testFunctionExecution(
        testData,
        "dict_set(dict_set($.d, \"c\", 3), \"e\", 4)",
        Map.of("a", 1L, "b", 2L, "c", 3L, "e", 4L));

    // input is not mutated
    testFunctionExecution(testData, "$.d", Map.of("a", 1L, "b", 2L));

    // errors: even arg count, non-string key (literal and dynamic), non-dict first arg
    assertThrows(
        IllegalArgumentException.class, () -> evaluateExpression("dict_set($.d, \"k\")", testData));
    assertThrows(
        IllegalArgumentException.class,
        () -> evaluateExpression("dict_set($.d, \"k\", 1, \"x\")", testData));
    assertThrows(
        IllegalArgumentException.class,
        () -> evaluateExpression("dict_set($.d, 1, \"v\")", testData));
    assertThrows(
        IllegalArgumentException.class,
        () -> evaluateExpression("dict_set($.d, $.nums.x, \"v\")", testData));
    assertThrows(
        IllegalArgumentException.class,
        () -> evaluateExpression("dict_set(\"str\", \"k\", \"v\")", testData));
  }

  @Test
  public void testDictSetNullHandling() {
    FleakData testData = FleakData.wrap(Map.of("d", Map.of("a", 1)));

    testFunctionExecution(testData, "dict_set(null, \"k\", 1)", null);
    testFunctionExecution(testData, "dict_set($.nonexistent, \"k\", 1)", null);

    // null key skips that pair; other pairs still applied
    testFunctionExecution(testData, "dict_set($.d, null, 1, \"b\", 2)", Map.of("a", 1L, "b", 2L));

    // null value sets the field to null
    Map<String, Object> expectedWithNull = new HashMap<>();
    expectedWithNull.put("a", 1L);
    expectedWithNull.put("b", null);
    testFunctionExecution(testData, "dict_set($.d, \"b\", null)", expectedWithNull);
  }
}
