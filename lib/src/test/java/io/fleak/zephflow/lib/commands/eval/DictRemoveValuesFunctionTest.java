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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DictRemoveValuesFunctionTest extends FeelFunctionTestBase {

  @Test
  public void testDictRemoveValues() {
    FleakData testData =
        FleakData.wrap(
            Map.ofEntries(
                Map.entry("flat", Map.of("a", "0x0", "b", 1)),
                Map.entry("win", Map.of("k1", "0x0", "k2", "0", "k3", "-", "k4", "ok")),
                Map.entry("nested", Map.of("a", Map.of("x", "-", "y", 2), "b", 3)),
                Map.entry("arr", Map.of("a", List.of(1, "-", 2))),
                Map.entry("dictInArr", Map.of("a", List.of(Map.of("x", "-", "y", 1)))),
                Map.entry("arrInArr", Map.of("a", List.of(List.of(1, "-", 2)))),
                Map.entry("containers", Map.of("a", List.of(), "b", List.of(1), "c", Map.of())),
                Map.entry("typed", Map.of("a", 0, "b", "0")),
                Map.entry("dbl", Map.of("a", 0.0, "b", 1.5)),
                Map.entry("bool", Map.of("a", false, "b", true, "c", "false")),
                Map.entry("emptied", Map.of("a", Map.of("x", "-"), "b", 1))));

    testFunctionExecution(testData, "dict_remove_values($.flat, \"0x0\")", Map.of("b", 1L));
    testFunctionExecution(
        testData, "dict_remove_values($.win, \"0x0\", \"0\", \"-\")", Map.of("k4", "ok"));
    testFunctionExecution(
        testData, "dict_remove_values($.nested, \"-\")", Map.of("a", Map.of("y", 2L), "b", 3L));
    testFunctionExecution(
        testData, "dict_remove_values($.arr, \"-\")", Map.of("a", List.of(1L, 2L)));
    testFunctionExecution(
        testData, "dict_remove_values($.dictInArr, \"-\")", Map.of("a", List.of(Map.of("y", 1L))));
    testFunctionExecution(
        testData, "dict_remove_values($.arrInArr, \"-\")", Map.of("a", List.of(List.of(1L, 2L))));

    // value argument resolved dynamically from the event
    testFunctionExecution(testData, "dict_remove_values($.flat, $.flat.a)", Map.of("b", 1L));

    // container-valued args remove fields structurally equal to that container
    testFunctionExecution(
        testData,
        "dict_remove_values($.containers, array())",
        Map.of("b", List.of(1L), "c", Map.of()));
    testFunctionExecution(
        testData,
        "dict_remove_values($.containers, array(1))",
        Map.of("a", List.of(), "c", Map.of()));

    // type-sensitive equality: string "0" vs number 0
    testFunctionExecution(testData, "dict_remove_values($.typed, \"0\")", Map.of("a", 0L));
    testFunctionExecution(testData, "dict_remove_values($.typed, 0)", Map.of("b", "0"));
    // numbers compare by value across long/double
    testFunctionExecution(testData, "dict_remove_values($.dbl, 0)", Map.of("b", 1.5));
    // boolean values; string "false" does not match boolean false
    testFunctionExecution(
        testData, "dict_remove_values($.bool, false)", Map.of("b", true, "c", "false"));

    // no match -> unchanged; emptied containers are kept
    testFunctionExecution(
        testData, "dict_remove_values($.flat, \"no_match\")", Map.of("a", "0x0", "b", 1L));
    testFunctionExecution(
        testData, "dict_remove_values($.emptied, \"-\")", Map.of("a", Map.of(), "b", 1L));

    // input is not mutated
    testFunctionExecution(testData, "$.flat", Map.of("a", "0x0", "b", 1L));

    assertThrows(
        IllegalArgumentException.class,
        () -> evaluateExpression("dict_remove_values(\"str\", \"x\")", testData));
    assertThrows(
        IllegalArgumentException.class,
        () -> evaluateExpression("dict_remove_values($.flat)", testData));
  }

  @Test
  public void testDictRemoveValuesNullHandling() {
    Map<String, Object> withNull = new HashMap<>();
    withNull.put("a", null);
    withNull.put("b", 1);

    Map<String, Object> innerNull = new HashMap<>();
    innerNull.put("x", null);
    innerNull.put("y", 2);
    List<Object> arrWithNull = new ArrayList<>();
    arrWithNull.add(1);
    arrWithNull.add(null);
    arrWithNull.add(2);
    FleakData testData =
        FleakData.wrap(
            Map.of(
                "withNull",
                withNull,
                "nestedNull",
                Map.of("inner", innerNull, "arr", arrWithNull)));

    testFunctionExecution(testData, "dict_remove_values(null, \"x\")", null);
    testFunctionExecution(testData, "dict_remove_values($.nonexistent, \"x\")", null);
    testFunctionExecution(testData, "dict_remove_values($.withNull, null)", Map.of("b", 1L));
    // nulls nested inside dicts and arrays are removed recursively
    testFunctionExecution(
        testData,
        "dict_remove_values($.nestedNull, null)",
        Map.of("inner", Map.of("y", 2L), "arr", List.of(1L, 2L)));
  }
}
