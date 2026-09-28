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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class FlattenFunctionTest extends FeelFunctionTestBase {

  private static final FleakData NESTED = FleakData.wrap(Map.of("a", Map.of("b", Map.of("c", 1))));

  @Test
  public void testFlattenArraysOfRecords() {
    FleakData testData =
        FleakData.wrap(
            Map.of(
                "accounting",
                List.of(Map.of("firstName", "John"), Map.of("firstName", "Mary")),
                "host",
                "h1"));

    testFunctionExecution(
        testData,
        "flatten($)",
        Map.of("accounting_0_firstName", "John", "accounting_1_firstName", "Mary", "host", "h1"));
  }

  @Test
  public void testFlattenCustomDelimiter() {
    testFunctionExecution(NESTED, "flatten($, \".\")", Map.of("a.b.c", 1L));
  }

  @Test
  public void testFlattenDepth() {
    testFunctionExecution(NESTED, "flatten($, \"_\", 1)", Map.of("a_b", Map.of("c", 1L)));
    testFunctionExecution(NESTED, "flatten($, \"_\", 2)", Map.of("a_b_c", 1L));

    FleakData deep =
        FleakData.wrap(
            Map.of(
                "l1",
                Map.of(
                    "l2", Map.of("l3", Map.of("l4", Map.of("l5", Map.of("l6", Map.of("v", 1))))))));
    testFunctionExecution(deep, "flatten($)", Map.of("l1_l2_l3_l4_l5_l6", Map.of("v", 1L)));
  }

  @Test
  public void testFlattenArrays() {
    testFunctionExecution(
        FleakData.wrap(Map.of("t", List.of("a", "b"))),
        "flatten($)",
        Map.of("t_0", "a", "t_1", "b"));
    testFunctionExecution(
        FleakData.wrap(Map.of("m", List.of(List.of(1, 2), List.of(3)))),
        "flatten($)",
        Map.of("m_0_0", 1L, "m_0_1", 2L, "m_1_0", 3L));
    testFunctionExecution(
        FleakData.wrap(Map.of("m", List.of(List.of(1, 2), List.of(3)))),
        "flatten($, \"_\", 1)",
        Map.of("m_0", List.of(1L, 2L), "m_1", List.of(3L)));
  }

  @Test
  public void testFlattenKeepsEmptiesAndNullsAsLeaves() {
    Map<String, Object> nested = new HashMap<>();
    nested.put("e", null);
    Map<String, Object> input = new HashMap<>();
    input.put("a", Map.of());
    input.put("b", List.of());
    input.put("c", null);
    input.put("d", nested);

    Map<String, Object> expected = new HashMap<>();
    expected.put("a", Map.of());
    expected.put("b", List.of());
    expected.put("c", null);
    expected.put("d_e", null);
    testFunctionExecution(FleakData.wrap(input), "flatten($)", expected);
  }

  @Test
  public void testFlattenSubpathAndPrefix() {
    FleakData testData =
        FleakData.wrap(Map.of("resource", Map.of("a", Map.of("b", 1), "c", 2), "host", "h1"));

    testFunctionExecution(testData, "flatten($.resource)", Map.of("a_b", 1L, "c", 2L));
    testFunctionExecution(
        testData, "flatten(dict(res=$.resource))", Map.of("res_a_b", 1L, "res_c", 2L));
  }

  @Test
  public void testFlattenCollisionKeepsOneValue() {
    FleakData testData = FleakData.wrap(Map.of("a_b", 1, "a", Map.of("b", 2)));

    FleakData result = evaluateExpression("flatten($)", testData);
    Map<String, FleakData> payload = result.getPayload();
    assertEquals(1, payload.size());
    assertTrue(List.of(1L, 2L).contains(payload.get("a_b").unwrap()));
  }

  @Test
  public void testFlattenComposition() {
    FleakData testData = FleakData.wrap(Map.of("id", 7, "nested", Map.of("x", Map.of("y", 1))));

    testFunctionExecution(
        testData,
        "dict_merge($, flatten($.nested))",
        Map.of("id", 7L, "nested", Map.of("x", Map.of("y", 1L)), "x_y", 1L));
    testFunctionExecution(testData, "size_of(flatten($))", 2L);
  }

  @Test
  public void testFlattenDoesNotMutateInput() {
    evaluateExpression("flatten($)", NESTED);
    testFunctionExecution(NESTED, "$", Map.of("a", Map.of("b", Map.of("c", 1L))));
  }

  @Test
  public void testFlattenNullReturnsNull() {
    testFunctionExecution(NESTED, "flatten(null)", null);
    testFunctionExecution(NESTED, "flatten($.nonexistent)", null);
  }

  @Test
  public void testFlattenNonDictionaryFails() {
    FleakData testData = FleakData.wrap(Map.of("a", "text"));
    assertErrorNames("flatten($.a)", testData, "text");
  }

  @Test
  public void testFlattenInvalidDelimiterFails() {
    assertErrorNames("flatten($, \"\")", NESTED, "delimiter");
    assertErrorNames("flatten($, 1)", NESTED, "1");
    assertErrorNames("flatten($, null)", NESTED, "null");
  }

  @ParameterizedTest
  @ValueSource(strings = {"0", "-1", "1.5", "\"2\""})
  public void testFlattenInvalidDepthFails(String depth) {
    assertErrorNames("flatten($, \"_\", " + depth + ")", NESTED, depth.replace("\"", ""));
  }

  @Test
  public void testFlattenNullDepthFails() {
    assertErrorNames("flatten($, \"_\", null)", NESTED, "null");
  }

  @Test
  public void testFlattenArity() {
    IllegalArgumentException none =
        assertThrows(IllegalArgumentException.class, () -> evaluateExpression("flatten()", NESTED));
    assertEquals("flatten expects 1 to 3 arguments but got 0", none.getMessage());

    IllegalArgumentException four =
        assertThrows(
            IllegalArgumentException.class,
            () -> evaluateExpression("flatten($, \"_\", 2, 3)", NESTED));
    assertEquals("flatten expects 1 to 3 arguments but got 4", four.getMessage());
  }

  private void assertErrorNames(String expression, FleakData testData, String offendingValue) {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> evaluateExpression(expression, testData));
    assertTrue(e.getMessage().startsWith("flatten:"), e.getMessage());
    assertTrue(e.getMessage().contains(offendingValue), e.getMessage());
  }
}
