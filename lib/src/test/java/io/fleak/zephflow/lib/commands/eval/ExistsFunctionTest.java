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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.lib.utils.JsonUtils;
import org.junit.jupiter.api.Test;

class ExistsFunctionTest extends FeelFunctionTestBase {

  private static FleakData event(String json) {
    return FleakData.wrap(
        JsonUtils.fromJsonString(json, new com.fasterxml.jackson.core.type.TypeReference<>() {}));
  }

  @Test
  void presentKeyWithValue() {
    testFunctionExecution(event("{\"a\": 1}"), "exists($.a)", true);
  }

  @Test
  void presentKeyWithJsonNullIsStillPresent() {
    testFunctionExecution(event("{\"a\": null}"), "exists($.a)", true);
  }

  @Test
  void absentKey() {
    testFunctionExecution(event("{\"a\": 1}"), "exists($.b)", false);
  }

  @Test
  void nullAndAbsentAreDistinguishable() {
    FleakData nullValued = event("{\"a\": null}");
    FleakData absent = event("{\"b\": 1}");

    // the whole point: '== null' cannot tell these apart, exists() can
    testFunctionExecution(nullValued, "$.a == null", true);
    testFunctionExecution(absent, "$.a == null", true);
    testFunctionExecution(nullValued, "exists($.a)", true);
    testFunctionExecution(absent, "exists($.a)", false);
  }

  @Test
  void nestedPath() {
    FleakData data = event("{\"a\": {\"b\": null, \"c\": 1}}");
    testFunctionExecution(data, "exists($.a.b)", true);
    testFunctionExecution(data, "exists($.a.c)", true);
    testFunctionExecution(data, "exists($.a.d)", false);
  }

  @Test
  void missingIntermediateContainer() {
    testFunctionExecution(event("{\"a\": 1}"), "exists($.x.y)", false);
  }

  @Test
  void scalarIntermediateIsNotAContainer() {
    testFunctionExecution(event("{\"a\": 1}"), "exists($.a.b)", false);
  }

  @Test
  void arrayIndexWithinAndOutsideBounds() {
    FleakData data = event("{\"list\": [10, null]}");
    testFunctionExecution(data, "exists($.list[0])", true);
    testFunctionExecution(data, "exists($.list[1])", true);
    testFunctionExecution(data, "exists($.list[2])", false);
  }

  @Test
  void fieldUnderArrayIsNotAddressableByDottedPath() {
    // documents the shape the inspector must not generate a dotted predicate for
    testFunctionExecution(event("{\"a\": [{\"b\": \"x\"}]}"), "exists($.a.b)", false);
  }

  @Test
  void literalDottedKey() {
    testFunctionExecution(event("{\"source.ip\": \"1.2.3.4\"}"), "exists($[\"source.ip\"])", true);
    testFunctionExecution(
        event("{\"source\": {\"ip\": \"1.2.3.4\"}}"), "exists($[\"source.ip\"])", false);
  }

  @Test
  void usableAsAFilterPredicateAlongsideValueComparison() {
    FleakData nullValued = event("{\"env\": null}");
    FleakData absent = event("{\"other\": 1}");
    FleakData valued = event("{\"env\": \"prod\"}");

    testFunctionExecution(nullValued, "exists($.env) and $.env == null", true);
    testFunctionExecution(absent, "exists($.env) and $.env == null", false);
    testFunctionExecution(valued, "exists($.env) and $.env == null", false);

    testFunctionExecution(absent, "not exists($.env)", true);
    testFunctionExecution(nullValued, "not exists($.env)", false);
  }

  @Test
  void nonPathArgumentIsRejected() {
    Exception e =
        assertThrows(
            Exception.class, () -> evaluateExpression("exists(\"a\")", event("{\"a\": 1}")));
    assertTrue(
        e.getMessage() != null && e.getMessage().contains("exists"),
        "unexpected message: " + e.getMessage());
  }

  @Test
  void rootAlwaysExists() {
    FleakData result = evaluateExpression("exists($)", event("{\"a\": 1}"));
    assertTrue(result.isTrueValue());
    assertFalse(!result.isTrueValue());
  }
}
