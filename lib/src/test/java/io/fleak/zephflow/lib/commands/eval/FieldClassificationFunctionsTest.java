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

import static io.fleak.zephflow.lib.TestUtils.JOB_CONTEXT;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fleak.zephflow.api.ScalarCommand;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FieldPopulationSelector;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.OperatorCommandRegistry;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FieldClassificationFunctionsTest extends FeelFunctionTestBase {
  @Test
  void compiledPredicatesPreserveTheActualSerializedEngineValues() throws Exception {
    for (String token :
        List.of("1", "1.0", "1e3", "-0.0", "9007199254740993", "9223372036854775807")) {
      RecordFleakData record = event("{\"v\":" + token + "}");
      String staged = JsonUtils.toJsonString(record.unwrap());
      List<String> scalars = new ArrayList<>();
      assertTrue(FieldPopulationSelector.visitJson(staged, "v", scalars::add));
      assertEquals(1, scalars.size());
      String expression = expression("v", scalars.getFirst());
      testFunctionExecution(record, expression, true);
      assertEquals(1, filter(expression, List.of(record)).size(), staged);
      if (token.equals("9007199254740993")) {
        assertEquals("9007199254740992", scalars.getFirst());
        testFunctionExecution(record, expression("v", token), false);
      }
    }
    testFunctionExecution(event("{\"v\":1.0}"), expression("v", "1"), false);
    testFunctionExecution(event("{\"v\":1}"), expression("v", "1.0"), false);
    testFunctionExecution(event("{\"v\":1.0000001}"), expression("v", "1"), false);
    // The established comparison operator is deliberately unchanged.
    testFunctionExecution(event("{\"v\":1.0000001}"), "$.v == 1", true);
  }

  @Test
  void realFilterSeparatesMissingNullContainersAndTypedValues() throws Exception {
    List<RecordFleakData> input =
        List.of(
            event("{\"id\":1,\"v\":null}"),
            event("{\"id\":2}"),
            event("{\"id\":3,\"v\":[]}"),
            event("{\"id\":4,\"v\":{}}"),
            event("{\"id\":5,\"v\":200}"),
            event("{\"id\":6,\"v\":\"200\"}"));
    assertEquals(List.of(1L, 3L, 4L, 5L, 6L), ids(filter("field_present($, \"v\")", input)));
    assertEquals(List.of(2L), ids(filter("not field_present($, \"v\")", input)));
    assertEquals(List.of(1L), ids(filter(expression("v", "null"), input)));
    assertEquals(List.of(5L), ids(filter(expression("v", "200"), input)));
    assertEquals(List.of(6L), ids(filter(expression("v", "\"200\""), input)));
  }

  @Test
  void realFilterTraversesArraysAndLogicalDottedNamesWithoutDuplicateOutputs() throws Exception {
    List<RecordFleakData> input =
        List.of(
            event("{\"id\":1,\"a.b\":\"x\",\"a\":{\"b\":\"y\"}}"),
            event("{\"id\":2,\"a\":[{\"b\":[[\"x\",\"new\"],\"x\"]}]}"),
            event("{\"id\":3,\"a\":{\"b\":[]}}"),
            event("{\"id\":4,\"a\":{}}"));
    assertEquals(List.of(1L, 2L), ids(filter(expression("a.b", "\"x\""), input)));
    assertEquals(List.of(1L), ids(filter(expression("a.b", "\"y\""), input)));
    assertEquals(List.of(1L, 2L, 3L), ids(filter("field_present($, \"a.b\")", input)));
    assertEquals(List.of(4L), ids(filter("not field_present($, \"a.b\")", input)));
  }

  @Test
  void quotesNamesAndScalarDataWithoutEvaluatingTheirContents() throws Exception {
    String name = "q\"\\\nżółć";
    String value = "\" or true or \"\\\n";
    ObjectNode json = JsonUtils.OBJECT_MAPPER.createObjectNode();
    json.put(name, value);
    RecordFleakData record = JsonUtils.fromJsonPayload(json);
    testFunctionExecution(record, expression(name, FieldPopulationSelector.quote(value)), true);
    testFunctionExecution(
        event("{}"), expression(name, FieldPopulationSelector.quote(value)), false);
    testFunctionExecution(event("{\"\":false}"), expression("", "false"), true);
  }

  @Test
  void validatesArityArgumentTypesAndScalarJson() throws Exception {
    RecordFleakData input = event("{\"v\":1}");
    for (String expression :
        List.of(
            "field_present($)",
            "field_present($, \"v\", true)",
            "field_present($, 1)",
            "field_present(null, \"v\")",
            "field_value_eq($, \"v\")",
            "field_value_eq($, \"v\", 1)",
            "field_value_eq($, \"v\", \"[]\")",
            "field_value_eq($, \"v\", \"{}\")",
            "field_value_eq($, \"v\", \"true false\")")) {
      assertThrows(Exception.class, () -> evaluateExpression(expression, input), expression);
    }
  }

  private static String expression(String field, String scalar) {
    return "field_value_eq($, "
        + FieldPopulationSelector.quote(field)
        + ", "
        + FieldPopulationSelector.quote(scalar)
        + ")";
  }

  private static RecordFleakData event(String json) throws Exception {
    return JsonUtils.fromJsonPayload((ObjectNode) JsonUtils.OBJECT_MAPPER.readTree(json));
  }

  private static List<Long> ids(List<RecordFleakData> records) {
    return records.stream()
        .map(record -> ((Number) record.getPayload().get("id").unwrap()).longValue())
        .toList();
  }

  private static List<RecordFleakData> filter(String expression, List<RecordFleakData> input)
      throws java.io.IOException {
    ScalarCommand filter =
        (ScalarCommand)
            OperatorCommandRegistry.OPERATOR_COMMANDS
                .get("filter")
                .createCommand("classification", JOB_CONTEXT);
    // Round-trip the saved configuration before compiling the real command.
    Map<String, Object> config =
        JsonUtils.fromJsonString(
            JsonUtils.toJsonString(Map.of("expression", expression)),
            new com.fasterxml.jackson.core.type.TypeReference<>() {});
    filter.parseAndValidateArg(config);
    filter.initialize(new MetricClientProvider.NoopMetricClientProvider());
    try {
      var result = filter.process(input, "classification", filter.getExecutionContext());
      assertTrue(result.getFailureEvents().isEmpty(), result.getFailureEvents().toString());
      return result.getOutput();
    } finally {
      filter.terminate();
    }
  }
}
