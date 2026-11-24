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
package io.fleak.zephflow.lib.commands.parser;

import static io.fleak.zephflow.lib.TestUtils.JOB_CONTEXT;
import static io.fleak.zephflow.lib.utils.JsonUtils.fromJsonBytes;
import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_RAW;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.parser.CompiledRules;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

/** Created by bolei on 3/18/25 */
class ParserCommandTest {
  @Test
  void detailedExecutionUsesTheSameCompiledCommandAndPreservesSource() throws Exception {
    var command = (ParserCommand) new ParserCommandFactory().createCommand("parse", JOB_CONTEXT);
    command.parseAndValidateArg(
        Map.of(
            "targetField",
            "payload.message",
            "targetFieldPath",
            List.of("payload", "message"),
            "removeTargetField",
            true,
            "extractionConfig",
            Map.of("type", "json", "outputFieldName", "parsed")));
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
    var context = (ParserExecutionContext) command.getExecutionContext();
    try {
      var input =
          (RecordFleakData) FleakData.wrap(Map.of("payload", Map.of("message", "{\"a\":1}")));
      var detailed = context.getParseRule().parseDetailed(input);
      assertEquals(
          command.processOneEvent(input, "test_user", context).getFirst(), detailed.outputRecord());
      assertEquals(Map.of("payload", Map.of("message", "{\"a\":1}")), input.unwrap());
      var invalid =
          (RecordFleakData) FleakData.wrap(Map.of("payload", Map.of("message", "startup")));
      var ordinary =
          assertThrows(
              RuntimeException.class, () -> command.processOneEvent(invalid, "test_user", context));
      var failure =
          assertThrows(
              CompiledRules.DetailedParseException.class,
              () -> context.getParseRule().parseDetailed(invalid));
      assertEquals(ordinary.getMessage(), failure.getMessage());
      assertEquals(ordinary.getCause().getClass(), failure.getCause().getClass());
      assertEquals(ordinary.getCause().getMessage(), failure.getCause().getMessage());
    } finally {
      context.close();
    }
  }

  @Test
  public void testParse() throws IOException {
    Map<String, Object> parserConfigMap =
        JsonUtils.fromJsonResource("/parser/cisco_asa_config.json", new TypeReference<>() {});
    ParserCommand command =
        (ParserCommand) new ParserCommandFactory().createCommand("my_node", JOB_CONTEXT);
    command.parseAndValidateArg(parserConfigMap);

    try (InputStream testInput = this.getClass().getResourceAsStream("/parser/cisco_asa_data.txt");
        InputStream expectedIn =
            this.getClass().getResourceAsStream("/parser/cisco_asa_parsed.json")) {
      byte[] input = IOUtils.toByteArray(Objects.requireNonNull(testInput));
      byte[] expectedOutput = IOUtils.toByteArray(Objects.requireNonNull(expectedIn));

      List<RecordFleakData> inputEvents = lines(input);
      command.initialize(new MetricClientProvider.NoopMetricClientProvider());
      var context = command.getExecutionContext();
      var processResult = command.process(inputEvents, "test_user", context);

      List<RecordFleakData> parsedEvents = processResult.getOutput();

      List<RecordFleakData> expected = jsonObjects(expectedOutput);
      assertEquals(expected, parsedEvents);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void testParseKvWithTabSeparator() throws Exception {
    Map<String, Object> parserConfigMap =
        JsonUtils.fromJsonResource("/parser/zscaler_kv_config.json", new TypeReference<>() {});
    ParserCommand command =
        (ParserCommand) new ParserCommandFactory().createCommand("my_node", JOB_CONTEXT);
    command.parseAndValidateArg(parserConfigMap);

    try (InputStream testInput =
            this.getClass().getResourceAsStream("/parser/zscaler_kv_data.json");
        InputStream expectedIn =
            this.getClass().getResourceAsStream("/parser/zscaler_kv_parsed.json")) {
      byte[] input = IOUtils.toByteArray(Objects.requireNonNull(testInput));
      byte[] expectedOutput = IOUtils.toByteArray(Objects.requireNonNull(expectedIn));

      List<RecordFleakData> inputEvents = List.of(jsonObject(input));
      command.initialize(new MetricClientProvider.NoopMetricClientProvider());
      var context = command.getExecutionContext();
      var processResult = command.process(inputEvents, "test_user", context);

      List<RecordFleakData> parsedEvents = processResult.getOutput();

      List<RecordFleakData> expected = List.of(jsonObject(expectedOutput));
      assertEquals(expected, parsedEvents);
    }
  }

  /** Each line as a {@code __raw__} record, as STRING_LINE read it. */
  private static List<RecordFleakData> lines(byte[] bytes) {
    return new String(bytes, StandardCharsets.UTF_8)
        .lines()
        .map(
            line -> new RecordFleakData(Map.of(FIELD_NAME_RAW, new StringPrimitiveFleakData(line))))
        .toList();
  }

  /** The array's objects as records, as JSON_ARRAY read them. */
  private static List<RecordFleakData> jsonObjects(byte[] bytes) {
    List<ObjectNode> objects = fromJsonBytes(bytes, new TypeReference<>() {});
    return objects.stream().map(JsonUtils::fromJsonPayload).toList();
  }

  /** The object as a record, as JSON_OBJECT read it. */
  private static RecordFleakData jsonObject(byte[] bytes) {
    return JsonUtils.fromJsonPayload(fromJsonBytes(bytes, new TypeReference<ObjectNode>() {}));
  }
}
