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
package io.fleak.zephflow.lib.parser;

import static io.fleak.zephflow.lib.utils.JsonUtils.fromJsonBytes;
import static io.fleak.zephflow.lib.utils.MiscUtils.FIELD_NAME_RAW;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.parser.extractions.JsonExtractionConfig;
import io.fleak.zephflow.lib.parser.extractions.KvPairExtractionConfig;
import io.fleak.zephflow.lib.parser.extractions.SyslogExtractionConfig;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Created by bolei on 10/14/24 */
class CompiledRulesTest {
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void preservesExtractedContentWhenSourceHasTheSameName(boolean removeTargetField) {
    var input = (RecordFleakData) FleakData.wrap(Map.of("content", "<134> hello", "other", "kept"));
    var config =
        ParserConfigs.ParserConfig.builder()
            .targetField("content")
            .removeTargetField(removeTargetField)
            .extractionConfig(
                SyslogExtractionConfig.builder()
                    .componentList(List.of(SyslogExtractionConfig.ComponentType.PRIORITY))
                    .build())
            .build();

    var output = new ParserConfigCompiler().compile(config).parse(input);

    assertEquals(
        FleakData.wrap(Map.of("priority", 134, "content", "hello", "other", "kept")), output);
    assertEquals(Map.of("content", "<134> hello", "other", "kept"), input.unwrap());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void preservesExtractedKvPairWhenSourceHasTheSameName(boolean removeTargetField) {
    var input = (RecordFleakData) FleakData.wrap(Map.of("message", "message=hello action=login"));
    var config =
        ParserConfigs.ParserConfig.builder()
            .targetField("message")
            .removeTargetField(removeTargetField)
            .extractionConfig(new KvPairExtractionConfig(" ", "="))
            .build();

    var output = new ParserConfigCompiler().compile(config).parse(input);

    assertEquals(Map.of("message", "hello", "action", "login"), output.unwrap());
    assertEquals(Map.of("message", "message=hello action=login"), input.unwrap());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void preservesJsonOutputWhenSourceHasTheSameName(boolean removeTargetField) {
    var input = (RecordFleakData) FleakData.wrap(Map.of("message", "{\"key\":\"value\"}"));
    var config =
        ParserConfigs.ParserConfig.builder()
            .targetField("message")
            .removeTargetField(removeTargetField)
            .extractionConfig(new JsonExtractionConfig("message"))
            .build();

    var output = new ParserConfigCompiler().compile(config).parse(input);

    assertEquals(Map.of("message", Map.of("key", "value")), output.unwrap());
    assertEquals(Map.of("message", "{\"key\":\"value\"}"), input.unwrap());
  }

  @Test
  void dispatchUsesExtractedFieldAfterSourceRemoval() {
    var input = (RecordFleakData) FleakData.wrap(Map.of("content", "<134> action=login"));
    var config =
        ParserConfigs.ParserConfig.builder()
            .targetField("content")
            .removeTargetField(true)
            .extractionConfig(
                SyslogExtractionConfig.builder()
                    .componentList(List.of(SyslogExtractionConfig.ComponentType.PRIORITY))
                    .build())
            .dispatchConfig(
                ParserConfigs.DispatchConfig.builder()
                    .dispatchField("content")
                    .defaultConfig(
                        ParserConfigs.ParserConfig.builder()
                            .targetField("content")
                            .removeTargetField(true)
                            .extractionConfig(new KvPairExtractionConfig(" ", "="))
                            .build())
                    .build())
            .build();

    var output = new ParserConfigCompiler().compile(config).parse(input);

    assertEquals(FleakData.wrap(Map.of("priority", 134, "action", "login")), output);
    assertEquals(Map.of("content", "<134> action=login"), input.unwrap());
  }

  @Test
  public void testParseCiscoAsaLog() throws Exception {
    ParserConfigCompiler parserConfigCompiler = new ParserConfigCompiler();

    ParserConfigs.ParserConfig parserConfig =
        JsonUtils.fromJsonResource("/parser/cisco_asa_config.json", new TypeReference<>() {});
    CompiledRules.ParseRule parseRule = parserConfigCompiler.compile(parserConfig);

    try (InputStream testInput = this.getClass().getResourceAsStream("/parser/cisco_asa_data.txt");
        InputStream expectedIn =
            this.getClass().getResourceAsStream("/parser/cisco_asa_parsed.json")) {
      byte[] input = IOUtils.toByteArray(Objects.requireNonNull(testInput));
      byte[] expectedOutput = IOUtils.toByteArray(Objects.requireNonNull(expectedIn));

      List<RecordFleakData> inputEvents = lines(input);
      List<RecordFleakData> parsedEvents = inputEvents.stream().map(parseRule::parse).toList();

      List<RecordFleakData> expected = jsonObjects(expectedOutput);
      assertEquals(expected, parsedEvents);
    }
  }

  @Test
  public void testJsonExtractionRule() {
    RecordFleakData inputEvent = (RecordFleakData) FleakData.wrap(Map.of("k", "{\"a\":100}"));
    ParserConfigs.ParserConfig parserConfig =
        ParserConfigs.ParserConfig.builder()
            .targetField("k")
            .extractionConfig(new JsonExtractionConfig("my_field"))
            .removeTargetField(true)
            .build();
    ParserConfigCompiler parserConfigCompiler = new ParserConfigCompiler();
    CompiledRules.ParseRule parseRule = parserConfigCompiler.compile(parserConfig);
    assertNotNull(inputEvent);
    RecordFleakData output = parseRule.parse(inputEvent);
    assertEquals(Map.of("my_field", Map.of("a", 100L)), output.unwrap());
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"", "  "})
  public void testJsonExtractionWithoutOutputFieldNameUsesParsedJson(String outputFieldName) {
    RecordFleakData inputEvent = (RecordFleakData) FleakData.wrap(Map.of("k", "{\"a\":100}"));
    ParserConfigs.ParserConfig parserConfig =
        ParserConfigs.ParserConfig.builder()
            .targetField("k")
            .extractionConfig(new JsonExtractionConfig(outputFieldName))
            .build();
    CompiledRules.ParseRule parseRule = new ParserConfigCompiler().compile(parserConfig);
    RecordFleakData output = parseRule.parse(inputEvent);
    assertEquals(Map.of("k", "{\"a\":100}", "parsed_json", Map.of("a", 100L)), output.unwrap());
  }

  @Test
  public void testNonStringTargetFieldReturnsInputUnchanged() {
    RecordFleakData inputEvent =
        (RecordFleakData) FleakData.wrap(Map.of("k", Map.of("a", 100L), "other", "value"));
    ParserConfigs.ParserConfig parserConfig =
        ParserConfigs.ParserConfig.builder()
            .targetField("k")
            .extractionConfig(new JsonExtractionConfig("my_field"))
            .removeTargetField(true)
            .build();
    ParserConfigCompiler parserConfigCompiler = new ParserConfigCompiler();
    CompiledRules.ParseRule parseRule = parserConfigCompiler.compile(parserConfig);
    RecordFleakData output = parseRule.parse(inputEvent);
    assertEquals(inputEvent, output);
  }

  @Test
  public void testEmptyTargetFieldReturnsInputUnchanged() {
    RecordFleakData inputEvent =
        (RecordFleakData) FleakData.wrap(Map.of("k", "", "other", "value"));
    ParserConfigs.ParserConfig parserConfig =
        ParserConfigs.ParserConfig.builder()
            .targetField("k")
            .extractionConfig(new JsonExtractionConfig("my_field"))
            .removeTargetField(true)
            .build();
    ParserConfigCompiler parserConfigCompiler = new ParserConfigCompiler();
    CompiledRules.ParseRule parseRule = parserConfigCompiler.compile(parserConfig);
    RecordFleakData output = parseRule.parse(inputEvent);
    assertEquals(inputEvent, output);
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
}
