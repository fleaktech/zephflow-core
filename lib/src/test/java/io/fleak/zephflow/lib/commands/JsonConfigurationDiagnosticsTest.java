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
package io.fleak.zephflow.lib.commands;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.ConfigurationPath;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.lib.commands.kafkasource.KafkaSourceDto;
import io.fleak.zephflow.lib.commands.kinesissource.KinesisSourceDto;
import io.fleak.zephflow.lib.parser.ParserConfigs;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class JsonConfigurationDiagnosticsTest {
  @Test
  void identifiesTheDiscriminatorAndItsAcceptedTypes() {
    var input = Map.<String, Object>of("extractionConfig", Map.of("type", "private-value"));
    var failure =
        assertThrows(
            ConfigurationValidationException.class,
            () -> new JsonConfigParser<>(ParserConfigs.ParserConfig.class).parseConfig(input));
    var issue = failure.issues().getFirst();
    assertEquals(ConfigurationPath.of("extractionConfig", "type"), issue.path());
    assertTrue(issue.message().contains("grok"));
    assertTrue(issue.message().contains("syslog"));
    assertFalse(issue.message().contains("private-value"));
    var original =
        assertThrows(
            IllegalArgumentException.class,
            () -> JsonUtils.OBJECT_MAPPER.convertValue(input, ParserConfigs.ParserConfig.class));
    assertEquals(original.getClass(), failure.originalFailure().getClass());
    assertEquals(original.getMessage(), failure.getMessage());
  }

  @Test
  void identifiesAMissingDiscriminatorWithoutInventingAnotherField() {
    var failure =
        assertThrows(
            ConfigurationValidationException.class,
            () ->
                new JsonConfigParser<>(ParserConfigs.ParserConfig.class)
                    .parseConfig(Map.of("extractionConfig", Map.of())));
    assertEquals(
        ConfigurationPath.of("extractionConfig", "type"), failure.issues().getFirst().path());
    assertTrue(failure.issues().getFirst().message().contains("required"));
  }

  @Test
  void preservesMapKeyBoundariesInNestedParserErrors() {
    String key = "key.with.\"quotes\"[0]";
    var input =
        Map.<String, Object>of(
            "dispatchConfig",
            Map.of(
                "dispatchMap",
                Map.of(key, Map.of("extractionConfig", Map.of("type", "unsupported")))));
    var failure =
        assertThrows(
            ConfigurationValidationException.class,
            () -> new JsonConfigParser<>(ParserConfigs.ParserConfig.class).parseConfig(input));
    assertEquals(
        ConfigurationPath.of("dispatchConfig", "dispatchMap", key, "extractionConfig", "type"),
        failure.issues().getFirst().path());
  }

  @Test
  void preservesACreatorFailureWithExactlyOneJacksonPrefix() {
    var failure =
        assertThrows(
            ConfigurationValidationException.class,
            () ->
                new JsonConfigParser<>(KafkaSourceDto.Config.class)
                    .parseConfig(
                        Map.of(
                            "decoding",
                            Map.of("format", Map.of("type", "json", "mode", "private-value")))));
    var issue = failure.issues().getFirst();
    assertEquals(ConfigurationPath.of("decoding", "format", "mode"), issue.path());
    assertTrue(issue.message().contains("[array, lines, object]"));
    assertFalse(issue.message().contains("private-value"));
  }

  @Test
  void reportsTheActualEnumValuesAndKeepsLegacyFailure() {
    var input = Map.<String, Object>of("commitStrategy", "private-value");
    var failure =
        assertThrows(
            ConfigurationValidationException.class,
            () -> new JsonConfigParser<>(KafkaSourceDto.Config.class).parseConfig(input));
    assertEquals(ConfigurationPath.of("commitStrategy"), failure.issues().getFirst().path());
    assertEquals(
        "Unsupported value. Accepted values: [BATCH, NONE, PER_RECORD]",
        failure.issues().getFirst().message());
    assertEquals(IllegalArgumentException.class, failure.originalFailure().getClass());
  }

  @Test
  void enumObjectsAndArraysReportAllowedValuesInsteadOfSuggestingAnObject() {
    for (Object value : List.of(Map.of("private", "value"), List.of("BATCH"))) {
      var input = Map.<String, Object>of("commitStrategy", value);
      var failure =
          assertThrows(
              ConfigurationValidationException.class,
              () -> new JsonConfigParser<>(KafkaSourceDto.Config.class).parseConfig(input));
      assertEquals(ConfigurationPath.of("commitStrategy"), failure.issues().getFirst().path());
      var message = failure.issues().getFirst().message();
      assertTrue(message.contains("BATCH"), message);
      assertTrue(message.contains("NONE"), message);
      assertFalse(message.contains("Expected an object"), message);
      assertFalse(message.contains("private"), message);
    }
  }

  @Test
  void nestedNullRejectedByAnExistingSetterNamesTheFieldWithoutChangingLegacyFailure() {
    var extraction = new java.util.HashMap<String, Object>();
    extraction.put("type", "delimited_text");
    extraction.put("delimiter", null);
    extraction.put("columns", List.of("value"));
    var input = Map.<String, Object>of("targetField", "message", "extractionConfig", extraction);
    var original =
        assertThrows(
            IllegalArgumentException.class,
            () -> JsonUtils.OBJECT_MAPPER.convertValue(input, ParserConfigs.ParserConfig.class));
    var failure =
        assertThrows(
            ConfigurationValidationException.class,
            () -> new JsonConfigParser<>(ParserConfigs.ParserConfig.class).parseConfig(input));
    assertEquals(
        ConfigurationPath.of("extractionConfig", "delimiter"), failure.issues().getFirst().path());
    assertTrue(failure.issues().getFirst().message().contains("null"));
    assertEquals(original.getClass(), failure.originalFailure().getClass());
    assertEquals(original.getMessage(), failure.originalFailure().getMessage());
    extraction.put("delimiter", ",");
    assertDoesNotThrow(
        () -> new JsonConfigParser<>(ParserConfigs.ParserConfig.class).parseConfig(input));
  }

  @Test
  void scalarDateAndUriFailuresDescribeTheActualInputTypeAndFormat() {
    for (String field : List.of("initialPositionTimestamp", "kinesisEndpoint")) {
      var input =
          Map.<String, Object>of(
              field,
              field.equals("initialPositionTimestamp") ? "not-a-date" : "https://PRIVATE bad-host");
      var failure =
          assertThrows(
              ConfigurationValidationException.class,
              () -> new JsonConfigParser<>(KinesisSourceDto.Config.class).parseConfig(input));
      assertEquals(ConfigurationPath.of(field), failure.issues().getFirst().path());
      String message = failure.issues().getFirst().message();
      assertTrue(
          message.contains(
              field.equals("initialPositionTimestamp") ? "yyyy-MM-dd'T'HH:mm:ss.SSSZ" : "URI"),
          message);
      assertFalse(message.contains("Expected an object"));
      assertFalse(message.contains("PRIVATE"));
    }
    assertDoesNotThrow(
        () ->
            new JsonConfigParser<>(KinesisSourceDto.Config.class)
                .parseConfig(
                    Map.of(
                        "initialPositionTimestamp",
                        "2026-11-13T12:00:00.000+0000",
                        "kinesisEndpoint",
                        "https://example.test")));
  }

  @ParameterizedTest
  @ValueSource(strings = {"OBJECT", "object", "Object"})
  void preservesExistingCreatorCaseConversion(String mode) {
    assertDoesNotThrow(
        () ->
            new JsonConfigParser<>(KafkaSourceDto.Config.class)
                .parseConfig(
                    Map.of("decoding", Map.of("format", Map.of("type", "json", "mode", mode)))));
  }

  @Test
  void namesUnknownFieldsWithoutDroppingTheirExactPath() {
    var failure =
        assertThrows(
            ConfigurationValidationException.class,
            () ->
                new JsonConfigParser<>(KafkaSourceDto.Config.class)
                    .parseConfig(Map.of("unknown.key", true)));
    assertEquals(ConfigurationPath.of("unknown.key"), failure.issues().getFirst().path());
    assertTrue(failure.issues().getFirst().message().contains("groupId"));
  }
}
