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
import io.fleak.zephflow.api.JobContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class OperatorConfigurationDiagnosticsTest {
  private static void validate(String name, Map<String, Object> config) {
    OperatorCommandRegistry.OPERATOR_COMMANDS
        .get(name)
        .createCommand("subject", JobContext.builder().build())
        .parseAndValidateArg(config);
  }

  private static Map<String, Object> kafka() {
    return new HashMap<>(
        Map.of(
            "broker",
            "localhost:9092",
            "topic",
            "events",
            "groupId",
            "consumer",
            "decoding",
            Map.of("format", Map.of("type", "json", "mode", "object"))));
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "\t"})
  void identifiesMissingOrBlankKafkaGroupWithoutChangingTheRule(String group) {
    var config = kafka();
    config.put("groupId", group);
    var failure =
        assertThrows(ConfigurationValidationException.class, () -> validate("kafkasource", config));
    assertEquals(ConfigurationPath.of("groupId"), failure.issues().getFirst().path());
    assertTrue(failure.issues().getFirst().message().contains("must not be blank"));
    assertEquals("no consumer group Id is provided", failure.originalFailure().getMessage());
  }

  @Test
  void retainsTheConditionalBatchConstraint() {
    var config = kafka();
    config.put("commitStrategy", "BATCH");
    config.put("commitBatchSize", 0);
    var failure =
        assertThrows(ConfigurationValidationException.class, () -> validate("kafkasource", config));
    assertEquals(ConfigurationPath.of("commitBatchSize"), failure.issues().getFirst().path());
    assertTrue(failure.issues().getFirst().message().contains("BATCH"));
    config.put("commitStrategy", "NONE");
    assertDoesNotThrow(() -> validate("kafkasource", config));
    config.put("commitStrategy", "BATCH");
    config.put("commitBatchSize", 1);
    assertDoesNotThrow(() -> validate("kafkasource", config));
  }

  @ParameterizedTest
  @ValueSource(strings = {"", " ", "dict(value = 'do-not-copy-this'"})
  void locatesExpressionSyntaxWithoutCopyingInput(String expression) {
    var failure =
        assertThrows(
            ConfigurationValidationException.class,
            () -> validate("filter", Map.of("expression", expression)));
    assertEquals(ConfigurationPath.of("expression"), failure.issues().getFirst().path());
    assertTrue(failure.issues().getFirst().message().contains("line 1"));
    assertFalse(failure.issues().getFirst().message().contains("do-not-copy-this"));
  }

  @Test
  void retainsATypeFailureWhileNamingTheExpressionField() {
    var failure =
        assertThrows(
            ConfigurationValidationException.class,
            () -> validate("filter", Map.of("expression", Map.of())));
    assertEquals(ConfigurationPath.of("expression"), failure.issues().getFirst().path());
    assertInstanceOf(ClassCastException.class, failure.originalFailure());
    assertDoesNotThrow(() -> validate("filter", Map.of("expression", "true")));
  }

  @Test
  void identifiesExpressionCompilationFailure() {
    var failure =
        assertThrows(
            ConfigurationValidationException.class,
            () -> validate("eval", Map.of("expression", "undefined_variable")));
    assertEquals(ConfigurationPath.of("expression"), failure.issues().getFirst().path());
    assertTrue(failure.issues().getFirst().message().contains("not defined"));
  }

  @Test
  void locatesTheSecondSampleRuleAndRetainsTheIntegerBounds() {
    var config =
        Map.<String, Object>of("rules", List.of(Map.of("sampleRate", 1), Map.of("sampleRate", 0)));
    var failure =
        assertThrows(ConfigurationValidationException.class, () -> validate("sample", config));
    assertEquals(
        ConfigurationPath.of("rules").index(1).field("sampleRate"),
        failure.issues().getFirst().path());
    assertEquals("Expected an integer from 1 to 2147483647", failure.issues().getFirst().message());
    assertDoesNotThrow(() -> validate("sample", Map.of("rules", List.of(Map.of("sampleRate", 1)))));
  }

  @Test
  void locatesRequiredParserFieldsWithoutAddingBlankValidation() {
    var extraction = Map.of("type", "grok", "grokExpression", "%{NONEXISTENT_PATTERN}");
    var failure =
        assertThrows(
            ConfigurationValidationException.class,
            () -> validate("parser", Map.of("extractionConfig", extraction)));
    assertEquals(ConfigurationPath.of("targetField"), failure.issues().getFirst().path());
    assertDoesNotThrow(
        () -> validate("parser", Map.of("targetField", "", "extractionConfig", extraction)));
  }
}
