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
package io.fleak.zephflow.lib.commands.throttle;

import static io.fleak.zephflow.lib.TestUtils.JOB_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.throttle.ThrottleCommandDto.Config;
import io.fleak.zephflow.lib.utils.JsonUtils;
import io.fleak.zephflow.lib.utils.YamlUtils;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class ThrottleConfigTest {

  private final ThrottleConfigParser parser = new ThrottleConfigParser();
  private final ThrottleConfigValidator validator = new ThrottleConfigValidator();

  private static final List<String> PARAMS =
      List.of("numToAllow", "periodSeconds", "cacheSizeLimit");

  private static final Map<String, String> NOT_INTEGER =
      Map.of(
          "3.7", "3.7",
          "0.5", "0.5",
          "3.0", "3.0",
          "1e2", "100.0",
          "\"3\"", "\"3\"",
          "true", "true",
          "[3]", "[3]",
          "{\"a\": 3}", "{a=3}",
          "null", "null");

  private void validate(CommandConfig config) {
    validator.validateConfig(config, "n1", JobContext.builder().build());
  }

  private static Map<String, Object> json(String s) {
    return JsonUtils.fromJsonString(s, new TypeReference<>() {});
  }

  private static Map<String, Object> yaml(String s) {
    return YamlUtils.fromYamlString(s, new TypeReference<>() {});
  }

  private static String withParam(String name, String jsonValue) {
    return String.format("{\"keyExpression\": \"$.host\", \"%s\": %s}", name, jsonValue);
  }

  @Test
  void parse_appliesDefaultsForOmittedFields() {
    assertEquals(
        new Config("$.host", 1, 30L, 50_000),
        parser.parseConfig(Map.of("keyExpression", "$.host")));
  }

  @Test
  void parse_readsExplicitValues() {
    assertEquals(
        new Config("$.host", 5, 60L, 100),
        parser.parseConfig(
            Map.of(
                "keyExpression", "$.host",
                "numToAllow", 5,
                "periodSeconds", 60,
                "cacheSizeLimit", 100)));
  }

  @Test
  void parse_rejectsMissingKeyExpression() {
    assertThrows(IllegalArgumentException.class, () -> parser.parseConfig(Map.of()));
  }

  static Stream<Arguments> invalidNumericParams() {
    Stream<Arguments> notInteger =
        PARAMS.stream()
            .flatMap(
                p ->
                    NOT_INTEGER.entrySet().stream()
                        .map(
                            v ->
                                Arguments.of(
                                    p,
                                    v.getKey(),
                                    "'" + p + "' must be an integer, got: " + v.getValue())));
    Stream<Arguments> outOfRange =
        Stream.of(
            Arguments.of(
                "numToAllow", "0", "'numToAllow' must be between 1 and 2147483647, got: 0"),
            Arguments.of(
                "numToAllow", "-1", "'numToAllow' must be between 1 and 2147483647, got: -1"),
            Arguments.of(
                "numToAllow",
                "2147483648",
                "'numToAllow' must be between 1 and 2147483647, got: 2147483648"),
            Arguments.of(
                "numToAllow",
                "4294967297",
                "'numToAllow' must be between 1 and 2147483647, got: 4294967297"),
            Arguments.of(
                "numToAllow",
                "99999999999999999999",
                "'numToAllow' must be between 1 and 2147483647, got: 99999999999999999999"),
            Arguments.of(
                "periodSeconds", "0", "'periodSeconds' must be between 1 and 31536000, got: 0"),
            Arguments.of(
                "periodSeconds", "-1", "'periodSeconds' must be between 1 and 31536000, got: -1"),
            Arguments.of(
                "periodSeconds",
                "99999999999999999999",
                "'periodSeconds' must be between 1 and 31536000, got: 99999999999999999999"),
            Arguments.of(
                "cacheSizeLimit", "0", "'cacheSizeLimit' must be between 1 and 2147483647, got: 0"),
            Arguments.of(
                "cacheSizeLimit",
                "99999999999999999999",
                "'cacheSizeLimit' must be between 1 and 2147483647, got: 99999999999999999999"),
            Arguments.of(
                "periodSeconds",
                "31536001",
                "'periodSeconds' must be between 1 and 31536000, got: 31536001"),
            Arguments.of(
                "periodSeconds",
                "9223372036854775807",
                "'periodSeconds' must be between 1 and 31536000, got: 9223372036854775807"),
            Arguments.of(
                "cacheSizeLimit",
                "-1",
                "'cacheSizeLimit' must be between 1 and 2147483647, got: -1"),
            Arguments.of(
                "cacheSizeLimit",
                "2147483648",
                "'cacheSizeLimit' must be between 1 and 2147483647, got: 2147483648"),
            Arguments.of(
                "cacheSizeLimit",
                "4294967297",
                "'cacheSizeLimit' must be between 1 and 2147483647, got: 4294967297"),
            Arguments.of(
                "numToAllow",
                "-99999999999999999999",
                "'numToAllow' must be between 1 and 2147483647, got: -99999999999999999999"),
            Arguments.of(
                "periodSeconds",
                "-99999999999999999999",
                "'periodSeconds' must be between 1 and 31536000, got: -99999999999999999999"),
            Arguments.of(
                "cacheSizeLimit",
                "-99999999999999999999",
                "'cacheSizeLimit' must be between 1 and 2147483647, got: -99999999999999999999"));
    return Stream.concat(notInteger, outOfRange);
  }

  @ParameterizedTest
  @MethodSource("invalidNumericParams")
  void rejectsInvalidNumericParam(String name, String jsonValue, String expectedMessage) {
    ThrottleCommand cmd =
        (ThrottleCommand) new ThrottleCommandFactory().createCommand("n1", JOB_CONTEXT);
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> cmd.parseAndValidateArg(json(withParam(name, jsonValue))));
    assertEquals(expectedMessage, e.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"numToAlow", "NumToAllow", "foo"})
  void rejectsUnknownParam(String name) {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> parser.parseConfig(json(withParam(name, "3"))));
    assertEquals(
        "unknown parameter '"
            + name
            + "'; allowed: [cacheSizeLimit, keyExpression, numToAllow, periodSeconds]",
        e.getMessage());
  }

  @Test
  void rejectsNonStringKey() {
    Map<Object, Object> config = new HashMap<>(Map.of("keyExpression", "$.host", 1, 3));
    @SuppressWarnings("unchecked")
    Map<String, Object> raw = (Map<String, Object>) (Map<?, ?>) config;
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> parser.parseConfig(raw));
    assertEquals(
        "unknown parameter '1'; allowed: [cacheSizeLimit, keyExpression, numToAllow, periodSeconds]",
        e.getMessage());
  }

  static Stream<Arguments> validConfigs() {
    return Stream.of(
        Arguments.of(
            "{\"keyExpression\": \"$.host\", \"numToAllow\": 1, \"periodSeconds\": 1,"
                + " \"cacheSizeLimit\": 1}",
            new Config("$.host", 1, 1L, 1)),
        Arguments.of(
            "{\"keyExpression\": \"$.host\", \"numToAllow\": 2147483647,"
                + " \"periodSeconds\": 31536000, \"cacheSizeLimit\": 2147483647}",
            new Config("$.host", Integer.MAX_VALUE, 31_536_000L, Integer.MAX_VALUE)),
        Arguments.of("{\"keyExpression\": \"$.host\"}", new Config("$.host", 1, 30L, 50_000)));
  }

  @ParameterizedTest
  @MethodSource("validConfigs")
  void acceptsValidConfig(String configJson, Config expected) {
    CommandConfig config = parser.parseConfig(json(configJson));
    validate(config);
    assertEquals(expected, config);
  }

  @Test
  void acceptsLongValuesWithinRange() {
    assertEquals(
        new Config("$.host", 5, 60L, 100),
        parser.parseConfig(
            Map.of(
                "keyExpression", "$.host",
                "numToAllow", 5L,
                "periodSeconds", 60L,
                "cacheSizeLimit", 100L)));
  }

  @Test
  void acceptsAllIntegralJavaTypesWithinRange() {
    assertEquals(
        new Config("$.host", Integer.MAX_VALUE, 60L, 7),
        parser.parseConfig(
            Map.of(
                "keyExpression",
                "$.host",
                "numToAllow",
                BigInteger.valueOf(Integer.MAX_VALUE),
                "periodSeconds",
                (short) 60,
                "cacheSizeLimit",
                (byte) 7)));
  }

  @Test
  void rejectsBigDecimal() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                parser.parseConfig(
                    Map.of("keyExpression", "$.host", "numToAllow", new BigDecimal("3.5"))));
    assertEquals("'numToAllow' must be an integer, got: 3.5", e.getMessage());
  }

  static Stream<Arguments> invalidYamlValues() {
    return Stream.of(
        Arguments.of("", "'numToAllow' must be an integer, got: null"),
        Arguments.of("~", "'numToAllow' must be an integer, got: null"),
        Arguments.of("3.7", "'numToAllow' must be an integer, got: 3.7"),
        Arguments.of("1e2", "'numToAllow' must be an integer, got: 100.0"),
        Arguments.of("\"3\"", "'numToAllow' must be an integer, got: \"3\""),
        Arguments.of("'3'", "'numToAllow' must be an integer, got: \"3\""),
        Arguments.of("yes", "'numToAllow' must be an integer, got: true"),
        Arguments.of("[3]", "'numToAllow' must be an integer, got: [3]"),
        Arguments.of("{a: 3}", "'numToAllow' must be an integer, got: {a=3}"),
        Arguments.of("0", "'numToAllow' must be between 1 and 2147483647, got: 0"),
        Arguments.of(
            "4294967297", "'numToAllow' must be between 1 and 2147483647, got: 4294967297"),
        Arguments.of(
            "0xFFFFFFFF", "'numToAllow' must be between 1 and 2147483647, got: 4294967295"),
        Arguments.of(
            "99999999999999999999",
            "'numToAllow' must be between 1 and 2147483647, got: 99999999999999999999"),
        Arguments.of(
            "-99999999999999999999",
            "'numToAllow' must be between 1 and 2147483647, got: -99999999999999999999"));
  }

  @ParameterizedTest
  @MethodSource("invalidYamlValues")
  void rejectsInvalidYamlValue(String yamlValue, String expectedMessage) {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> parser.parseConfig(yaml("keyExpression: $.host\nnumToAllow: " + yamlValue)));
    assertEquals(expectedMessage, e.getMessage());
  }

  @Test
  void rejectsYamlPeriodAboveOneYear() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> parser.parseConfig(yaml("keyExpression: $.host\nperiodSeconds: 31536001")));
    assertEquals("'periodSeconds' must be between 1 and 31536000, got: 31536001", e.getMessage());
  }

  @Test
  void acceptsYamlIntegers() {
    assertEquals(
        new Config("$.host", 16, 31_536_000L, 1000),
        parser.parseConfig(
            yaml(
                """
                keyExpression: $.host
                numToAllow: 0x10
                periodSeconds: 31536000
                cacheSizeLimit: 1_000
                """)));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"keyExpression\": 5}",
        "{\"keyExpression\": \"\"}",
        "{\"keyExpression\": \"   \"}",
        "{\"keyExpression\": null}",
        "{\"keyExpression\": [\"$.host\"]}",
        "{\"numToAllow\": 1}"
      })
  void rejectsInvalidKeyExpression(String configJson) {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> parser.parseConfig(json(configJson)));
    assertEquals("throttle command requires 'keyExpression' to be configured", e.getMessage());
  }

  @Test
  void validate_rejectsBadExpressionAndOutOfRangeValues() {
    assertThrows(Exception.class, () -> validate(new Config("* 3", 1, 30L, 50_000)));
    assertThrows(
        IllegalArgumentException.class, () -> validate(new Config("$.host", 0, 30L, 50_000)));
    assertThrows(
        IllegalArgumentException.class, () -> validate(new Config("$.host", 1, 0L, 50_000)));
    assertThrows(IllegalArgumentException.class, () -> validate(new Config("$.host", 1, 30L, 0)));
  }

  @Test
  void validate_acceptsValidConfig() {
    validate(new Config("$.host", 1, 30L, 50_000));
  }
}
