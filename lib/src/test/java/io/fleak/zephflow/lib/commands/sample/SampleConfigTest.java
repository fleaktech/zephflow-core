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
package io.fleak.zephflow.lib.commands.sample;

import static io.fleak.zephflow.lib.TestUtils.JOB_CONTEXT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.lib.commands.sample.SampleCommandDto.Config;
import io.fleak.zephflow.lib.commands.sample.SampleCommandDto.Rule;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SampleConfigTest {

  private static Map<String, Object> json(String s) {
    return JsonUtils.fromJsonString(s, new TypeReference<>() {});
  }

  private static Config parseAndValidate(String configJson) {
    Config config = (Config) new SampleConfigParser().parseConfig(json(configJson));
    new SampleConfigValidator().validateConfig(config, "n1", JOB_CONTEXT);
    return config;
  }

  static Stream<Arguments> invalidConfigs() {
    return Stream.of(
        Arguments.of("{}", "'rules'"),
        Arguments.of("{\"rules\": []}", "'rules'"),
        Arguments.of("{\"rules\": null}", "'rules'"),
        Arguments.of("{\"rules\": \"$.a == 1\"}", "'rules'"),
        Arguments.of("{\"rules\": {\"sampleRate\": 2}}", "'rules'"),
        Arguments.of("{\"rules\": [5]}", "'rules[0]'"),
        Arguments.of("{\"rules\": [{\"sampleRate\": -1}]}", "'rules[0].sampleRate'"),
        Arguments.of(
            "{\"rules\": [{\"sampleRate\": 99999999999999999999}]}", "'rules[0].sampleRate'"),
        Arguments.of("{\"rules\": [{\"condition\": \"$.a == 1\"}]}", "'rules[0].sampleRate'"),
        Arguments.of("{\"rules\": [{\"sampleRate\": null}]}", "'rules[0].sampleRate'"),
        Arguments.of("{\"rules\": [{\"sampleRate\": 0}]}", "'rules[0].sampleRate'"),
        Arguments.of("{\"rules\": [{\"sampleRate\": 2147483648}]}", "'rules[0].sampleRate'"),
        Arguments.of("{\"rules\": [{\"sampleRate\": 3.0}]}", "'rules[0].sampleRate'"),
        Arguments.of("{\"rules\": [{\"sampleRate\": \"3\"}]}", "'rules[0].sampleRate'"),
        Arguments.of(
            "{\"rules\": [{\"condition\": \"$.a ==\", \"sampleRate\": 2}]}",
            "'rules[0].condition'"),
        Arguments.of(
            "{\"rules\": [{\"condition\": \"nope(1)\", \"sampleRate\": 2}]}",
            "'rules[0].condition'"),
        Arguments.of(
            "{\"rules\": [{\"condition\": \"  \", \"sampleRate\": 2}]}", "'rules[0].condition'"),
        Arguments.of(
            "{\"rules\": [{\"condition\": 1, \"sampleRate\": 2}]}", "'rules[0].condition'"),
        Arguments.of(
            "{\"rules\": [{\"sampleRate\": 2}], \"sampleRateField\": \"\"}", "'sampleRateField'"),
        Arguments.of(
            "{\"rules\": [{\"sampleRate\": 2}], \"sampleRateField\": \"  \"}", "'sampleRateField'"),
        Arguments.of(
            "{\"rules\": [{\"sampleRate\": 2}], \"sampleRateField\": 5}", "'sampleRateField'"),
        Arguments.of(
            "{\"rules\": [{\"sampleRate\": 2}], \"sampleRateField\": null}", "'sampleRateField'"),
        Arguments.of(
            "{\"rules\": [{\"conditon\": \"$.a == 1\", \"sampleRate\": 2}]}",
            "unknown parameter 'rules[0].conditon'"),
        Arguments.of(
            "{\"rules\": [{\"sampleRate\": 2}], \"sampleRatefield\": \"rate\"}",
            "unknown parameter 'sampleRatefield'"),
        Arguments.of(
            "{\"rules\": [{\"Condition\": \"$.a == 1\", \"sampleRate\": 2}]}",
            "unknown parameter 'rules[0].Condition'"));
  }

  @Test
  void rejectsNonStringRuleKey() {
    Map<Object, Object> rule = new HashMap<>(Map.of("sampleRate", 2, 1, "x"));
    Map<String, Object> config = new HashMap<>(Map.of("rules", List.of(rule)));
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> new SampleConfigParser().parseConfig(config));
    assertTrue(e.getMessage().contains("unknown parameter 'rules[0].1'"), e.getMessage());
  }

  @ParameterizedTest
  @MethodSource("invalidConfigs")
  void rejectsInvalidConfigNamingTheParameter(String configJson, String parameter) {
    SampleCommand cmd = (SampleCommand) new SampleCommandFactory().createCommand("n1", JOB_CONTEXT);
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class, () -> cmd.parseAndValidateArg(json(configJson)));
    assertTrue(e.getMessage().contains(parameter), e.getMessage());
  }

  @ParameterizedTest
  @MethodSource("validConfigs")
  void acceptsValidConfig(String configJson, Config expected) {
    assertEquals(expected, parseAndValidate(configJson));
  }

  static Stream<Arguments> validConfigs() {
    return Stream.of(
        Arguments.of(
            "{\"rules\": [{\"condition\": \"$.a == 1\", \"sampleRate\": 1}]}",
            new Config(List.of(new Rule("$.a == 1", 1)), "__sampled__")),
        Arguments.of(
            "{\"rules\": [{\"sampleRate\": 2147483647}], \"sampleRateField\": \"rate\"}",
            new Config(List.of(new Rule(null, Integer.MAX_VALUE)), "rate")),
        Arguments.of(
            "{\"rules\": [{\"sampleRate\": 3}, {\"condition\": \"$.x == 1\", \"sampleRate\": 2}]}",
            new Config(List.of(new Rule(null, 3), new Rule("$.x == 1", 2)), "__sampled__")),
        Arguments.of(
            "{\"rules\": [{\"condition\": null, \"sampleRate\": 2}], \"sampleRateField\": \"a.b\"}",
            new Config(List.of(new Rule(null, 2)), "a.b")));
  }
}
