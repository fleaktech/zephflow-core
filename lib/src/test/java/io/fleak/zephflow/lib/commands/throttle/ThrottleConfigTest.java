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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.throttle.ThrottleCommandDto.Config;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ThrottleConfigTest {

  private final ThrottleConfigParser parser = new ThrottleConfigParser();
  private final ThrottleConfigValidator validator = new ThrottleConfigValidator();

  private void validate(CommandConfig config) {
    validator.validateConfig(config, "n1", JobContext.builder().build());
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
