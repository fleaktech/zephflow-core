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
package io.fleak.zephflow.lib.commands.jdbcsource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.fleak.zephflow.lib.TestUtils;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class JdbcSourceConfigValidatorTest {

  @ParameterizedTest
  @MethodSource("getTestConfigs")
  void testConfig(JdbcSourceDto.Config config, boolean error) {
    try {
      var validator = new JdbcSourceConfigValidator();
      validator.validateConfig(config, "test-node", TestUtils.JOB_CONTEXT);
      assertFalse(error, "An error was expected for config: " + config);
    } catch (Exception e) {
      assertTrue(
          error,
          "An error was not expected for config " + config + " exception: " + e.getMessage());
    }
  }

  public static Stream<Arguments> getTestConfigs() {
    return Stream.of(
        Arguments.of(
            JdbcSourceDto.Config.builder()
                .jdbcUrl("jdbc:h2:mem:test")
                .query("SELECT * FROM test")
                .build(),
            false),
        Arguments.of(
            JdbcSourceDto.Config.builder()
                .jdbcUrl("jdbc:h2:mem:test")
                .query("SELECT * FROM test WHERE id > :watermark")
                .watermarkColumn("id")
                .build(),
            false),
        Arguments.of(JdbcSourceDto.Config.builder().query("SELECT * FROM test").build(), true),
        Arguments.of(JdbcSourceDto.Config.builder().jdbcUrl("jdbc:h2:mem:test").build(), true),
        Arguments.of(
            JdbcSourceDto.Config.builder()
                .jdbcUrl("jdbc:h2:mem:test")
                .query("SELECT * FROM test WHERE id > 0")
                .watermarkColumn("id")
                .build(),
            true),
        Arguments.of(
            JdbcSourceDto.Config.builder()
                .jdbcUrl("jdbc:h2:mem:test")
                .query("SELECT * FROM test")
                .fetchSize(0)
                .build(),
            true),
        Arguments.of(
            JdbcSourceDto.Config.builder()
                .jdbcUrl("jdbc:h2:mem:test")
                .query("SELECT * FROM test")
                .pollIntervalMs(-1)
                .build(),
            true));
  }
}
