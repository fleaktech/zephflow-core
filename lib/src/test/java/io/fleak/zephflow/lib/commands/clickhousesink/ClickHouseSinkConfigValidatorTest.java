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
package io.fleak.zephflow.lib.commands.clickhousesink;


import io.fleak.zephflow.lib.TestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClickHouseSinkConfigValidatorTest {

  @ParameterizedTest
  @MethodSource("getTestConfigs")
  public void testConfig(ClickHouseSinkDto.Config config, boolean error) {
    try {
      var validator = new ClickHouseConfigValidator();
      validator.validateConfig(config, "test-node", TestUtils.JOB_CONTEXT);
      assertFalse(error, "An error was expected for config: " + config);
    } catch (Exception e) {
      assertTrue(error, "An error was not expected for config " + config + " exception: " + e.getMessage());
    }
  }

  public static Stream<Arguments> getTestConfigs() {
    return Stream.of(
            Arguments.of(
                    ClickHouseSinkDto.Config.builder()
                            .database("test")
                            .table("test")
                            .endpoint("test")
                            .build(),
                    false
            ),
            Arguments.of(
                    ClickHouseSinkDto.Config.builder()
                            .database("test")
                            .table("test")
                            .endpoint("test")
                            .credentialId("test")
                            .build(),
                    true
            )
    );
  }

}

