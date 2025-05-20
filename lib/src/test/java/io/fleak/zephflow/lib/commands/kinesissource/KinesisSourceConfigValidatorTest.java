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
package io.fleak.zephflow.lib.commands.kinesissource;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.TestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class KinesisSourceConfigValidatorTest {
  static final JobContext KINESIS_SOURCE_TEST_JOB_CONTEXT =
      JobContext.builder()
          .metricTags(TestUtils.JOB_CONTEXT.getMetricTags())
          .build();

  @ParameterizedTest
  @MethodSource("testData")
  void validateConfig(CommandConfig config, boolean error) {
    KinesisSourceConfigValidator validator = new KinesisSourceConfigValidator();

    if(error) {
      assertThrows(IllegalArgumentException.class, () -> validator.validateConfig(config, "test", KINESIS_SOURCE_TEST_JOB_CONTEXT));
    } else {
      assertDoesNotThrow(() -> validator.validateConfig(config, "test", KINESIS_SOURCE_TEST_JOB_CONTEXT));
    }
  }

  public static Stream<Arguments> testData() {
    return Stream.of(
            Arguments.of(KinesisSourceDto.Config.builder().applicationName("test").regionStr("us-east-1").encodingType("JSON_OBJECT").streamName("test").build(), false),
            Arguments.of(KinesisSourceDto.Config.builder().applicationName("test").regionStr("us-east-1").encodingType("BLA").streamName("test").build(), true)
            );
  }
}
