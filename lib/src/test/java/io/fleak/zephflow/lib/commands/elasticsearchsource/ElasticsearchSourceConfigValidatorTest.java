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
package io.fleak.zephflow.lib.commands.elasticsearchsource;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullSource;

class ElasticsearchSourceConfigValidatorTest {

  private final ElasticsearchSourceConfigValidator validator =
      new ElasticsearchSourceConfigValidator();

  @ParameterizedTest
  @NullSource
  @EnumSource(EncodingType.class)
  void anyOrNoEncodingTypePasses(EncodingType encodingType) {
    assertDoesNotThrow(
        () ->
            validator.validateConfig(
                config().encodingType(encodingType).build(), "node", TestUtils.JOB_CONTEXT));
  }

  static Stream<Arguments> invalidConfigs() {
    return Stream.of(
        Arguments.of(config().host(" ").build(), "host is required"),
        Arguments.of(config().index("").build(), "index is required"),
        Arguments.of(config().batchSize(0).build(), "batchSize must be at least 1"));
  }

  @ParameterizedTest
  @MethodSource("invalidConfigs")
  void invalidConfigsFail(ElasticsearchSourceDto.Config config, String message) {
    assertEquals(
        message,
        assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateConfig(config, "node", TestUtils.JOB_CONTEXT))
            .getMessage());
  }

  private static ElasticsearchSourceDto.Config.ConfigBuilder config() {
    return ElasticsearchSourceDto.Config.builder().host("http://localhost:9200").index("idx");
  }
}
