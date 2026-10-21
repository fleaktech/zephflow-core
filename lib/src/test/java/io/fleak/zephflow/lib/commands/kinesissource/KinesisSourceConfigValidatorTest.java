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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.serdes.CompressionType;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ParquetFormatConfig;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class KinesisSourceConfigValidatorTest {
  static final JobContext KINESIS_SOURCE_TEST_JOB_CONTEXT =
      JobContext.builder().metricTags(TestUtils.JOB_CONTEXT.getMetricTags()).build();

  private final KinesisSourceConfigValidator validator = new KinesisSourceConfigValidator();

  @ParameterizedTest
  @MethodSource("testData")
  void validateConfig(CommandConfig config, boolean error) {
    if (error) {
      var ex =
          assertThrows(
              Exception.class,
              () -> validator.validateConfig(config, "test", KINESIS_SOURCE_TEST_JOB_CONTEXT));
      assertTrue(
          ex instanceof IllegalArgumentException
              || ex instanceof IllegalStateException
              || ex instanceof NullPointerException,
          "Expected IllegalArgumentException or NullPointerException but got: " + ex.getClass());
    } else {
      assertDoesNotThrow(
          () -> validator.validateConfig(config, "test", KINESIS_SOURCE_TEST_JOB_CONTEXT));
    }
  }

  public static Stream<Arguments> testData() {
    return Stream.of(
        Arguments.of(new KinesisSourceDto.Config(), true),
        Arguments.of(
            JsonUtils.fromJsonString(
                JsonUtils.toJsonString(
                    Map.of(
                        "applicationName", "test",
                        "streamName", "stream",
                        "encodingType", "JSON_OBJECT")),
                KinesisSourceDto.Config.class),
            true),
        Arguments.of(
            JsonUtils.fromJsonString(
                JsonUtils.toJsonString(
                    Map.of(
                        "streamName", "stream",
                        "regionStr", "us-east-1",
                        "encodingType", "JSON_OBJECT")),
                KinesisSourceDto.Config.class),
            true),
        Arguments.of(
            JsonUtils.fromJsonString(
                JsonUtils.toJsonString(
                    Map.of(
                        "applicationName", "app",
                        "regionStr", "us-east-1",
                        "streamName", "stream")),
                KinesisSourceDto.Config.class),
            true),
        // initialPosition = AT_TIMESTAMP but no timestamp provided
        Arguments.of(
            JsonUtils.fromJsonString(
                JsonUtils.toJsonString(
                    Map.of(
                        "streamName", "stream",
                        "regionStr", "us-east-1",
                        "encodingType", "JSON_OBJECT",
                        "applicationName", "app",
                        "initialPosition", "AT_TIMESTAMP")),
                KinesisSourceDto.Config.class),
            true),
        // timestamp is provided but initialPosition is not AT_TIMESTAMP
        Arguments.of(
            JsonUtils.fromJsonString(
                JsonUtils.toJsonString(
                    Map.of(
                        "streamName", "stream",
                        "regionStr", "us-east-1",
                        "encodingType", "JSON_OBJECT",
                        "applicationName", "app",
                        "initialPosition", "LATEST",
                        "initialPositionTimestamp", "2024-01-01T00:00:00.000+0000")),
                KinesisSourceDto.Config.class),
            true),
        // timestamp is provided but initialPosition is missing
        Arguments.of(
            JsonUtils.fromJsonString(
                JsonUtils.toJsonString(
                    Map.of(
                        "streamName", "stream",
                        "regionStr", "us-east-1",
                        "encodingType", "JSON_OBJECT",
                        "applicationName", "app",
                        "initialPositionTimestamp", "2024-01-01T00:00:00.000+0000")),
                KinesisSourceDto.Config.class),
            true),
        Arguments.of(
            JsonUtils.fromJsonString(
                JsonUtils.toJsonString(
                    Map.of(
                        "streamName", "stream",
                        "regionStr", "us-east-1",
                        "encodingType", "JSON_OBJECT",
                        "applicationName", "app",
                        "initialPosition", "AT_TIMESTAMP",
                        "initialPositionTimestamp", "2024-01-01T00:00:00.000+0000")),
                KinesisSourceDto.Config.class),
            false),
        Arguments.of(
            JsonUtils.fromJsonString(
                JsonUtils.toJsonString(
                    Map.of(
                        "streamName", "stream",
                        "regionStr", "us-east-1",
                        "encodingType", "JSON_OBJECT",
                        "applicationName", "app",
                        "initialPosition", "LATEST")),
                KinesisSourceDto.Config.class),
            false),
        Arguments.of(
            KinesisSourceDto.Config.builder()
                .applicationName("test")
                .regionStr("us-east-1")
                .encodingType(EncodingType.JSON_OBJECT)
                .streamName("test")
                .build(),
            false),
        // unsupported encoding type PARQUET
        Arguments.of(
            KinesisSourceDto.Config.builder()
                .applicationName("test")
                .regionStr("us-east-1")
                .encodingType(EncodingType.PARQUET)
                .streamName("test")
                .build(),
            true));
  }

  @Test
  void neitherEncodingTypeNorDecodingIsRejected() {
    KinesisSourceDto.Config config = decodingBase().build();

    assertEquals("exactly one of encodingType and decoding must be set", decodingRejection(config));
  }

  @Test
  void decodingReplacesEncodingType() {
    KinesisSourceDto.Config config =
        decodingBase()
            .decoding(
                new DecodingConfig(
                    List.of(DecompressionCodec.ZSTD),
                    new JsonFormatConfig(JsonFormatConfig.Mode.LINES)))
            .build();

    assertDoesNotThrow(
        () -> validator.validateConfig(config, "test", KINESIS_SOURCE_TEST_JOB_CONTEXT));
  }

  @Test
  void encodingTypeAndDecodingTogetherAreRejected() {
    KinesisSourceDto.Config config =
        decodingBase()
            .encodingType(EncodingType.JSON_OBJECT)
            .decoding(new DecodingConfig(List.of(), new ParquetFormatConfig()))
            .build();

    assertEquals("exactly one of encodingType and decoding must be set", decodingRejection(config));
  }

  @Test
  void aDecodingTheCompilerRejectsFailsValidation() {
    KinesisSourceDto.Config config =
        decodingBase()
            .decoding(
                new DecodingConfig(List.of(DecompressionCodec.GZIP), new ParquetFormatConfig()))
            .build();

    assertEquals(
        "parquet cannot be combined with compression; its pages carry their own",
        decodingRejection(config));
  }

  private String decodingRejection(KinesisSourceDto.Config config) {
    return assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test", KINESIS_SOURCE_TEST_JOB_CONTEXT))
        .getMessage();
  }

  @Test
  void compressionTypesAreRejected() {
    KinesisSourceDto.Config config =
        decodingBase()
            .encodingType(EncodingType.JSON_OBJECT)
            .compressionTypes(List.of(CompressionType.GZIP))
            .build();

    assertEquals(
        "compressionTypes is no longer supported; set decoding with the compression instead",
        decodingRejection(config));
  }

  private static KinesisSourceDto.Config.ConfigBuilder decodingBase() {
    return KinesisSourceDto.Config.builder()
        .applicationName("test")
        .regionStr("us-east-1")
        .streamName("test");
  }
}
