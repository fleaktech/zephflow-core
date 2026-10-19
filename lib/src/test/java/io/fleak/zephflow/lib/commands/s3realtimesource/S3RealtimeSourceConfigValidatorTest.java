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
package io.fleak.zephflow.lib.commands.s3realtimesource;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.serdes.CompressionType;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class S3RealtimeSourceConfigValidatorTest {

  static final JobContext TEST_JOB_CONTEXT =
      JobContext.builder()
          .metricTags(TestUtils.JOB_CONTEXT.getMetricTags())
          .otherProperties(
              new HashMap<>(
                  Map.of(
                      "example-credential-id",
                      new HashMap<>(
                          Map.of("username", "test-access-key", "password", "test-secret-key")))))
          .build();

  private final S3RealtimeSourceConfigValidator validator = new S3RealtimeSourceConfigValidator();

  private static S3RealtimeSourceDto.Config.ConfigBuilder validConfig() {
    return S3RealtimeSourceDto.Config.builder()
        .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
        .regionStr("us-east-1")
        .encodingType(EncodingType.JSON_OBJECT_LINE)
        .credentialId("example-credential-id");
  }

  @Test
  void validateConfig_validConfig() {
    assertDoesNotThrow(
        () -> validator.validateConfig(validConfig().build(), "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_blankQueueUrl() {
    S3RealtimeSourceDto.Config config = validConfig().queueUrl("").build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_blankRegion() {
    S3RealtimeSourceDto.Config config = validConfig().regionStr("").build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_invalidMaxNumberOfMessages() {
    S3RealtimeSourceDto.Config config = validConfig().maxNumberOfMessages(11).build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_invalidWaitTimeSeconds() {
    S3RealtimeSourceDto.Config config = validConfig().waitTimeSeconds(21).build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_negativeVisibilityTimeout() {
    S3RealtimeSourceDto.Config config = validConfig().visibilityTimeoutSeconds(-1).build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_nonPositiveMaxObjectSize() {
    S3RealtimeSourceDto.Config config = validConfig().maxObjectSizeBytes(0L).build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_invalidMaxRetries() {
    S3RealtimeSourceDto.Config config = validConfig().maxRetries(0).build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_validBoundaryValues() {
    S3RealtimeSourceDto.Config config =
        validConfig()
            .maxNumberOfMessages(10)
            .waitTimeSeconds(20)
            .visibilityTimeoutSeconds(0)
            .maxObjectSizeBytes(1L)
            .maxRetries(1)
            .build();
    assertDoesNotThrow(() -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_decodingInsteadOfEncodingType() {
    S3RealtimeSourceDto.Config config =
        validConfig()
            .encodingType(null)
            .decoding(decoding("{\"format\":{\"type\":\"parquet\"}}"))
            .build();
    assertDoesNotThrow(() -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_rejectsNeitherAndBothEncodingTypeAndDecoding() {
    String message = "exactly one of encodingType and decoding must be set";
    S3RealtimeSourceDto.Config neither = validConfig().encodingType(null).build();
    S3RealtimeSourceDto.Config both =
        validConfig().decoding(decoding("{\"format\":{\"type\":\"parquet\"}}")).build();
    assertEquals(
        List.of(message, message),
        List.of(
            assertThrows(
                    IllegalArgumentException.class,
                    () -> validator.validateConfig(neither, "nodeId", TEST_JOB_CONTEXT))
                .getMessage(),
            assertThrows(
                    IllegalArgumentException.class,
                    () -> validator.validateConfig(both, "nodeId", TEST_JOB_CONTEXT))
                .getMessage()));
  }

  @Test
  void validateConfig_rejectsCompressionTypeWithDecoding() {
    S3RealtimeSourceDto.Config config =
        validConfig()
            .encodingType(null)
            .compressionType(CompressionType.GZIP)
            .decoding(decoding("{\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}"))
            .build();
    assertEquals(
        "compressionType is only allowed with encodingType; decoding carries its own compression",
        assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT))
            .getMessage());
  }

  @Test
  void validateConfig_compilesTheDecoding() {
    S3RealtimeSourceDto.Config badRegex =
        validConfig()
            .encodingType(null)
            .decoding(
                decoding(
                    "{\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"multiline\",\"startPattern\":\"(\"}}}"))
            .build();
    S3RealtimeSourceDto.Config compressedParquet =
        validConfig()
            .encodingType(null)
            .decoding(decoding("{\"compression\":\"gzip\",\"format\":{\"type\":\"parquet\"}}"))
            .build();
    assertTrue(
        assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateConfig(badRegex, "nodeId", TEST_JOB_CONTEXT))
            .getMessage()
            .startsWith("invalid startPattern: "));
    assertEquals(
        "parquet cannot be combined with compression; its pages carry their own",
        assertThrows(
                IllegalArgumentException.class,
                () -> validator.validateConfig(compressedParquet, "nodeId", TEST_JOB_CONTEXT))
            .getMessage());
  }

  @Test
  void validateConfig_rejectsParquetEncodingType() {
    S3RealtimeSourceDto.Config config = validConfig().encodingType(EncodingType.PARQUET).build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  private static DecodingConfig decoding(String json) {
    return JsonUtils.fromJsonString(json, DecodingConfig.class);
  }
}
