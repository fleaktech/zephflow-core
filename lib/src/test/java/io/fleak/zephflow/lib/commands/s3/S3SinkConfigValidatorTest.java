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
package io.fleak.zephflow.lib.commands.s3;

import static io.fleak.zephflow.lib.TestUtils.JOB_CONTEXT;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.commands.JsonConfigParser;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class S3SinkConfigValidatorTest {

  private static final String BATCHING_FALSE_REFUSED =
      "s3sink no longer supports batching false; it always writes one file per batch";

  private static final Map<String, Object> TEST_AVRO_SCHEMA =
      Map.of(
          "type", "record",
          "name", "TestRecord",
          "fields", List.of(Map.of("name", "id", "type", "int")));

  private static final Map<String, Object> JSON_LINES = Map.of("type", "json", "mode", "lines");

  private final S3SinkConfigValidator validator = new S3SinkConfigValidator();
  private final JsonConfigParser<S3SinkDto.Config> configParser =
      new JsonConfigParser<>(S3SinkDto.Config.class);

  @Test
  void validateEncodingConfig_batchingFalseIsRefused() {
    Map<String, Object> configMap = encodingConfig(Map.of("format", JSON_LINES));
    configMap.put("batching", false);

    assertRefused(configMap, BATCHING_FALSE_REFUSED);
  }

  @Test
  void validateEncodingConfig_nonPositiveBatchSizeIsRefused() {
    Map<String, Object> configMap = encodingConfig(Map.of("format", JSON_LINES));
    configMap.put("batchSize", -1);

    assertRefused(
        configMap,
        "Batch size must be positive when batching is enabled, but found: -1. If omitted, ensure"
            + " default is applied correctly.");
  }

  @Test
  void validateEncodingConfig_success() {
    Map<String, Object> configMap =
        encodingConfig(Map.of("compression", List.of("gzip"), "format", JSON_LINES));

    S3SinkDto.Config config = configParser.parseConfig(configMap);
    assertDoesNotThrow(() -> validator.validateConfig(config, "abc", JOB_CONTEXT));
  }

  @Test
  void validateEncodingConfig_jsonObjectIsRefused() {
    assertRefused(
        encodingConfig(Map.of("format", Map.of("type", "json", "mode", "object"))),
        "s3sink: json mode object holds exactly one record and s3sink writes many records per"
            + " unit; use mode array or lines");
  }

  @Test
  void validateEncodingConfig_textLineAndMultilineAreAccepted() {
    for (Map<String, Object> framing :
        List.of(
            Map.<String, Object>of("type", "line"),
            Map.<String, Object>of("type", "multiline", "startPattern", "^<Event"))) {
      S3SinkDto.Config config =
          configParser.parseConfig(
              encodingConfig(
                  Map.of(
                      "compression",
                      List.of("gzip"),
                      "format",
                      Map.of("type", "text", "framing", framing))));
      assertDoesNotThrow(() -> validator.validateConfig(config, "abc", JOB_CONTEXT));
    }
  }

  @Test
  void validateEncodingConfig_textWholeIsRefused() {
    assertRefused(
        encodingConfig(
            Map.of("format", Map.of("type", "text", "framing", Map.of("type", "whole")))),
        "s3sink: text framing whole holds one record and s3sink writes many records per unit;"
            + " use framing line or multiline");
  }

  @Test
  void validateEncodingConfig_parquetWithGzipIsRefused() {
    Map<String, Object> configMap =
        encodingConfig(
            Map.of(
                "compression",
                List.of("gzip"),
                "format",
                Map.of("type", "parquet", "schema", TEST_AVRO_SCHEMA)));

    assertRefused(
        configMap,
        "s3sink: parquet cannot be combined with compression; its pages carry their own");
  }

  @Test
  void validateConfig_withoutEncodingIsRefused() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("regionStr", "us-east-1");
    configMap.put("bucketName", "example-bucket");
    configMap.put("keyName", "example-key");
    configMap.put("credentialId", "credential_2");

    assertRefused(configMap, "s3sink: encoding is required");
  }

  @Test
  void validateParquetConfig_success() {
    Map<String, Object> configMap =
        encodingConfig(Map.of("format", Map.of("type", "parquet", "schema", TEST_AVRO_SCHEMA)));

    S3SinkDto.Config config = configParser.parseConfig(configMap);
    assertDoesNotThrow(() -> validator.validateConfig(config, "abc", JOB_CONTEXT));
  }

  @Test
  void validateParquetConfig_requiresASchema() {
    assertRefused(
        encodingConfig(Map.of("format", Map.of("type", "parquet"))),
        "s3sink: parquet option schema is required when writing");
  }

  @Test
  void testBatchingDefaultsToTrue() {
    Map<String, Object> configMap =
        Map.of(
            "regionStr", "us-east-1",
            "bucketName", "example-bucket",
            "keyName", "example-key",
            "encoding", Map.of("format", JSON_LINES),
            "credentialId", "credential_2");

    S3SinkDto.Config config = configParser.parseConfig(configMap);
    assertTrue(config.isBatching());
    assertEquals(10_000, config.getBatchSize());
  }

  @Test
  void validateEncodingConfig_storeAndForwardWithNonPositiveMaxBytesIsRefused() {
    Map<String, Object> configMap = encodingConfig(Map.of("format", JSON_LINES));
    configMap.put("storeAndForwardEnabled", true);
    configMap.put("localStoreMaxBytes", 0);

    assertRefused(
        configMap, "localStoreMaxBytes must be positive when storeAndForwardEnabled. Got: 0");
  }

  @Test
  void validateEncodingConfig_storeAndForwardWithNonPositiveRetryIntervalIsRefused() {
    Map<String, Object> configMap = encodingConfig(Map.of("format", JSON_LINES));
    configMap.put("storeAndForwardEnabled", true);
    configMap.put("forwardRetryIntervalMillis", 0);

    assertRefused(
        configMap,
        "forwardRetryIntervalMillis must be positive when storeAndForwardEnabled. Got: 0");
  }

  private static Map<String, Object> encodingConfig(Map<String, Object> encoding) {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("regionStr", "us-east-1");
    configMap.put("bucketName", "example-bucket");
    configMap.put("keyName", "example-key");
    configMap.put("credentialId", "credential_2");
    configMap.put("encoding", encoding);
    return configMap;
  }

  private void assertRefused(Map<String, Object> configMap, String message) {
    S3SinkDto.Config config = configParser.parseConfig(configMap);
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "abc", JOB_CONTEXT));
    assertEquals(message, exception.getMessage());
  }
}
