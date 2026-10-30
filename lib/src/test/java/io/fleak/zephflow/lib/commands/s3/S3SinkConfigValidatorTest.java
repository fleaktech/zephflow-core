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
  void validateConfig_batchingFalseIsRefused() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("regionStr", "us-east-1");
    configMap.put("bucketName", "example-bucket");
    configMap.put("keyName", "example-key");
    configMap.put("encodingType", "JSON_OBJECT_LINE");
    configMap.put("batching", false);
    configMap.put("credentialId", "credential_2");

    assertRefused(configMap, BATCHING_FALSE_REFUSED);
  }

  @Test
  void validateEncodingConfig_batchingFalseIsRefused() {
    Map<String, Object> configMap = encodingConfig(Map.of("format", JSON_LINES));
    configMap.put("batching", false);

    assertRefused(configMap, BATCHING_FALSE_REFUSED);
  }

  @Test
  void validateConfig_nonPositiveBatchSizeIsRefused() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("regionStr", "us-east-1");
    configMap.put("bucketName", "example-bucket");
    configMap.put("keyName", "example-key");
    configMap.put("encodingType", "JSON_OBJECT_LINE");
    configMap.put("batchSize", 0);
    configMap.put("credentialId", "credential_2");

    assertRefused(
        configMap,
        "Batch size must be positive when batching is enabled, but found: 0. If omitted, ensure"
            + " default is applied correctly.");
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
  void validateEncodingConfig_withLegacyFieldsIsRefused() {
    Map<String, Object> configMap = encodingConfig(Map.of("format", JSON_LINES));
    configMap.put("encodingType", "JSON_OBJECT_LINE");
    configMap.put("compressionType", "GZIP");

    assertRefused(
        configMap, "s3sink: encoding cannot be combined with encodingType, compressionType");
  }

  @Test
  void validateEncodingConfig_withCompressionTypeIsRefused() {
    Map<String, Object> configMap = encodingConfig(Map.of("format", Map.of("type", "csv")));
    configMap.put("compressionType", "GZIP");

    assertRefused(configMap, "s3sink: encoding cannot be combined with compressionType");
  }

  @Test
  void validateEncodingConfig_jsonObjectIsRefused() {
    assertRefused(
        encodingConfig(Map.of("format", Map.of("type", "json", "mode", "object"))),
        "s3sink: json mode object holds exactly one record and s3sink writes many records per"
            + " unit; use mode array or lines");
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
  void validateConfig_withoutEncodingOrEncodingTypeKeepsTodaysError() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("regionStr", "us-east-1");
    configMap.put("bucketName", "example-bucket");
    configMap.put("keyName", "example-key");
    configMap.put("credentialId", "credential_2");

    S3SinkDto.Config config = configParser.parseConfig(configMap);
    NullPointerException exception =
        assertThrows(
            NullPointerException.class, () -> validator.validateConfig(config, "abc", JOB_CONTEXT));
    assertEquals("enumStr is marked non-null but is null", exception.getMessage());
  }

  @Test
  void validateParquetConfig_success() {
    Map<String, Object> avroSchema =
        Map.of(
            "type", "record",
            "name", "TestRecord",
            "fields", List.of(Map.of("name", "id", "type", "int")));
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("regionStr", "us-east-1");
    configMap.put("bucketName", "example-bucket");
    configMap.put("keyName", "example-key");
    configMap.put("encodingType", "PARQUET");
    configMap.put("batching", true);
    configMap.put("avroSchema", avroSchema);
    configMap.put("credentialId", "credential_2");

    S3SinkDto.Config config = configParser.parseConfig(configMap);
    assertDoesNotThrow(() -> validator.validateConfig(config, "abc", JOB_CONTEXT));
  }

  @Test
  void validateParquetConfig_requiresAvroSchema() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("regionStr", "us-east-1");
    configMap.put("bucketName", "example-bucket");
    configMap.put("keyName", "example-key");
    configMap.put("encodingType", "PARQUET");
    configMap.put("batching", true);
    configMap.put("credentialId", "credential_2");

    S3SinkDto.Config config = configParser.parseConfig(configMap);
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "abc", JOB_CONTEXT));
    assertTrue(exception.getMessage().contains("avroSchema"));
  }

  @Test
  void testBatchingDefaultsToTrue() {
    Map<String, Object> configMap =
        Map.of(
            "regionStr", "us-east-1",
            "bucketName", "example-bucket",
            "keyName", "example-key",
            "encodingType", "JSON_OBJECT_LINE",
            "credentialId", "credential_2");

    S3SinkDto.Config config = configParser.parseConfig(configMap);
    assertTrue(config.isBatching());
    assertEquals(10_000, config.getBatchSize());
  }

  @Test
  void validateConfig_unsupportedEncodingType() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("regionStr", "us-east-1");
    configMap.put("bucketName", "example-bucket");
    configMap.put("keyName", "example-key");
    configMap.put("encodingType", "TEXT");
    configMap.put("credentialId", "credential_2");

    S3SinkDto.Config config = configParser.parseConfig(configMap);
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "abc", JOB_CONTEXT));
    assertTrue(exception.getMessage().contains("Unsupported encoding type for S3 sink"));
  }

  @Test
  void validateJsonObjectWithBatching_fails() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("regionStr", "us-east-1");
    configMap.put("bucketName", "example-bucket");
    configMap.put("keyName", "example-key");
    configMap.put("encodingType", "JSON_OBJECT");
    configMap.put("batching", true);
    configMap.put("credentialId", "credential_2");

    S3SinkDto.Config config = configParser.parseConfig(configMap);
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "abc", JOB_CONTEXT));
    assertTrue(exception.getMessage().contains("JSON_OBJECT encoding does not support batching"));
  }

  @Test
  void validateGzipCompressionWithTextEncoding_success() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("regionStr", "us-east-1");
    configMap.put("bucketName", "example-bucket");
    configMap.put("keyName", "example-key");
    configMap.put("encodingType", "JSON_OBJECT_LINE");
    configMap.put("compressionType", "GZIP");
    configMap.put("credentialId", "credential_2");

    S3SinkDto.Config config = configParser.parseConfig(configMap);
    assertDoesNotThrow(() -> validator.validateConfig(config, "abc", JOB_CONTEXT));
  }

  @Test
  void validateGzipCompressionWithParquet_fails() {
    Map<String, Object> avroSchema =
        Map.of(
            "type", "record",
            "name", "TestRecord",
            "fields", List.of(Map.of("name", "id", "type", "int")));
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("regionStr", "us-east-1");
    configMap.put("bucketName", "example-bucket");
    configMap.put("keyName", "example-key");
    configMap.put("encodingType", "PARQUET");
    configMap.put("avroSchema", avroSchema);
    configMap.put("compressionType", "GZIP");
    configMap.put("credentialId", "credential_2");

    S3SinkDto.Config config = configParser.parseConfig(configMap);
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "abc", JOB_CONTEXT));
    assertTrue(exception.getMessage().contains("compressionType"));
  }

  @Test
  void validateKvConfig_parsesKvOptions() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("regionStr", "us-east-1");
    configMap.put("bucketName", "example-bucket");
    configMap.put("keyName", "example-key");
    configMap.put("encodingType", "KV");
    configMap.put("credentialId", "credential_2");
    configMap.put("kvFields", List.of("src_ip", "action"));
    configMap.put("kvPairDelimiter", "|");
    configMap.put("kvCleanFields", true);

    S3SinkDto.Config config = configParser.parseConfig(configMap);
    validator.validateConfig(config, "abc", JOB_CONTEXT);
    assertEquals(List.of("src_ip", "action"), config.kvOptions().fields());
    assertEquals("|", config.kvOptions().pairDelimiter());
    assertTrue(config.kvOptions().cleanFields());

    configMap.put("kvFields", List.of("a", "a"));
    S3SinkDto.Config duplicate = configParser.parseConfig(configMap);
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(duplicate, "abc", JOB_CONTEXT));
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
