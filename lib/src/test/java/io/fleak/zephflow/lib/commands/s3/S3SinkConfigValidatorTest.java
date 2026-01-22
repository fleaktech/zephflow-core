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

  private final S3SinkConfigValidator validator = new S3SinkConfigValidator();
  private final JsonConfigParser<S3SinkDto.Config> configParser =
      new JsonConfigParser<>(S3SinkDto.Config.class);

  @Test
  void validateConfig() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("regionStr", "us-east-1");
    configMap.put("bucketName", "example-bucket");
    configMap.put("keyName", "example-key");
    configMap.put("encodingType", "JSON_OBJECT");
    configMap.put("batching", false);
    configMap.put("credentialId", "credential_2");

    S3SinkDto.Config config = configParser.parseConfig(configMap);
    validator.validateConfig(config, "abc", JOB_CONTEXT);
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
  void validateParquetConfig_requiresBatching() {
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
    configMap.put("batching", false);
    configMap.put("avroSchema", avroSchema);
    configMap.put("credentialId", "credential_2");

    S3SinkDto.Config config = configParser.parseConfig(configMap);
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "abc", JOB_CONTEXT));
    assertTrue(exception.getMessage().contains("batching mode"));
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
}
