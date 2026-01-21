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
package io.fleak.zephflow.lib.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.fleak.zephflow.lib.commands.s3.S3SinkDto;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonConfigParserTest {

  @Test
  void parseConfig_withNullBatchSize_usesDefaultValue() {
    var parser = new JsonConfigParser<>(S3SinkDto.Config.class);

    Map<String, Object> config = new HashMap<>();
    config.put("regionStr", "us-west-2");
    config.put("bucketName", "test-bucket");
    config.put("keyName", "test-key");
    config.put("encodingType", "json");
    config.put("batchSize", null);

    var result = parser.parseConfig(config);

    assertEquals(10_000, result.getBatchSize());
  }

  @Test
  void parseConfig_withExplicitBatchSize_usesProvidedValue() {
    var parser = new JsonConfigParser<>(S3SinkDto.Config.class);

    Map<String, Object> config = new HashMap<>();
    config.put("regionStr", "us-west-2");
    config.put("bucketName", "test-bucket");
    config.put("keyName", "test-key");
    config.put("encodingType", "json");
    config.put("batchSize", 5000);

    var result = parser.parseConfig(config);

    assertEquals(5000, result.getBatchSize());
  }

  @Test
  void parseConfig_withOmittedBatchSize_usesDefaultValue() {
    var parser = new JsonConfigParser<>(S3SinkDto.Config.class);

    Map<String, Object> config = new HashMap<>();
    config.put("regionStr", "us-west-2");
    config.put("bucketName", "test-bucket");
    config.put("keyName", "test-key");
    config.put("encodingType", "json");

    var result = parser.parseConfig(config);

    assertEquals(10_000, result.getBatchSize());
  }
}
