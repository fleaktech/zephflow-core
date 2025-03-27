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
package io.fleak.zephflow.lib.commands.kafkasink;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.serdes.EncodingType;
import org.junit.jupiter.api.Test;

class KafkaSinkConfigValidatorTest {

  private final KafkaSinkConfigValidator validator = new KafkaSinkConfigValidator();

  @Test
  void validateConfig_validConfig() {
    KafkaSinkDto.Config config =
        KafkaSinkDto.Config.builder()
            .broker("localhost:9092")
            .topic("test-topic")
            .encodingType(EncodingType.JSON_OBJECT.name())
            .partitionKeyFieldExpressionStr("$.user_id")
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", null));
  }

  @Test
  void validateConfig_missingBroker() {
    KafkaSinkDto.Config config =
        KafkaSinkDto.Config.builder()
            .broker("") // Empty string instead of null
            .topic("test-topic")
            .encodingType(EncodingType.JSON_OBJECT.name())
            .build();

    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", null));
    assertEquals("Broker must be provided", exception.getMessage());
  }

  @Test
  void validateConfig_missingTopic() {
    KafkaSinkDto.Config config =
        KafkaSinkDto.Config.builder()
            .broker("localhost:9092")
            .topic("") // Empty string instead of null
            .encodingType(EncodingType.JSON_OBJECT.name())
            .build();

    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", null));
    assertEquals("Topic must be provided", exception.getMessage());
  }

  @Test
  void validateConfig_invalidEncodingType() {
    KafkaSinkDto.Config config =
        KafkaSinkDto.Config.builder()
            .broker("localhost:9092")
            .topic("test-topic")
            .encodingType("INVALID_TYPE")
            .build();

    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", null));
    assertTrue(exception.getMessage().contains("Invalid encoding type"));
  }
}
