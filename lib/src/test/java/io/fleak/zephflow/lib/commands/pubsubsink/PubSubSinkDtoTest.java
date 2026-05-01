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
package io.fleak.zephflow.lib.commands.pubsubsink;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.Test;

class PubSubSinkDtoTest {

  @Test
  void testConfigBuilder() {
    PubSubSinkDto.Config config =
        PubSubSinkDto.Config.builder()
            .projectId("my-project")
            .topic("my-topic")
            .encodingType("JSON_OBJECT")
            .credentialId("my-cred")
            .orderingKeyExpression("$.tenantId")
            .batchSize(250)
            .build();

    assertEquals("my-project", config.getProjectId());
    assertEquals("my-topic", config.getTopic());
    assertEquals("JSON_OBJECT", config.getEncodingType());
    assertEquals("my-cred", config.getCredentialId());
    assertEquals("$.tenantId", config.getOrderingKeyExpression());
    assertEquals(250, config.getBatchSize());
  }

  @Test
  void testDefaultValues() {
    PubSubSinkDto.Config config =
        PubSubSinkDto.Config.builder().topic("t").encodingType("JSON_OBJECT").build();

    assertEquals(PubSubSinkDto.DEFAULT_BATCH_SIZE, config.getBatchSize());
    assertNull(config.getCredentialId());
    assertNull(config.getProjectId());
    assertNull(config.getOrderingKeyExpression());
  }

  @Test
  void testJsonParsing() {
    Map<String, Object> jsonMap =
        Map.of(
            "projectId", "p",
            "topic", "t",
            "encodingType", "JSON_OBJECT",
            "batchSize", 50,
            "orderingKeyExpression", "$.id");

    PubSubSinkDto.Config config = OBJECT_MAPPER.convertValue(jsonMap, PubSubSinkDto.Config.class);

    assertEquals("p", config.getProjectId());
    assertEquals("t", config.getTopic());
    assertEquals("JSON_OBJECT", config.getEncodingType());
    assertEquals(50, config.getBatchSize());
    assertEquals("$.id", config.getOrderingKeyExpression());
  }
}
