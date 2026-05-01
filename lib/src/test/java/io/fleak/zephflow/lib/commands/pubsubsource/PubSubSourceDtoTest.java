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
package io.fleak.zephflow.lib.commands.pubsubsource;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.serdes.EncodingType;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PubSubSourceDtoTest {

  @Test
  void testConfigBuilder() {
    PubSubSourceDto.Config config =
        PubSubSourceDto.Config.builder()
            .projectId("my-project")
            .subscription("my-sub")
            .encodingType(EncodingType.JSON_OBJECT)
            .credentialId("my-cred")
            .maxMessages(50)
            .returnImmediately(true)
            .ackDeadlineExtensionSeconds(120)
            .build();

    assertEquals("my-project", config.getProjectId());
    assertEquals("my-sub", config.getSubscription());
    assertEquals(EncodingType.JSON_OBJECT, config.getEncodingType());
    assertEquals("my-cred", config.getCredentialId());
    assertEquals(50, config.getMaxMessages());
    assertTrue(config.getReturnImmediately());
    assertEquals(120, config.getAckDeadlineExtensionSeconds());
  }

  @Test
  void testDefaultValues() {
    PubSubSourceDto.Config config =
        PubSubSourceDto.Config.builder()
            .subscription("my-sub")
            .encodingType(EncodingType.JSON_OBJECT)
            .build();

    assertEquals(PubSubSourceDto.DEFAULT_MAX_MESSAGES, config.getMaxMessages());
    assertFalse(config.getReturnImmediately());
    assertNull(config.getCredentialId());
    assertNull(config.getProjectId());
    assertNull(config.getAckDeadlineExtensionSeconds());
  }

  @Test
  void testJsonParsing() {
    Map<String, Object> jsonMap =
        Map.of(
            "projectId",
            "p",
            "subscription",
            "s",
            "encodingType",
            "JSON_OBJECT",
            "maxMessages",
            200,
            "returnImmediately",
            false,
            "ackDeadlineExtensionSeconds",
            60);

    PubSubSourceDto.Config config =
        OBJECT_MAPPER.convertValue(jsonMap, PubSubSourceDto.Config.class);

    assertEquals("p", config.getProjectId());
    assertEquals("s", config.getSubscription());
    assertEquals(EncodingType.JSON_OBJECT, config.getEncodingType());
    assertEquals(200, config.getMaxMessages());
    assertFalse(config.getReturnImmediately());
    assertEquals(60, config.getAckDeadlineExtensionSeconds());
  }
}
