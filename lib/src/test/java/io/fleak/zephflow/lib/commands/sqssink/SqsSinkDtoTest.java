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
package io.fleak.zephflow.lib.commands.sqssink;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.Test;

class SqsSinkDtoTest {

  @Test
  void testConfigBuilder() {
    SqsSinkDto.Config config =
        SqsSinkDto.Config.builder()
            .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue.fifo")
            .regionStr("us-east-1")
            .encodingType("JSON_OBJECT")
            .credentialId("my-cred")
            .messageGroupIdExpression("$.groupId")
            .deduplicationIdExpression("$.dedupId")
            .batchSize(5)
            .build();

    assertEquals(
        "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue.fifo", config.getQueueUrl());
    assertEquals("us-east-1", config.getRegionStr());
    assertEquals("JSON_OBJECT", config.getEncodingType());
    assertEquals("my-cred", config.getCredentialId());
    assertEquals("$.groupId", config.getMessageGroupIdExpression());
    assertEquals("$.dedupId", config.getDeduplicationIdExpression());
    assertEquals(5, config.getBatchSize());
  }

  @Test
  void testDefaultValues() {
    SqsSinkDto.Config config =
        SqsSinkDto.Config.builder()
            .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
            .regionStr("us-east-1")
            .encodingType("JSON_OBJECT")
            .build();

    assertEquals(SqsSinkDto.DEFAULT_BATCH_SIZE, config.getBatchSize());
    assertNull(config.getCredentialId());
    assertNull(config.getMessageGroupIdExpression());
    assertNull(config.getDeduplicationIdExpression());
  }

  @Test
  void testJsonParsing() {
    Map<String, Object> jsonMap =
        Map.of(
            "queueUrl", "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
            "regionStr", "us-east-1",
            "encodingType", "JSON_OBJECT",
            "batchSize", 7,
            "messageGroupIdExpression", "$.group");

    SqsSinkDto.Config config = OBJECT_MAPPER.convertValue(jsonMap, SqsSinkDto.Config.class);

    assertEquals("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue", config.getQueueUrl());
    assertEquals("us-east-1", config.getRegionStr());
    assertEquals("JSON_OBJECT", config.getEncodingType());
    assertEquals(7, config.getBatchSize());
    assertEquals("$.group", config.getMessageGroupIdExpression());
  }
}
