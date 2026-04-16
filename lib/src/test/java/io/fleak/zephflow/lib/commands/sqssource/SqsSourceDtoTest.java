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
package io.fleak.zephflow.lib.commands.sqssource;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.serdes.EncodingType;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SqsSourceDtoTest {

  @Test
  void testConfigBuilder() {
    SqsSourceDto.Config config =
        SqsSourceDto.Config.builder()
            .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
            .regionStr("us-east-1")
            .encodingType(EncodingType.JSON_OBJECT)
            .credentialId("my-cred")
            .maxNumberOfMessages(5)
            .waitTimeSeconds(10)
            .visibilityTimeoutSeconds(60)
            .build();

    assertEquals("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue", config.getQueueUrl());
    assertEquals("us-east-1", config.getRegionStr());
    assertEquals(EncodingType.JSON_OBJECT, config.getEncodingType());
    assertEquals("my-cred", config.getCredentialId());
    assertEquals(5, config.getMaxNumberOfMessages());
    assertEquals(10, config.getWaitTimeSeconds());
    assertEquals(60, config.getVisibilityTimeoutSeconds());
  }

  @Test
  void testDefaultValues() {
    SqsSourceDto.Config config =
        SqsSourceDto.Config.builder()
            .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
            .regionStr("us-east-1")
            .encodingType(EncodingType.JSON_OBJECT)
            .build();

    assertEquals(SqsSourceDto.DEFAULT_MAX_NUMBER_OF_MESSAGES, config.getMaxNumberOfMessages());
    assertEquals(SqsSourceDto.DEFAULT_WAIT_TIME_SECONDS, config.getWaitTimeSeconds());
    assertEquals(
        SqsSourceDto.DEFAULT_VISIBILITY_TIMEOUT_SECONDS, config.getVisibilityTimeoutSeconds());
    assertNull(config.getCredentialId());
  }

  @Test
  void testJsonParsing() {
    Map<String, Object> jsonMap =
        Map.of(
            "queueUrl", "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
            "regionStr", "us-east-1",
            "encodingType", "JSON_OBJECT",
            "maxNumberOfMessages", 7);

    SqsSourceDto.Config config = OBJECT_MAPPER.convertValue(jsonMap, SqsSourceDto.Config.class);

    assertEquals("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue", config.getQueueUrl());
    assertEquals("us-east-1", config.getRegionStr());
    assertEquals(EncodingType.JSON_OBJECT, config.getEncodingType());
    assertEquals(7, config.getMaxNumberOfMessages());
  }
}
