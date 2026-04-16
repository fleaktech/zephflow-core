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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SqsSinkConfigValidatorTest {

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

  @Test
  void validateConfig_validStandardQueue() {
    SqsSinkConfigValidator validator = new SqsSinkConfigValidator();
    SqsSinkDto.Config config =
        SqsSinkDto.Config.builder()
            .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
            .regionStr("us-east-1")
            .encodingType(EncodingType.JSON_OBJECT.name())
            .credentialId("example-credential-id")
            .build();
    assertDoesNotThrow(() -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_validFifoQueue() {
    SqsSinkConfigValidator validator = new SqsSinkConfigValidator();
    SqsSinkDto.Config config =
        SqsSinkDto.Config.builder()
            .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue.fifo")
            .regionStr("us-east-1")
            .encodingType(EncodingType.JSON_OBJECT.name())
            .messageGroupIdExpression("$.groupId")
            .deduplicationIdExpression("$.dedupId")
            .build();
    assertDoesNotThrow(() -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_fifoQueueMissingMessageGroupId() {
    SqsSinkConfigValidator validator = new SqsSinkConfigValidator();
    SqsSinkDto.Config config =
        SqsSinkDto.Config.builder()
            .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue.fifo")
            .regionStr("us-east-1")
            .encodingType(EncodingType.JSON_OBJECT.name())
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_missingQueueUrl() {
    SqsSinkConfigValidator validator = new SqsSinkConfigValidator();
    SqsSinkDto.Config config = new SqsSinkDto.Config();
    config.setQueueUrl("");
    config.setRegionStr("us-east-1");
    config.setEncodingType(EncodingType.JSON_OBJECT.name());
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_missingRegion() {
    SqsSinkConfigValidator validator = new SqsSinkConfigValidator();
    SqsSinkDto.Config config = new SqsSinkDto.Config();
    config.setQueueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue");
    config.setRegionStr("");
    config.setEncodingType(EncodingType.JSON_OBJECT.name());
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_unsupportedEncodingType() {
    SqsSinkConfigValidator validator = new SqsSinkConfigValidator();
    SqsSinkDto.Config config =
        SqsSinkDto.Config.builder()
            .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
            .regionStr("us-east-1")
            .encodingType(EncodingType.STRING_LINE.name())
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_allSupportedEncodingTypes() {
    SqsSinkConfigValidator validator = new SqsSinkConfigValidator();
    for (EncodingType type : SerializerFactory.SUPPORTED_ENCODING_TYPES) {
      SqsSinkDto.Config config =
          SqsSinkDto.Config.builder()
              .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
              .regionStr("us-east-1")
              .encodingType(type.name())
              .build();
      assertDoesNotThrow(() -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
    }
  }

  @Test
  void validateConfig_invalidBatchSize() {
    SqsSinkConfigValidator validator = new SqsSinkConfigValidator();
    SqsSinkDto.Config config =
        SqsSinkDto.Config.builder()
            .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
            .regionStr("us-east-1")
            .encodingType(EncodingType.JSON_OBJECT.name())
            .batchSize(11)
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_zeroBatchSize() {
    SqsSinkConfigValidator validator = new SqsSinkConfigValidator();
    SqsSinkDto.Config config =
        SqsSinkDto.Config.builder()
            .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
            .regionStr("us-east-1")
            .encodingType(EncodingType.JSON_OBJECT.name())
            .batchSize(0)
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }
}
