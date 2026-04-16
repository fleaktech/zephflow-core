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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SqsSourceConfigValidatorTest {

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
  void validateConfig_validConfig() {
    SqsSourceConfigValidator validator = new SqsSourceConfigValidator();
    SqsSourceDto.Config config =
        SqsSourceDto.Config.builder()
            .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
            .regionStr("us-east-1")
            .encodingType(EncodingType.JSON_OBJECT)
            .credentialId("example-credential-id")
            .build();
    assertDoesNotThrow(() -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_missingQueueUrl() {
    SqsSourceConfigValidator validator = new SqsSourceConfigValidator();
    assertThrows(
        NullPointerException.class,
        () -> {
          SqsSourceDto.Config config =
              SqsSourceDto.Config.builder()
                  .queueUrl(null)
                  .regionStr("us-east-1")
                  .encodingType(EncodingType.JSON_OBJECT)
                  .build();
        });
  }

  @Test
  void validateConfig_blankQueueUrl() {
    SqsSourceConfigValidator validator = new SqsSourceConfigValidator();
    SqsSourceDto.Config config = new SqsSourceDto.Config();
    config.setQueueUrl("");
    config.setRegionStr("us-east-1");
    config.setEncodingType(EncodingType.JSON_OBJECT);
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_missingRegion() {
    SqsSourceConfigValidator validator = new SqsSourceConfigValidator();
    SqsSourceDto.Config config = new SqsSourceDto.Config();
    config.setQueueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue");
    config.setRegionStr("");
    config.setEncodingType(EncodingType.JSON_OBJECT);
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_invalidMaxNumberOfMessages() {
    SqsSourceConfigValidator validator = new SqsSourceConfigValidator();
    SqsSourceDto.Config config =
        SqsSourceDto.Config.builder()
            .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
            .regionStr("us-east-1")
            .encodingType(EncodingType.JSON_OBJECT)
            .maxNumberOfMessages(11)
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_invalidWaitTimeSeconds() {
    SqsSourceConfigValidator validator = new SqsSourceConfigValidator();
    SqsSourceDto.Config config =
        SqsSourceDto.Config.builder()
            .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
            .regionStr("us-east-1")
            .encodingType(EncodingType.JSON_OBJECT)
            .waitTimeSeconds(21)
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_negativeVisibilityTimeout() {
    SqsSourceConfigValidator validator = new SqsSourceConfigValidator();
    SqsSourceDto.Config config =
        SqsSourceDto.Config.builder()
            .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
            .regionStr("us-east-1")
            .encodingType(EncodingType.JSON_OBJECT)
            .visibilityTimeoutSeconds(-1)
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_validBoundaryValues() {
    SqsSourceConfigValidator validator = new SqsSourceConfigValidator();
    SqsSourceDto.Config config =
        SqsSourceDto.Config.builder()
            .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
            .regionStr("us-east-1")
            .encodingType(EncodingType.JSON_OBJECT)
            .maxNumberOfMessages(10)
            .waitTimeSeconds(20)
            .visibilityTimeoutSeconds(0)
            .build();
    assertDoesNotThrow(() -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_zeroMaxNumberOfMessages() {
    SqsSourceConfigValidator validator = new SqsSourceConfigValidator();
    SqsSourceDto.Config config =
        SqsSourceDto.Config.builder()
            .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
            .regionStr("us-east-1")
            .encodingType(EncodingType.JSON_OBJECT)
            .maxNumberOfMessages(0)
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }
}
