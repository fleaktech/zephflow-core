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

import static io.fleak.zephflow.lib.serdes.encode.EncodeTestSupport.format;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.serdes.encode.EncodeTestSupport;
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
            .encoding(EncodeTestSupport.format("{\"type\":\"json\",\"mode\":\"object\"}"))
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
            .encoding(EncodeTestSupport.format("{\"type\":\"json\",\"mode\":\"object\"}"))
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
            .encoding(EncodeTestSupport.format("{\"type\":\"json\",\"mode\":\"object\"}"))
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
    config.setEncoding(EncodeTestSupport.format("{\"type\":\"json\",\"mode\":\"object\"}"));
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
    config.setEncoding(EncodeTestSupport.format("{\"type\":\"json\",\"mode\":\"object\"}"));
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_invalidBatchSize() {
    SqsSinkConfigValidator validator = new SqsSinkConfigValidator();
    SqsSinkDto.Config config =
        SqsSinkDto.Config.builder()
            .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
            .regionStr("us-east-1")
            .encoding(EncodeTestSupport.format("{\"type\":\"json\",\"mode\":\"object\"}"))
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
            .encoding(EncodeTestSupport.format("{\"type\":\"json\",\"mode\":\"object\"}"))
            .batchSize(0)
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_encodingAlone() {
    SqsSinkDto.Config config =
        standardQueue().encoding(format("{\"type\":\"json\",\"mode\":\"object\"}")).build();
    assertDoesNotThrow(
        () -> new SqsSinkConfigValidator().validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_encodingJsonLines() {
    SqsSinkDto.Config config =
        standardQueue().encoding(format("{\"type\":\"json\",\"mode\":\"lines\"}")).build();
    assertEquals("sqssink cannot write json mode lines", refusal(config));
  }

  @Test
  void validateConfig_encodingCsv() {
    SqsSinkDto.Config config = standardQueue().encoding(format("{\"type\":\"csv\"}")).build();
    assertEquals("sqssink cannot write csv", refusal(config));
  }

  @Test
  void validateConfig_withoutEncoding() {
    assertDoesNotThrow(
        () ->
            new SqsSinkConfigValidator()
                .validateConfig(standardQueue().build(), "nodeId", TEST_JOB_CONTEXT));
  }

  private static SqsSinkDto.Config.ConfigBuilder standardQueue() {
    return SqsSinkDto.Config.builder()
        .queueUrl("https://sqs.us-east-1.amazonaws.com/123456789012/my-queue")
        .regionStr("us-east-1");
  }

  private static String refusal(SqsSinkDto.Config config) {
    return assertThrows(
            IllegalArgumentException.class,
            () -> new SqsSinkConfigValidator().validateConfig(config, "nodeId", TEST_JOB_CONTEXT))
        .getMessage();
  }
}
