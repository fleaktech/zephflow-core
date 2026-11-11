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

import static io.fleak.zephflow.lib.serdes.encode.EncodeTestSupport.format;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.credentials.GcpCredential;
import io.fleak.zephflow.lib.serdes.encode.EncodeTestSupport;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PubSubSinkConfigValidatorTest {

  static final JobContext TEST_JOB_CONTEXT =
      JobContext.builder()
          .metricTags(TestUtils.JOB_CONTEXT.getMetricTags())
          .otherProperties(
              new HashMap<>(
                  Map.of(
                      "example-credential-id",
                      new HashMap<>(
                          Map.of(
                              "authType",
                              GcpCredential.AuthType.APPLICATION_DEFAULT.name(),
                              "projectId",
                              "test-project")))))
          .build();

  @Test
  void validateConfig_valid() {
    PubSubSinkConfigValidator validator = new PubSubSinkConfigValidator();
    PubSubSinkDto.Config config =
        PubSubSinkDto.Config.builder()
            .projectId("p")
            .topic("t")
            .encoding(EncodeTestSupport.format("{\"type\":\"json\",\"mode\":\"object\"}"))
            .credentialId("example-credential-id")
            .build();
    assertDoesNotThrow(() -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_blankTopic() {
    PubSubSinkConfigValidator validator = new PubSubSinkConfigValidator();
    PubSubSinkDto.Config config = new PubSubSinkDto.Config();
    config.setTopic("");
    config.setEncoding(EncodeTestSupport.format("{\"type\":\"json\",\"mode\":\"object\"}"));
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_invalidBatchSizeAboveCap() {
    PubSubSinkConfigValidator validator = new PubSubSinkConfigValidator();
    PubSubSinkDto.Config config =
        PubSubSinkDto.Config.builder()
            .topic("t")
            .encoding(EncodeTestSupport.format("{\"type\":\"json\",\"mode\":\"object\"}"))
            .batchSize(1001)
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_zeroBatchSize() {
    PubSubSinkConfigValidator validator = new PubSubSinkConfigValidator();
    PubSubSinkDto.Config config =
        PubSubSinkDto.Config.builder()
            .topic("t")
            .encoding(EncodeTestSupport.format("{\"type\":\"json\",\"mode\":\"object\"}"))
            .batchSize(0)
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_invalidOrderingKeyExpression() {
    PubSubSinkConfigValidator validator = new PubSubSinkConfigValidator();
    PubSubSinkDto.Config config =
        PubSubSinkDto.Config.builder()
            .topic("t")
            .encoding(EncodeTestSupport.format("{\"type\":\"json\",\"mode\":\"object\"}"))
            .orderingKeyExpression("not a valid expression")
            .build();
    assertThrows(
        Exception.class, () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_encodingAlone() {
    PubSubSinkDto.Config config =
        PubSubSinkDto.Config.builder()
            .topic("t")
            .encoding(format("{\"type\":\"json\",\"mode\":\"object\"}"))
            .build();
    assertDoesNotThrow(
        () -> new PubSubSinkConfigValidator().validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_encodingJsonLines() {
    PubSubSinkDto.Config config =
        PubSubSinkDto.Config.builder()
            .topic("t")
            .encoding(format("{\"type\":\"json\",\"mode\":\"lines\"}"))
            .build();
    assertEquals("pubsubsink cannot write json mode lines", refusal(config));
  }

  @Test
  void validateConfig_encodingCsv() {
    PubSubSinkDto.Config config =
        PubSubSinkDto.Config.builder().topic("t").encoding(format("{\"type\":\"csv\"}")).build();
    assertEquals("pubsubsink cannot write csv", refusal(config));
  }

  @Test
  void validateConfig_withoutEncoding() {
    assertDoesNotThrow(
        () ->
            new PubSubSinkConfigValidator()
                .validateConfig(
                    PubSubSinkDto.Config.builder().topic("t").build(), "nodeId", TEST_JOB_CONTEXT));
  }

  private static String refusal(PubSubSinkDto.Config config) {
    return assertThrows(
            IllegalArgumentException.class,
            () ->
                new PubSubSinkConfigValidator().validateConfig(config, "nodeId", TEST_JOB_CONTEXT))
        .getMessage();
  }
}
