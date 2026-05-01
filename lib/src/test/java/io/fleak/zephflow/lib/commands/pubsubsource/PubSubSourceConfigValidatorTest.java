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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.credentials.GcpCredential;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PubSubSourceConfigValidatorTest {

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
  void validateConfig_validConfig() {
    PubSubSourceConfigValidator validator = new PubSubSourceConfigValidator();
    PubSubSourceDto.Config config =
        PubSubSourceDto.Config.builder()
            .projectId("p")
            .subscription("my-sub")
            .encodingType(EncodingType.JSON_OBJECT)
            .credentialId("example-credential-id")
            .build();
    assertDoesNotThrow(() -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_blankSubscription() {
    PubSubSourceConfigValidator validator = new PubSubSourceConfigValidator();
    PubSubSourceDto.Config config = new PubSubSourceDto.Config();
    config.setSubscription("");
    config.setEncodingType(EncodingType.JSON_OBJECT);
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_invalidMaxMessagesAboveCap() {
    PubSubSourceConfigValidator validator = new PubSubSourceConfigValidator();
    PubSubSourceDto.Config config =
        PubSubSourceDto.Config.builder()
            .subscription("s")
            .encodingType(EncodingType.JSON_OBJECT)
            .maxMessages(1001)
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_zeroMaxMessages() {
    PubSubSourceConfigValidator validator = new PubSubSourceConfigValidator();
    PubSubSourceDto.Config config =
        PubSubSourceDto.Config.builder()
            .subscription("s")
            .encodingType(EncodingType.JSON_OBJECT)
            .maxMessages(0)
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_negativeAckDeadlineExtension() {
    PubSubSourceConfigValidator validator = new PubSubSourceConfigValidator();
    PubSubSourceDto.Config config =
        PubSubSourceDto.Config.builder()
            .subscription("s")
            .encodingType(EncodingType.JSON_OBJECT)
            .ackDeadlineExtensionSeconds(-1)
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_ackDeadlineExtensionAboveCap() {
    PubSubSourceConfigValidator validator = new PubSubSourceConfigValidator();
    PubSubSourceDto.Config config =
        PubSubSourceDto.Config.builder()
            .subscription("s")
            .encodingType(EncodingType.JSON_OBJECT)
            .ackDeadlineExtensionSeconds(601)
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_validBoundaryValues() {
    PubSubSourceConfigValidator validator = new PubSubSourceConfigValidator();
    PubSubSourceDto.Config config =
        PubSubSourceDto.Config.builder()
            .subscription("s")
            .encodingType(EncodingType.JSON_OBJECT)
            .maxMessages(1000)
            .ackDeadlineExtensionSeconds(0)
            .build();
    assertDoesNotThrow(() -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }
}
