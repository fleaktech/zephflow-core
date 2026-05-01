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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.credentials.GcpCredential;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
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
            .encodingType(EncodingType.JSON_OBJECT.name())
            .credentialId("example-credential-id")
            .build();
    assertDoesNotThrow(() -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_blankTopic() {
    PubSubSinkConfigValidator validator = new PubSubSinkConfigValidator();
    PubSubSinkDto.Config config = new PubSubSinkDto.Config();
    config.setTopic("");
    config.setEncodingType(EncodingType.JSON_OBJECT.name());
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_blankEncoding() {
    PubSubSinkConfigValidator validator = new PubSubSinkConfigValidator();
    PubSubSinkDto.Config config = new PubSubSinkDto.Config();
    config.setTopic("t");
    config.setEncodingType("");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_unsupportedEncoding() {
    PubSubSinkConfigValidator validator = new PubSubSinkConfigValidator();
    PubSubSinkDto.Config config =
        PubSubSinkDto.Config.builder()
            .topic("t")
            .encodingType(EncodingType.STRING_LINE.name())
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_allSupportedEncodings() {
    PubSubSinkConfigValidator validator = new PubSubSinkConfigValidator();
    for (EncodingType type : SerializerFactory.SUPPORTED_ENCODING_TYPES) {
      PubSubSinkDto.Config config =
          PubSubSinkDto.Config.builder().topic("t").encodingType(type.name()).build();
      assertDoesNotThrow(() -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
    }
  }

  @Test
  void validateConfig_invalidBatchSizeAboveCap() {
    PubSubSinkConfigValidator validator = new PubSubSinkConfigValidator();
    PubSubSinkDto.Config config =
        PubSubSinkDto.Config.builder()
            .topic("t")
            .encodingType(EncodingType.JSON_OBJECT.name())
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
            .encodingType(EncodingType.JSON_OBJECT.name())
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
            .encodingType(EncodingType.JSON_OBJECT.name())
            .orderingKeyExpression("not a valid expression")
            .build();
    assertThrows(
        Exception.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }
}
