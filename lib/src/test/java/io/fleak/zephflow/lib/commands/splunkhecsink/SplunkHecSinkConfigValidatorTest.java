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
package io.fleak.zephflow.lib.commands.splunkhecsink;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.TestUtils;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SplunkHecSinkConfigValidatorTest {

  static final JobContext TEST_JOB_CONTEXT =
      JobContext.builder()
          .metricTags(TestUtils.JOB_CONTEXT.getMetricTags())
          .otherProperties(
              new HashMap<>(Map.of("hec_token", new HashMap<>(Map.of("key", "abcd-1234")))))
          .build();

  private final SplunkHecSinkConfigValidator validator = new SplunkHecSinkConfigValidator();

  private SplunkHecSinkDto.Config validConfig() {
    return SplunkHecSinkDto.Config.builder()
        .hecUrl("https://splunk:8088/services/collector/event")
        .credentialId("hec_token")
        .index("security_logs")
        .sourcetype("paloalto:traffic")
        .build();
  }

  @Test
  void validateConfig_minimalValid() {
    SplunkHecSinkDto.Config c =
        SplunkHecSinkDto.Config.builder()
            .hecUrl("https://splunk:8088/services/collector/event")
            .credentialId("hec_token")
            .build();
    assertDoesNotThrow(() -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_fullyPopulatedValid() {
    assertDoesNotThrow(() -> validator.validateConfig(validConfig(), "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_blankHecUrl() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setHecUrl("");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_malformedHecUrl() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setHecUrl("not a url");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_nonHttpScheme() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setHecUrl("ftp://splunk:8088/services/collector/event");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_blankCredentialId() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setCredentialId("");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_batchSizeZero() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setBatchSize(0);
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_batchSizeOne_isValid() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setBatchSize(1);
    assertDoesNotThrow(() -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }
}
