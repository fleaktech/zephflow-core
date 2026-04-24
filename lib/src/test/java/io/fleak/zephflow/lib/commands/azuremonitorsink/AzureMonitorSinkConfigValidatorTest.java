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
package io.fleak.zephflow.lib.commands.azuremonitorsink;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.TestUtils;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AzureMonitorSinkConfigValidatorTest {

  static final JobContext TEST_JOB_CONTEXT =
      JobContext.builder()
          .metricTags(TestUtils.JOB_CONTEXT.getMetricTags())
          .otherProperties(
              new HashMap<>(
                  Map.of(
                      "sentinel_cred",
                      new HashMap<>(Map.of("username", "client-id", "password", "client-secret")))))
          .build();

  private final AzureMonitorSinkConfigValidator validator = new AzureMonitorSinkConfigValidator();

  private AzureMonitorSinkDto.Config validConfig() {
    return AzureMonitorSinkDto.Config.builder()
        .tenantId("tenant-id")
        .dceEndpoint("https://my-dce.eastus.ingest.monitor.azure.com")
        .dcrImmutableId("dcr-abc123")
        .streamName("Custom-ZephflowTest_CL")
        .credentialId("sentinel_cred")
        .build();
  }

  @Test
  void validateConfig_valid() {
    assertDoesNotThrow(() -> validator.validateConfig(validConfig(), "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_missingTenantId() {
    AzureMonitorSinkDto.Config c = validConfig();
    c.setTenantId("");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_missingDceEndpoint() {
    AzureMonitorSinkDto.Config c = validConfig();
    c.setDceEndpoint("");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_missingDcrImmutableId() {
    AzureMonitorSinkDto.Config c = validConfig();
    c.setDcrImmutableId("");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_dcrImmutableIdWrongPrefix() {
    AzureMonitorSinkDto.Config c = validConfig();
    c.setDcrImmutableId("abc-123456");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_missingStreamName() {
    AzureMonitorSinkDto.Config c = validConfig();
    c.setStreamName("");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_missingCredentialId() {
    AzureMonitorSinkDto.Config c = validConfig();
    c.setCredentialId("");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_batchSizeZero() {
    AzureMonitorSinkDto.Config c = validConfig();
    c.setBatchSize(0);
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_batchSizeOne_isValid() {
    AzureMonitorSinkDto.Config c = validConfig();
    c.setBatchSize(1);
    assertDoesNotThrow(() -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }
}
