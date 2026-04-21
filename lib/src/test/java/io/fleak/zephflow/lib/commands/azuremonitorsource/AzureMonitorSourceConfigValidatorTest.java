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
package io.fleak.zephflow.lib.commands.azuremonitorsource;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.TestUtils;
import org.junit.jupiter.api.Test;

class AzureMonitorSourceConfigValidatorTest {

  private final AzureMonitorSourceConfigValidator validator =
      new AzureMonitorSourceConfigValidator();

  @Test
  void validConfig_passes() {
    var config =
        AzureMonitorSourceDto.Config.builder()
            .workspaceId("ws-guid")
            .tenantId("tenant-guid")
            .kqlQuery("TableName_CL | limit 10")
            .credentialId("cred-id")
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "node", TestUtils.JOB_CONTEXT));
  }

  @Test
  void batchSizeNegative_fails() {
    var config =
        AzureMonitorSourceDto.Config.builder()
            .workspaceId("ws-guid")
            .tenantId("tenant-guid")
            .kqlQuery("TableName_CL | limit 10")
            .credentialId("cred-id")
            .batchSize(-1)
            .build();

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "node", TestUtils.JOB_CONTEXT));
    assertTrue(ex.getMessage().contains("batchSize"));
  }

  @Test
  void missingWorkspaceId_fails() {
    var config =
        AzureMonitorSourceDto.Config.builder()
            .tenantId("tenant-guid")
            .kqlQuery("TableName_CL | limit 10")
            .credentialId("cred-id")
            .build();

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "node", TestUtils.JOB_CONTEXT));
    assertTrue(ex.getMessage().contains("workspaceId"));
  }

  @Test
  void missingTenantId_fails() {
    var config =
        AzureMonitorSourceDto.Config.builder()
            .workspaceId("ws-guid")
            .kqlQuery("TableName_CL | limit 10")
            .credentialId("cred-id")
            .build();

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "node", TestUtils.JOB_CONTEXT));
    assertTrue(ex.getMessage().contains("tenantId"));
  }

  @Test
  void missingKqlQuery_fails() {
    var config =
        AzureMonitorSourceDto.Config.builder()
            .workspaceId("ws-guid")
            .tenantId("tenant-guid")
            .credentialId("cred-id")
            .build();

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "node", TestUtils.JOB_CONTEXT));
    assertTrue(ex.getMessage().contains("kqlQuery"));
  }

  @Test
  void missingCredentialId_fails() {
    var config =
        AzureMonitorSourceDto.Config.builder()
            .workspaceId("ws-guid")
            .tenantId("tenant-guid")
            .kqlQuery("TableName_CL | limit 10")
            .build();

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "node", TestUtils.JOB_CONTEXT));
    assertTrue(ex.getMessage().contains("credentialId"));
  }

  @Test
  void batchSizeZero_fails() {
    var config =
        AzureMonitorSourceDto.Config.builder()
            .workspaceId("ws-guid")
            .tenantId("tenant-guid")
            .kqlQuery("TableName_CL | limit 10")
            .credentialId("cred-id")
            .batchSize(0)
            .build();

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "node", TestUtils.JOB_CONTEXT));
    assertTrue(ex.getMessage().contains("batchSize"));
  }

  @Test
  void batchSizeOne_passes() {
    var config =
        AzureMonitorSourceDto.Config.builder()
            .workspaceId("ws-guid")
            .tenantId("tenant-guid")
            .kqlQuery("TableName_CL | limit 10")
            .credentialId("cred-id")
            .batchSize(1)
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "node", TestUtils.JOB_CONTEXT));
  }
}
