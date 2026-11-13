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

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.require;
import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import org.apache.commons.lang3.StringUtils;

public class AzureMonitorSourceConfigValidator implements ConfigValidator {

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    AzureMonitorSourceDto.Config config = (AzureMonitorSourceDto.Config) commandConfig;

    require(
        StringUtils.isNotBlank(config.getWorkspaceId()),
        of("workspaceId"),
        "workspaceId is required and must not be blank.",
        "workspaceId is required");
    require(
        StringUtils.isNotBlank(config.getTenantId()),
        of("tenantId"),
        "tenantId is required and must not be blank.",
        "tenantId is required");
    require(
        StringUtils.isNotBlank(config.getKqlQuery()),
        of("kqlQuery"),
        "kqlQuery is required and must not be blank.",
        "kqlQuery is required");
    require(
        StringUtils.isNotBlank(config.getCredentialId()),
        of("credentialId"),
        "credentialId is required and must not be blank.",
        "credentialId is required");
    require(
        config.getBatchSize() >= 1,
        of("batchSize"),
        "batchSize must be at least 1.",
        "batchSize must be at least 1");

    if (enforceCredentials(jobContext)) {
      lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
    }
  }
}
