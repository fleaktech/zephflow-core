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

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.require;
import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import org.apache.commons.lang3.StringUtils;

public class AzureMonitorSinkConfigValidator implements ConfigValidator {

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    AzureMonitorSinkDto.Config config = (AzureMonitorSinkDto.Config) commandConfig;

    require(
        StringUtils.isNotBlank(config.getTenantId()),
        of("tenantId"),
        "tenantId is required and must not be blank.",
        "tenantId is required");
    require(
        StringUtils.isNotBlank(config.getDceEndpoint()),
        of("dceEndpoint"),
        "dceEndpoint is required and must not be blank.",
        "dceEndpoint is required");
    require(
        StringUtils.isNotBlank(config.getDcrImmutableId()),
        of("dcrImmutableId"),
        "dcrImmutableId is required and must not be blank.",
        "dcrImmutableId is required");
    require(
        config.getDcrImmutableId().startsWith("dcr-"),
        of("dcrImmutableId"),
        "dcrImmutableId must start with 'dcr-'.",
        "dcrImmutableId must start with 'dcr-'");
    require(
        StringUtils.isNotBlank(config.getStreamName()),
        of("streamName"),
        "streamName is required and must not be blank.",
        "streamName is required");
    require(
        StringUtils.isNotBlank(config.getCredentialId()),
        of("credentialId"),
        "credentialId is required and must not be blank.",
        "credentialId is required");

    if (config.getBatchSize() != null) {
      require(
          config.getBatchSize() >= 1,
          of("batchSize"),
          "batchSize must be at least 1.",
          "batchSize must be at least 1");
    }

    if (enforceCredentials(jobContext)) {
      lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
    }
  }
}
