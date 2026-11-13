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
package io.fleak.zephflow.lib.commands.azureblobsink;

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.require;
import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.ConfigurationIssue;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.api.JobContext;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public class AzureBlobSinkConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    AzureBlobSinkDto.Config config = (AzureBlobSinkDto.Config) commandConfig;

    require(
        StringUtils.isNotBlank(config.getContainerName()),
        of("containerName"),
        "containerName is required and must not be blank.",
        "containerName is required");
    if (!(StringUtils.isNotBlank(config.getConnectionString())
        || StringUtils.isNotBlank(config.getCredentialId()))) {
      throw new ConfigurationValidationException(
          new IllegalArgumentException("Either connectionString or credentialId must be provided"),
          List.of("connectionString", "credentialId").stream()
              .map(
                  field ->
                      new ConfigurationIssue(
                          of(field), "Either connectionString or credentialId must be provided"))
              .toList());
    }

    if (config.getBatchSize() != null) {
      require(
          config.getBatchSize() >= 1,
          of("batchSize"),
          "batchSize must be at least 1.",
          "batchSize must be at least 1");
    }

    if (StringUtils.isNotBlank(config.getCredentialId()) && enforceCredentials(jobContext)) {
      lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
    }
  }
}
