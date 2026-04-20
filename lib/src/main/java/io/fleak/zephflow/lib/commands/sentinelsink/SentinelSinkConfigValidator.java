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
package io.fleak.zephflow.lib.commands.sentinelsink;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import org.apache.commons.lang3.StringUtils;

public class SentinelSinkConfigValidator implements ConfigValidator {

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    SentinelSinkDto.Config config = (SentinelSinkDto.Config) commandConfig;

    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getTenantId()), "tenantId is required");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getDceEndpoint()), "dceEndpoint is required");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getDcrImmutableId()), "dcrImmutableId is required");
    Preconditions.checkArgument(
        config.getDcrImmutableId().startsWith("dcr-"), "dcrImmutableId must start with 'dcr-'");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getStreamName()), "streamName is required");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getCredentialId()), "credentialId is required");

    if (config.getBatchSize() != null) {
      Preconditions.checkArgument(config.getBatchSize() >= 1, "batchSize must be at least 1");
    }

    if (enforceCredentials(jobContext)) {
      lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
    }
  }
}
