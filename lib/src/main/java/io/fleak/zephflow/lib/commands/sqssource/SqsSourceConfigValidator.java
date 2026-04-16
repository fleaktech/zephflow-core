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
package io.fleak.zephflow.lib.commands.sqssource;

import static io.fleak.zephflow.lib.utils.MiscUtils.enforceCredentials;
import static io.fleak.zephflow.lib.utils.MiscUtils.lookupUsernamePasswordCredential;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import org.apache.commons.lang3.StringUtils;

public class SqsSourceConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    SqsSourceDto.Config config = (SqsSourceDto.Config) commandConfig;
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getQueueUrl()), "no queue URL is provided");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getRegionStr()), "no region is provided");
    Preconditions.checkNotNull(config.getEncodingType(), "no encoding type is provided");
    DeserializerFactory.validateEncodingType(config.getEncodingType());

    if (config.getMaxNumberOfMessages() != null) {
      Preconditions.checkArgument(
          config.getMaxNumberOfMessages() >= 1
              && config.getMaxNumberOfMessages() <= SqsSourceDto.MAX_MAX_NUMBER_OF_MESSAGES,
          "maxNumberOfMessages must be between 1 and %s",
          SqsSourceDto.MAX_MAX_NUMBER_OF_MESSAGES);
    }

    if (config.getWaitTimeSeconds() != null) {
      Preconditions.checkArgument(
          config.getWaitTimeSeconds() >= 0
              && config.getWaitTimeSeconds() <= SqsSourceDto.MAX_WAIT_TIME_SECONDS,
          "waitTimeSeconds must be between 0 and %s",
          SqsSourceDto.MAX_WAIT_TIME_SECONDS);
    }

    if (config.getVisibilityTimeoutSeconds() != null) {
      Preconditions.checkArgument(
          config.getVisibilityTimeoutSeconds() >= 0,
          "visibilityTimeoutSeconds must be non-negative");
    }

    if (StringUtils.trimToNull(config.getCredentialId()) != null
        && enforceCredentials(jobContext)) {
      lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
    }
  }
}
