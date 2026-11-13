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

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.require;
import static io.fleak.zephflow.lib.utils.MiscUtils.enforceCredentials;
import static io.fleak.zephflow.lib.utils.MiscUtils.lookupUsernamePasswordCredential;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.filedecode.SourceDecoding;
import org.apache.commons.lang3.StringUtils;

public class SqsSourceConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    SqsSourceDto.Config config = (SqsSourceDto.Config) commandConfig;
    require(
        StringUtils.isNotBlank(config.getQueueUrl()),
        of("queueUrl"),
        "queueUrl is required and must not be blank.",
        "no queue URL is provided");
    require(
        StringUtils.isNotBlank(config.getRegionStr()),
        of("regionStr"),
        "regionStr is required and must not be blank.",
        "no region is provided");
    SourceDecoding.validate(SqsSourceCommand.decodingConfig(config));

    if (config.getMaxNumberOfMessages() != null) {
      require(
          config.getMaxNumberOfMessages() >= 1
              && config.getMaxNumberOfMessages() <= SqsSourceDto.MAX_MAX_NUMBER_OF_MESSAGES,
          of("maxNumberOfMessages"),
          "maxNumberOfMessages must be between 1 and "
              + SqsSourceDto.MAX_MAX_NUMBER_OF_MESSAGES
              + ".",
          "maxNumberOfMessages must be between 1 and %s",
          SqsSourceDto.MAX_MAX_NUMBER_OF_MESSAGES);
    }

    if (config.getWaitTimeSeconds() != null) {
      require(
          config.getWaitTimeSeconds() >= 0
              && config.getWaitTimeSeconds() <= SqsSourceDto.MAX_WAIT_TIME_SECONDS,
          of("waitTimeSeconds"),
          "waitTimeSeconds must be between 0 and " + SqsSourceDto.MAX_WAIT_TIME_SECONDS + ".",
          "waitTimeSeconds must be between 0 and %s",
          SqsSourceDto.MAX_WAIT_TIME_SECONDS);
    }

    if (config.getVisibilityTimeoutSeconds() != null) {
      require(
          config.getVisibilityTimeoutSeconds() >= 0,
          of("visibilityTimeoutSeconds"),
          "visibilityTimeoutSeconds must be greater than or equal to 0.",
          "visibilityTimeoutSeconds must be non-negative");
    }

    if (StringUtils.trimToNull(config.getCredentialId()) != null
        && enforceCredentials(jobContext)) {
      lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
    }
  }
}
