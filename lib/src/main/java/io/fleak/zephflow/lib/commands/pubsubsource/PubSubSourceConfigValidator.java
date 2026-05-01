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
package io.fleak.zephflow.lib.commands.pubsubsource;

import static io.fleak.zephflow.lib.utils.MiscUtils.enforceCredentials;
import static io.fleak.zephflow.lib.utils.MiscUtils.lookupGcpCredential;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import org.apache.commons.lang3.StringUtils;

public class PubSubSourceConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    PubSubSourceDto.Config config = (PubSubSourceDto.Config) commandConfig;
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getSubscription()), "no subscription is provided");
    Preconditions.checkNotNull(config.getEncodingType(), "no encoding type is provided");
    DeserializerFactory.validateEncodingType(config.getEncodingType());

    if (config.getMaxMessages() != null) {
      Preconditions.checkArgument(
          config.getMaxMessages() >= 1
              && config.getMaxMessages() <= PubSubSourceDto.MAX_MAX_MESSAGES,
          "maxMessages must be between 1 and %s",
          PubSubSourceDto.MAX_MAX_MESSAGES);
    }

    if (config.getAckDeadlineExtensionSeconds() != null) {
      Preconditions.checkArgument(
          config.getAckDeadlineExtensionSeconds() >= 0
              && config.getAckDeadlineExtensionSeconds()
                  <= PubSubSourceDto.MAX_ACK_DEADLINE_EXTENSION_SECONDS,
          "ackDeadlineExtensionSeconds must be between 0 and %s",
          PubSubSourceDto.MAX_ACK_DEADLINE_EXTENSION_SECONDS);
    }

    if (StringUtils.trimToNull(config.getCredentialId()) != null
        && enforceCredentials(jobContext)) {
      lookupGcpCredential(jobContext, config.getCredentialId());
    }
  }
}
