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
package io.fleak.zephflow.lib.commands.activemqsource;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import org.apache.commons.lang3.StringUtils;

public class ActiveMqSourceConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    ActiveMqSourceDto.Config config = (ActiveMqSourceDto.Config) commandConfig;

    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getBrokerUrl()), "no brokerUrl is provided");
    Preconditions.checkNotNull(config.getBrokerType(), "no brokerType is provided");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getDestination()), "no destination is provided");
    Preconditions.checkNotNull(config.getEncodingType(), "no encoding type is provided");
    DeserializerFactory.validateEncodingType(config.getEncodingType());

    if (config.getDestinationType() == ActiveMqSourceDto.DestinationType.TOPIC) {
      Preconditions.checkArgument(
          StringUtils.isNotBlank(config.getClientId()),
          "clientId is required for durable topic subscriptions");
      Preconditions.checkArgument(
          StringUtils.isNotBlank(config.getSubscriptionName()),
          "subscriptionName is required for durable topic subscriptions");
    }

    if (config.getCommitStrategy() == ActiveMqSourceDto.CommitStrategyType.BATCH) {
      if (config.getCommitBatchSize() != null) {
        Preconditions.checkArgument(
            config.getCommitBatchSize() > 0,
            "commitBatchSize must be positive, got: %s",
            config.getCommitBatchSize());
      }
      if (config.getCommitIntervalMs() != null) {
        Preconditions.checkArgument(
            config.getCommitIntervalMs() > 0,
            "commitIntervalMs must be positive, got: %s",
            config.getCommitIntervalMs());
      }
    }
  }
}
