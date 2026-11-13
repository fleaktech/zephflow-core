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

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.require;
import static io.fleak.zephflow.api.ConfigurationValidationException.requireNonNull;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.filedecode.SourceDecoding;
import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;

public class ActiveMqSourceConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    ActiveMqSourceDto.Config config = (ActiveMqSourceDto.Config) commandConfig;

    require(
        StringUtils.isNotBlank(config.getBrokerUrl()),
        of("brokerUrl"),
        "brokerUrl is required and must not be blank.",
        "no brokerUrl is provided");
    requireNonNull(
        config.getBrokerType(),
        of("brokerType"),
        "brokerType is required. Accepted values: "
            + Arrays.toString(ActiveMqSourceDto.BrokerType.values()),
        "no brokerType is provided");
    require(
        StringUtils.isNotBlank(config.getDestination()),
        of("destination"),
        "destination is required and must not be blank.",
        "no destination is provided");
    // Compiles the decoding, so a bad one fails validation rather than the first message.
    SourceDecoding.validate(ActiveMqSourceCommand.decodingConfig(config));

    if (config.getDestinationType() == ActiveMqSourceDto.DestinationType.TOPIC) {
      require(
          StringUtils.isNotBlank(config.getClientId()),
          of("clientId"),
          "clientId is required and must not be blank for durable topic subscriptions.",
          "clientId is required for durable topic subscriptions");
      require(
          StringUtils.isNotBlank(config.getSubscriptionName()),
          of("subscriptionName"),
          "subscriptionName is required and must not be blank for durable topic subscriptions.",
          "subscriptionName is required for durable topic subscriptions");
    }

    if (config.getCommitStrategy() == ActiveMqSourceDto.CommitStrategyType.BATCH) {
      if (config.getCommitBatchSize() != null) {
        require(
            config.getCommitBatchSize() > 0,
            of("commitBatchSize"),
            "commitBatchSize must be greater than 0 when commitStrategy is BATCH.",
            "commitBatchSize must be positive, got: %s",
            config.getCommitBatchSize());
      }
      if (config.getCommitIntervalMs() != null) {
        require(
            config.getCommitIntervalMs() > 0,
            of("commitIntervalMs"),
            "commitIntervalMs must be greater than 0 when commitStrategy is BATCH.",
            "commitIntervalMs must be positive, got: %s",
            config.getCommitIntervalMs());
      }
    }
  }
}
