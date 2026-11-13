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
package io.fleak.zephflow.lib.commands.kafkasource;

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.require;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.filedecode.SourceDecoding;
import org.apache.commons.lang3.StringUtils;

/** Created by bolei on 9/24/24 */
public class KafkaSourceConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    KafkaSourceDto.Config config = (KafkaSourceDto.Config) commandConfig;

    require(
        StringUtils.isNotBlank(config.getBroker()),
        of("broker"),
        "Broker is required and must not be blank",
        "no broker is provided");
    require(
        StringUtils.isNotBlank(config.getTopic()),
        of("topic"),
        "Topic is required and must not be blank",
        "no topic is provided");
    require(
        StringUtils.isNotBlank(config.getGroupId()),
        of("groupId"),
        "Consumer group ID is required and must not be blank",
        "no consumer group Id is provided");
    // Compiles the decoding, so a bad one fails validation rather than the first message.
    SourceDecoding.validate(KafkaSourceCommand.decodingConfig(config));

    // Validate commit strategy configuration
    if (config.getCommitStrategy() != null
        && config.getCommitStrategy() == KafkaSourceDto.CommitStrategyType.BATCH) {
      if (config.getCommitBatchSize() != null) {
        require(
            config.getCommitBatchSize() > 0,
            of("commitBatchSize"),
            "commitBatchSize must be greater than zero when commitStrategy is BATCH",
            "commitBatchSize must be positive, got: %s",
            config.getCommitBatchSize());
      }
      if (config.getCommitIntervalMs() != null) {
        require(
            config.getCommitIntervalMs() > 0,
            of("commitIntervalMs"),
            "commitIntervalMs must be greater than zero when commitStrategy is BATCH",
            "commitIntervalMs must be positive, got: %s",
            config.getCommitIntervalMs());
      }
    }
  }
}
