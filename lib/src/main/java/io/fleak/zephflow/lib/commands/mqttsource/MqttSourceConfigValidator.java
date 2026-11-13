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
package io.fleak.zephflow.lib.commands.mqttsource;

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.require;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.filedecode.SourceDecoding;
import org.apache.commons.lang3.StringUtils;

public class MqttSourceConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    MqttSourceDto.Config config = (MqttSourceDto.Config) commandConfig;

    require(
        StringUtils.isNotBlank(config.getBrokerUrl()),
        of("brokerUrl"),
        "brokerUrl is required and must not be blank.",
        "no brokerUrl is provided");
    if (config.isUseTls()) {
      String lowerCaseBrokerUrl = config.getBrokerUrl().toLowerCase();
      require(
          lowerCaseBrokerUrl.startsWith("ssl://") || lowerCaseBrokerUrl.startsWith("wss://"),
          of("brokerUrl"),
          "brokerUrl must start with ssl:// or wss:// when useTls is true.",
          "useTls is enabled but brokerUrl scheme is not ssl:// or wss://: %s",
          config.getBrokerUrl());
    }
    require(
        StringUtils.isNotBlank(config.getTopicFilter()),
        of("topicFilter"),
        "topicFilter is required and must not be blank.",
        "no topicFilter is provided");
    require(
        StringUtils.isNotBlank(config.getClientId()),
        of("clientId"),
        "clientId is required and must not be blank.",
        "no clientId is provided");
    // Compiles the decoding, so a bad one fails validation rather than the first message.
    SourceDecoding.validate(MqttSourceCommand.decodingConfig(config));

    require(
        config.getQos() >= 0 && config.getQos() <= 2,
        of("qos"),
        "qos must be 0, 1 or 2.",
        "qos must be 0, 1, or 2, got: %s",
        config.getQos());

    if (config.getMaxBatchSize() != null) {
      require(
          config.getMaxBatchSize() > 0,
          of("maxBatchSize"),
          "maxBatchSize must be greater than 0.",
          "maxBatchSize must be positive, got: %s",
          config.getMaxBatchSize());
    }
    if (config.getReceiveQueueCapacity() != null) {
      require(
          config.getReceiveQueueCapacity() > 0,
          of("receiveQueueCapacity"),
          "receiveQueueCapacity must be greater than 0.",
          "receiveQueueCapacity must be positive, got: %s",
          config.getReceiveQueueCapacity());
    }
    if (config.getReceiveTimeoutMs() != null) {
      require(
          config.getReceiveTimeoutMs() > 0,
          of("receiveTimeoutMs"),
          "receiveTimeoutMs must be greater than 0.",
          "receiveTimeoutMs must be positive, got: %s",
          config.getReceiveTimeoutMs());
    }
  }
}
