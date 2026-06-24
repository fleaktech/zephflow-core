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

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import org.apache.commons.lang3.StringUtils;

public class MqttSourceConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    MqttSourceDto.Config config = (MqttSourceDto.Config) commandConfig;

    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getBrokerUrl()), "no brokerUrl is provided");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getTopicFilter()), "no topicFilter is provided");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getClientId()), "no clientId is provided");
    Preconditions.checkNotNull(config.getEncodingType(), "no encoding type is provided");
    DeserializerFactory.validateEncodingType(config.getEncodingType());

    Preconditions.checkArgument(
        config.getQos() >= 0 && config.getQos() <= 2,
        "qos must be 0, 1, or 2, got: %s",
        config.getQos());

    if (config.getMaxBatchSize() != null) {
      Preconditions.checkArgument(
          config.getMaxBatchSize() > 0,
          "maxBatchSize must be positive, got: %s",
          config.getMaxBatchSize());
    }
    if (config.getReceiveQueueCapacity() != null) {
      Preconditions.checkArgument(
          config.getReceiveQueueCapacity() > 0,
          "receiveQueueCapacity must be positive, got: %s",
          config.getReceiveQueueCapacity());
    }
    if (config.getReceiveTimeoutMs() != null) {
      Preconditions.checkArgument(
          config.getReceiveTimeoutMs() > 0,
          "receiveTimeoutMs must be positive, got: %s",
          config.getReceiveTimeoutMs());
    }
  }
}
