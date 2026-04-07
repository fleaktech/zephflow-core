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

import jakarta.jms.ConnectionFactory;
import org.apache.activemq.ActiveMQConnectionFactory;

public class JmsConnectionFactoryProvider {

  public ConnectionFactory createConnectionFactory(ActiveMqSourceDto.Config config) {
    return switch (config.getBrokerType()) {
      case CLASSIC -> createClassicConnectionFactory(config);
      case ARTEMIS -> createArtemisConnectionFactory(config);
    };
  }

  private ConnectionFactory createClassicConnectionFactory(ActiveMqSourceDto.Config config) {
    return new ActiveMQConnectionFactory(config.getBrokerUrl());
  }

  // FQN required: Artemis shares the same class name as Classic
  private ConnectionFactory createArtemisConnectionFactory(ActiveMqSourceDto.Config config) {
    return new org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory(
        config.getBrokerUrl());
  }
}
