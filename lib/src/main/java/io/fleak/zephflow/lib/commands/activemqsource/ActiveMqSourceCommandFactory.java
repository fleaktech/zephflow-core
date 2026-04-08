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

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.JsonConfigParser;
import io.fleak.zephflow.lib.commands.source.SourceCommandFactory;

public class ActiveMqSourceCommandFactory extends SourceCommandFactory {

  @Override
  public ActiveMqSourceCommand createCommand(String nodeId, JobContext jobContext) {
    JsonConfigParser<ActiveMqSourceDto.Config> configParser =
        new JsonConfigParser<>(ActiveMqSourceDto.Config.class);
    ActiveMqSourceConfigValidator validator = new ActiveMqSourceConfigValidator();
    JmsConnectionFactoryProvider jmsProvider = new JmsConnectionFactoryProvider();
    return new ActiveMqSourceCommand(nodeId, jobContext, configParser, validator, jmsProvider);
  }
}
