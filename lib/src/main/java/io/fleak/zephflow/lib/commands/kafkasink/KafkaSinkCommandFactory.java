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
package io.fleak.zephflow.lib.commands.kafkasink;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.lib.commands.JsonConfigParser;

public class KafkaSinkCommandFactory extends CommandFactory {
  private final KafkaProducerClientFactory kafkaProducerClientFactory;

  public KafkaSinkCommandFactory() {
    this(new KafkaProducerClientFactory());
  }

  public KafkaSinkCommandFactory(KafkaProducerClientFactory kafkaProducerClientFactory) {
    this.kafkaProducerClientFactory = kafkaProducerClientFactory;
  }

  @Override
  public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
    JsonConfigParser<KafkaSinkDto.Config> configParser =
        new JsonConfigParser<>(KafkaSinkDto.Config.class);
    KafkaSinkConfigValidator validator = new KafkaSinkConfigValidator();
    KafkaSinkCommandInitializerFactory initializerFactory =
        new KafkaSinkCommandInitializerFactory(kafkaProducerClientFactory);
    return new KafkaSinkCommand(nodeId, jobContext, configParser, validator, initializerFactory);
  }

  @Override
  public CommandType commandType() {
    return CommandType.SINK;
  }
}
