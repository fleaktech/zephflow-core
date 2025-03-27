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
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SinkCommandInitializerFactory;

public class KafkaSinkCommandInitializerFactory
    extends SinkCommandInitializerFactory<RecordFleakData> {
  private final KafkaProducerClientFactory kafkaProducerClientFactory;

  public KafkaSinkCommandInitializerFactory(KafkaProducerClientFactory kafkaProducerClientFactory) {
    this.kafkaProducerClientFactory = kafkaProducerClientFactory;
  }

  @Override
  protected CommandPartsFactory createCommandPartsFactory(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    return new KafkaSinkPartsFactory(
        metricClientProvider,
        jobContext,
        (KafkaSinkDto.Config) commandConfig,
        kafkaProducerClientFactory);
  }
}
