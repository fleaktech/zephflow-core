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
package io.fleak.zephflow.lib.commands.kinesissource;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KinesisSourceCommandPartsFactory extends SourceCommandPartsFactory<SerializedEvent> {

  public KinesisSourceCommandPartsFactory(MetricClientProvider metricClientProvider) {
    super(metricClientProvider);
  }

  @Override
  public Fetcher<SerializedEvent> createFetcher(CommandConfig commandConfig) {
    KinesisSourceDto.Config config = (KinesisSourceDto.Config) commandConfig;

    KinesisSourceFetcher fetcher = new KinesisSourceFetcher(config);
    fetcher.start();
    return fetcher;
  }

  @Override
  public RawDataConverter<SerializedEvent> createRawDataConverter(CommandConfig commandConfig) {
    if (!(commandConfig instanceof KinesisSourceDto.Config config)) {
      throw new IllegalArgumentException("Expected KinesisSourceDto.Config");
    }
    return new BytesRawDataConverter(
        DeserializerFactory.createDeserializerFactory(config.getEncodingType())
            .createDeserializer());
  }

  @Override
  public RawDataEncoder<SerializedEvent> createRawDataEncoder(CommandConfig commandConfig) {
    return new BytesRawDataEncoder();
  }
}
