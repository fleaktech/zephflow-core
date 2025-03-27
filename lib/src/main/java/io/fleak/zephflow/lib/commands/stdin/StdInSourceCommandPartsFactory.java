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
package io.fleak.zephflow.lib.commands.stdin;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;

/** Created by bolei on 12/20/24 */
public class StdInSourceCommandPartsFactory extends SourceCommandPartsFactory<SerializedEvent> {
  protected StdInSourceCommandPartsFactory(MetricClientProvider metricClientProvider) {
    super(metricClientProvider);
  }

  @Override
  public Fetcher<SerializedEvent> createFetcher(CommandConfig commandConfig) {
    return new StdInSourceFetcher();
  }

  @Override
  public RawDataConverter<SerializedEvent> createRawDataConverter(CommandConfig commandConfig) {
    StdInSourceDto.Config config = (StdInSourceDto.Config) commandConfig;
    FleakDeserializer<?> deserializer =
        DeserializerFactory.createDeserializerFactory(config.getEncodingType())
            .createDeserializer();

    return new BytesRawDataConverter(deserializer);
  }

  @Override
  public RawDataEncoder<SerializedEvent> createRawDataEncoder(CommandConfig commandConfig) {
    return new BytesRawDataEncoder();
  }
}
