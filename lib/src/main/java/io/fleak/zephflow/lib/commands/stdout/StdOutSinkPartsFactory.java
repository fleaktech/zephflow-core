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
package io.fleak.zephflow.lib.commands.stdout;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.PassThroughMessagePreProcessor;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkCommandPartsFactory;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;

/** Created by bolei on 12/20/24 */
public class StdOutSinkPartsFactory extends SinkCommandPartsFactory<RecordFleakData> {

  private final StdOutDto.Config config;

  protected StdOutSinkPartsFactory(
      MetricClientProvider metricClientProvider, JobContext jobContext, StdOutDto.Config config) {
    super(metricClientProvider, jobContext);
    this.config = config;
  }

  @Override
  public SimpleSinkCommand.SinkMessagePreProcessor<RecordFleakData> createMessagePreProcessor() {
    return new PassThroughMessagePreProcessor();
  }

  @Override
  public SimpleSinkCommand.Flusher<RecordFleakData> createFlusher() {
    SerializerFactory<?> serializerFactory =
        SerializerFactory.createSerializerFactory(config.getEncodingType());
    FleakSerializer<?> serializer = serializerFactory.createSerializer();
    return new StdOutSinkFlusher(serializer);
  }
}
