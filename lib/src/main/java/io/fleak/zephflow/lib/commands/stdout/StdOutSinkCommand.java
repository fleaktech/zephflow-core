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

import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_STDOUT;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.PassThroughMessagePreProcessor;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;

/** Created by bolei on 12/20/24 */
public class StdOutSinkCommand extends SimpleSinkCommand<RecordFleakData> {
  protected StdOutSinkCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator) {
    super(nodeId, jobContext, configParser, configValidator);
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    SinkCounters counters =
        createSinkCounters(metricClientProvider, jobContext, commandName(), nodeId);

    StdOutDto.Config config = (StdOutDto.Config) commandConfig;
    SimpleSinkCommand.Flusher<RecordFleakData> flusher = createStdOutFlusher(config);
    SimpleSinkCommand.SinkMessagePreProcessor<RecordFleakData> messagePreProcessor =
        new PassThroughMessagePreProcessor();

    return new SinkExecutionContext<>(
        flusher,
        messagePreProcessor,
        counters.inputMessageCounter(),
        counters.errorCounter(),
        counters.sinkOutputCounter(),
        counters.outputSizeCounter(),
        counters.sinkErrorCounter());
  }

  private SimpleSinkCommand.Flusher<RecordFleakData> createStdOutFlusher(StdOutDto.Config config) {
    SerializerFactory<?> serializerFactory =
        SerializerFactory.createSerializerFactory(config.getEncodingType());
    FleakSerializer<?> serializer = serializerFactory.createSerializer();
    return new StdOutSinkFlusher(serializer);
  }

  @Override
  protected int batchSize() {
    return 1;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_STDOUT;
  }
}
