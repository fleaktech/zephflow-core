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
package io.fleak.zephflow.lib.commands.sink;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import java.util.Map;

/** Created by bolei on 9/3/24 */
public class SinkInitializer<T> extends CommandInitializer {

  public SinkInitializer(String nodeId, CommandPartsFactory commandPartsFactory) {
    super(nodeId, commandPartsFactory);
  }

  @Override
  public InitializedConfig initialize(
      String commandName, JobContext jobContext, CommandConfig commandConfig) {

    //noinspection unchecked
    SinkCommandPartsFactory<T> partsFactory = (SinkCommandPartsFactory<T>) commandPartsFactory;

    Map<String, String> metricTags =
        basicCommandMetricTags(jobContext.getMetricTags(), commandName, nodeId);
    FleakCounter inputMessageCounter =
        partsFactory.getMetricClientProvider().counter(METRIC_NAME_INPUT_EVENT_COUNT, metricTags);
    FleakCounter errorCounter = // error count during preparation phase
        partsFactory.getMetricClientProvider().counter(METRIC_NAME_ERROR_EVENT_COUNT, metricTags);
    FleakCounter sinkOutputCounter =
        partsFactory.getMetricClientProvider().counter(METRIC_NAME_SINK_OUTPUT_COUNT, metricTags);
    FleakCounter sinkErrorCounter = // error count during flushing phase
        partsFactory.getMetricClientProvider().counter(METRIC_NAME_SINK_ERROR_COUNT, metricTags);

    SimpleSinkCommand.Flusher<T> flusher = partsFactory.createFlusher();
    SimpleSinkCommand.SinkMessagePreProcessor<T> messagePreProcessor =
        partsFactory.createMessagePreProcessor();

    return new SinkInitializedConfig<>(
        flusher,
        messagePreProcessor,
        inputMessageCounter,
        errorCounter,
        sinkOutputCounter,
        sinkErrorCounter);
  }
}
