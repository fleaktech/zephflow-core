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
package io.fleak.zephflow.lib.commands;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_ERROR_EVENT_COUNT;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import java.util.Map;

/** Created by bolei on 10/25/24 */
public abstract class DefaultCommandInitializer extends CommandInitializer {
  protected DefaultCommandInitializer(String nodeId, CommandPartsFactory commandPartsFactory) {
    super(nodeId, commandPartsFactory);
  }

  @Override
  public DefaultExecutionContext initialize(
      String commandName, JobContext jobContext, CommandConfig commandConfig) {

    Map<String, String> metricTags =
        basicCommandMetricTags(jobContext.getMetricTags(), commandName, nodeId);
    FleakCounter inputMessageCounter =
        commandPartsFactory
            .getMetricClientProvider()
            .counter(METRIC_NAME_INPUT_EVENT_COUNT, metricTags);
    FleakCounter outputMessageCounter =
        commandPartsFactory
            .getMetricClientProvider()
            .counter(METRIC_NAME_OUTPUT_EVENT_COUNT, metricTags);
    FleakCounter errorCounter =
        commandPartsFactory
            .getMetricClientProvider()
            .counter(METRIC_NAME_ERROR_EVENT_COUNT, metricTags);

    return doInitialize(
        commandName,
        jobContext,
        commandConfig,
        inputMessageCounter,
        outputMessageCounter,
        errorCounter);
  }

  protected abstract DefaultExecutionContext doInitialize(
      String commandName,
      JobContext jobContext,
      CommandConfig commandConfig,
      FleakCounter inputMessageCounter,
      FleakCounter outputMessageCounter,
      FleakCounter errorCounter);
}
