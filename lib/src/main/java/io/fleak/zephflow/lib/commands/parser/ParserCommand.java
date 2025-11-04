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
package io.fleak.zephflow.lib.commands.parser;

import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_PARSER;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_ERROR_EVENT_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_INPUT_EVENT_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_OUTPUT_EVENT_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.basicCommandMetricTags;
import static io.fleak.zephflow.lib.utils.MiscUtils.getCallingUserTagAndEventTags;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.parser.CompiledRules;
import io.fleak.zephflow.lib.parser.ParserConfigCompiler;
import io.fleak.zephflow.lib.parser.ParserConfigs;
import java.util.List;
import java.util.Map;

/** Created by bolei on 3/18/25 */
public class ParserCommand extends ScalarCommand {
  protected ParserCommand(
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
    // Create counters
    Map<String, String> metricTags =
        basicCommandMetricTags(jobContext.getMetricTags(), commandName(), nodeId);
    FleakCounter inputMessageCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_EVENT_COUNT, metricTags);
    FleakCounter outputMessageCounter =
        metricClientProvider.counter(METRIC_NAME_OUTPUT_EVENT_COUNT, metricTags);
    FleakCounter errorCounter =
        metricClientProvider.counter(METRIC_NAME_ERROR_EVENT_COUNT, metricTags);

    // Compile parser rules
    ParserConfigs.ParserConfig parserConfig = (ParserConfigs.ParserConfig) commandConfig;
    ParserConfigCompiler compiler = new ParserConfigCompiler();
    CompiledRules.ParseRule parseRule = compiler.compile(parserConfig);

    return new ParserExecutionContext(
        inputMessageCounter, outputMessageCounter, errorCounter, parseRule);
  }

  @Override
  protected List<RecordFleakData> processOneEvent(
      RecordFleakData event, String callingUser, ExecutionContext context) {
    Map<String, String> tags = getCallingUserTagAndEventTags(callingUser, event);
    ParserExecutionContext parserContext = (ParserExecutionContext) context;
    parserContext.getInputMessageCounter().increase(tags);
    try {
      RecordFleakData parsed = parserContext.getParseRule().parse(event);
      return List.of(parsed);
    } catch (Exception e) {
      parserContext.getErrorCounter().increase(tags);
      throw e;
    }
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_PARSER;
  }
}
