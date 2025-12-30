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
package io.fleak.zephflow.lib.commands.eval;

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_EVAL;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_ERROR_EVENT_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_INPUT_EVENT_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_NAME_OUTPUT_EVENT_COUNT;
import static io.fleak.zephflow.lib.utils.MiscUtils.basicCommandMetricTags;
import static io.fleak.zephflow.lib.utils.MiscUtils.getCallingUserTagAndEventTags;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.eval.python.PythonExecutor;
import io.fleak.zephflow.lib.utils.AntlrUtils;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/** Created by bolei on 10/19/24 */
@Slf4j
public class EvalCommand extends ScalarCommand {
  protected EvalCommand(
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
    return createEvalExecutionContext(
        metricClientProvider, jobContext, commandConfig, nodeId, commandName());
  }

  /**
   * Shared helper to create EvalExecutionContext. Used by EvalCommand, AssertionCommand, and
   * FilterCommand.
   */
  public static EvalExecutionContext createEvalExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId,
      String commandName) {
    // Create counters
    Map<String, String> metricTags =
        basicCommandMetricTags(jobContext.getMetricTags(), commandName, nodeId);
    FleakCounter inputMessageCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_EVENT_COUNT, metricTags);
    FleakCounter outputMessageCounter =
        metricClientProvider.counter(METRIC_NAME_OUTPUT_EVENT_COUNT, metricTags);
    FleakCounter errorCounter =
        metricClientProvider.counter(METRIC_NAME_ERROR_EVENT_COUNT, metricTags);

    // Parse expression and create execution context
    EvalCommandDto.Config config = (EvalCommandDto.Config) commandConfig;
    EvalExpressionParser parser =
        (EvalExpressionParser)
            AntlrUtils.parseInput(config.expression(), AntlrUtils.GrammarType.EVAL);
    EvalExpressionParser.LanguageContext languageContext = parser.language();

    PythonExecutor pythonExecutor = null;
    try {
      pythonExecutor = PythonExecutor.createPythonExecutor(languageContext);
    } catch (Exception e) {
      log.error(
          "An unexpected error occurred during Python support initialization. Python execution will be disabled.",
          e);
    }

    // Build expression cache to avoid repeated expensive operations during evaluation
    ExpressionCache expressionCache = ExpressionCache.build(languageContext);

    return new EvalExecutionContext(
        inputMessageCounter,
        outputMessageCounter,
        errorCounter,
        languageContext,
        config.assertion(),
        pythonExecutor,
        expressionCache);
  }

  @Override
  protected List<RecordFleakData> processOneEvent(
      RecordFleakData event, String callingUser, ExecutionContext context) {
    Map<String, String> callingUserTagAndEventTags =
        getCallingUserTagAndEventTags(callingUser, event);
    EvalExecutionContext evalContext = (EvalExecutionContext) context;
    evalContext.getInputMessageCounter().increase(callingUserTagAndEventTags);
    try {
      ExpressionValueVisitor expressionValueVisitor =
          ExpressionValueVisitor.createInstance(
              event, evalContext.getPythonExecutor(), evalContext.getExpressionCache());
      FleakData fleakData = expressionValueVisitor.visit(evalContext.getLanguageContext());
      if (fleakData == null) {
        return List.of();
      }
      if (fleakData instanceof RecordFleakData) {
        evalContext.getOutputMessageCounter().increase(callingUserTagAndEventTags);
        return List.of((RecordFleakData) fleakData);
      }
      if (fleakData instanceof ArrayFleakData) {
        List<RecordFleakData> outputEvents =
            fleakData.getArrayPayload().stream()
                .map(
                    fd -> {
                      try {
                        return ((RecordFleakData) fd);
                      } catch (ClassCastException e) {
                        throw new IllegalArgumentException(
                            String.format("failed to cast %s into RecordFleakData", fd));
                      }
                    })
                .toList();
        evalContext
            .getOutputMessageCounter()
            .increase(outputEvents.size(), callingUserTagAndEventTags);
        return outputEvents;
      }
      throw new IllegalArgumentException(
          "Illegal result type: " + toJsonString(fleakData.unwrap()));
    } catch (Exception e) {
      evalContext.getErrorCounter().increase(callingUserTagAndEventTags);
      throw e;
    }
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_EVAL;
  }
}
