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
import io.fleak.zephflow.lib.commands.eval.compiled.CompiledExpression;
import io.fleak.zephflow.lib.commands.eval.compiled.ExpressionCompiler;
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

    // Compile expression to lightweight AST for efficient evaluation
    CompiledExpression compiledExpression =
        ExpressionCompiler.compile(languageContext, pythonExecutor);

    return new EvalExecutionContext(
        inputMessageCounter,
        outputMessageCounter,
        errorCounter,
        languageContext,
        config.assertion(),
        pythonExecutor,
        compiledExpression);
  }

  @Override
  public ProcessResult process(
      List<RecordFleakData> events, String callingUser, ExecutionContext context) {
    ProcessResult result = new ProcessResult();
    EvalExecutionContext evalContext = (EvalExecutionContext) context;

    for (RecordFleakData event : events) {
      Map<String, String> tags = getCallingUserTagAndEventTags(callingUser, event);
      evalContext.getInputMessageCounter().increase(tags);
      try {
        FleakData fleakData = evalContext.getCompiledExpression().evaluate(event);
        if (fleakData == null) {
          continue;
        }
        if (fleakData instanceof RecordFleakData rd) {
          evalContext.getOutputMessageCounter().increase(tags);
          result.getOutput().add(rd);
        } else if (fleakData instanceof ArrayFleakData) {
          for (FleakData element : fleakData.getArrayPayload()) {
            if (element instanceof RecordFleakData rd) {
              evalContext.getOutputMessageCounter().increase(tags);
              result.getOutput().add(rd);
            } else {
              evalContext.getErrorCounter().increase(tags);
              result
                  .getFailureEvents()
                  .add(
                      new ErrorOutput(
                          event, String.format("failed to cast %s into RecordFleakData", element)));
            }
          }
        } else {
          evalContext.getErrorCounter().increase(tags);
          result
              .getFailureEvents()
              .add(
                  new ErrorOutput(
                      event, "Illegal result type: " + toJsonString(fleakData.unwrap())));
        }
      } catch (Exception e) {
        evalContext.getErrorCounter().increase(tags);
        result.getFailureEvents().add(new ErrorOutput(event, e.getMessage()));
      }
    }
    return result;
  }

  @Override
  protected List<RecordFleakData> processOneEvent(
      RecordFleakData event, String callingUser, ExecutionContext context) {
    throw new UnsupportedOperationException("Use process() instead");
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_EVAL;
  }
}
