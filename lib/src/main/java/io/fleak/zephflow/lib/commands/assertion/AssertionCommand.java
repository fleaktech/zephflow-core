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
package io.fleak.zephflow.lib.commands.assertion;

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_ASSERTION;
import static io.fleak.zephflow.lib.utils.MiscUtils.getCallingUserTagAndEventTags;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.structure.BooleanPrimitiveFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.eval.EvalExecutionContext;
import io.fleak.zephflow.lib.commands.eval.ExpressionValueVisitor;
import java.util.List;
import java.util.Map;

/** Created by bolei on 11/12/24 */
public class AssertionCommand extends ScalarCommand {
  protected AssertionCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      CommandInitializerFactory commandInitializerFactory) {
    super(nodeId, jobContext, configParser, configValidator, commandInitializerFactory);
  }

  @Override
  protected List<RecordFleakData> processOneEvent(
      RecordFleakData event, String callingUser, ExecutionContext context) {
    Map<String, String> callingUserTagAndEventTags =
        getCallingUserTagAndEventTags(callingUser, event);
    EvalExecutionContext evalContext = (EvalExecutionContext) context;
    evalContext.getInputMessageCounter().increase(callingUserTagAndEventTags);
    ExpressionValueVisitor expressionValueVisitor =
        ExpressionValueVisitor.createInstance(event, evalContext.getPythonExecutor());
    FleakData fleakData = new BooleanPrimitiveFleakData(false);
    try {
      fleakData = expressionValueVisitor.visit(evalContext.getLanguageContext());
    } catch (Exception e) {
      // no-op
      // any exception thrown during evaluation should be caught here and causing the expression
      // evaluation to false
    }

    if (fleakData instanceof BooleanPrimitiveFleakData && fleakData.isTrueValue()) {
      evalContext.getOutputMessageCounter().increase(callingUserTagAndEventTags);
      return List.of(event);
    }

    if (!evalContext.isAssertion()) {
      return List.of();
    }
    evalContext.getErrorCounter().increase(callingUserTagAndEventTags);
    throw new IllegalArgumentException("assertion failed: " + toJsonString(event.unwrap()));
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_ASSERTION;
  }
}
