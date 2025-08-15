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
import static io.fleak.zephflow.lib.utils.MiscUtils.getCallingUserTagAndEventTags;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.List;
import java.util.Map;

/** Created by bolei on 10/19/24 */
public class EvalCommand extends ScalarCommand {
  protected EvalCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      CommandInitializerFactory commandInitializerFactory) {
    super(nodeId, jobContext, configParser, configValidator, commandInitializerFactory);
  }

  @Override
  protected List<RecordFleakData> processOneEvent(
      RecordFleakData event, String callingUser, InitializedConfig initializedConfig) {
    Map<String, String> callingUserTagAndEventTags =
        getCallingUserTagAndEventTags(callingUser, event);
    EvalInitializedConfig evalInitializedConfig = (EvalInitializedConfig) initializedConfig;
    evalInitializedConfig.getInputMessageCounter().increase(callingUserTagAndEventTags);
    try {
      ExpressionValueVisitor expressionValueVisitor =
          ExpressionValueVisitor.createInstance(event, evalInitializedConfig.getPythonExecutor());
      FleakData fleakData =
          expressionValueVisitor.visit(evalInitializedConfig.getLanguageContext());
      if (fleakData instanceof RecordFleakData) {
        evalInitializedConfig.getOutputMessageCounter().increase(callingUserTagAndEventTags);
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
        evalInitializedConfig
            .getOutputMessageCounter()
            .increase(outputEvents.size(), callingUserTagAndEventTags);
        return outputEvents;
      }
      throw new IllegalArgumentException(
          "Illegal result type: " + toJsonString(fleakData.unwrap()));
    } catch (Exception e) {
      evalInitializedConfig.getErrorCounter().increase(callingUserTagAndEventTags);
      throw e;
    }
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_EVAL;
  }
}
