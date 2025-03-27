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

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.lib.antlr.EvalExpressionParser;
import io.fleak.zephflow.lib.commands.DefaultCommandInitializer;
import io.fleak.zephflow.lib.commands.DefaultInitializedConfig;
import io.fleak.zephflow.lib.utils.AntlrUtils;

/** Created by bolei on 10/19/24 */
public class EvalCommandInitializer extends DefaultCommandInitializer {
  protected EvalCommandInitializer(String nodeId, EvalCommandPartsFactory commandPartsFactory) {
    super(nodeId, commandPartsFactory);
  }

  @Override
  protected DefaultInitializedConfig doInitialize(
      String commandName,
      JobContext jobContext,
      CommandConfig commandConfig,
      FleakCounter inputMessageCounter,
      FleakCounter outputMessageCounter,
      FleakCounter errorCounter) {
    EvalCommandDto.Config config = (EvalCommandDto.Config) commandConfig;
    EvalExpressionParser parser =
        (EvalExpressionParser)
            AntlrUtils.parseInput(config.expression(), AntlrUtils.GrammarType.EVAL);
    return new EvalInitializedConfig(
        inputMessageCounter,
        outputMessageCounter,
        errorCounter,
        parser.language(),
        config.assertion());
  }
}
