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

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.CommandPartsFactory;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.lib.commands.DefaultCommandInitializer;
import io.fleak.zephflow.lib.commands.DefaultInitializedConfig;
import io.fleak.zephflow.lib.parser.CompiledRules;
import io.fleak.zephflow.lib.parser.ParserConfigCompiler;
import io.fleak.zephflow.lib.parser.ParserConfigs;

/** Created by bolei on 3/18/25 */
public class ParserCommandInitializer extends DefaultCommandInitializer {

  protected ParserCommandInitializer(String nodeId, CommandPartsFactory commandPartsFactory) {
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
    ParserConfigs.ParserConfig parserConfig = (ParserConfigs.ParserConfig) commandConfig;
    ParserConfigCompiler compiler = new ParserConfigCompiler();
    CompiledRules.ParseRule parseRule = compiler.compile(parserConfig);
    return new ParserInitializedConfig(
        inputMessageCounter, outputMessageCounter, errorCounter, parseRule);
  }
}
