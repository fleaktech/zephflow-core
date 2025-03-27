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
package io.fleak.zephflow.lib.commands.sql;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.lib.commands.DefaultCommandInitializer;
import io.fleak.zephflow.lib.commands.DefaultInitializedConfig;
import io.fleak.zephflow.lib.sql.SQLInterpreter;

/** Created by bolei on 9/4/24 */
public class SqlCommandInitializer extends DefaultCommandInitializer {
  protected SqlCommandInitializer(String nodeId, SqlCommandPartsFactory commandPartsFactory) {
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
    SQLInterpreter sqlInterpreter = SQLInterpreter.defaultInterpreter();
    SQLInterpreter.CompiledQuery query =
        sqlInterpreter.compileQuery(((SqlCommandDto.SqlCommandConfig) commandConfig).sql());
    return new SqlInitializedConfig(
        inputMessageCounter, outputMessageCounter, errorCounter, sqlInterpreter, query);
  }
}
