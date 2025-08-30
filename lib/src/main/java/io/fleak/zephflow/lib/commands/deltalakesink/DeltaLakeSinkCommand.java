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
package io.fleak.zephflow.lib.commands.deltalakesink;

import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_DELTA_LAKE_SINK;

import io.fleak.zephflow.api.ConfigParser;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.util.Map;

public class DeltaLakeSinkCommand extends SimpleSinkCommand<Map<String, Object>> {

  protected DeltaLakeSinkCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      DeltaLakeSinkCommandInitializerFactory initializerFactory) {
    super(nodeId, jobContext, configParser, configValidator, initializerFactory);
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_DELTA_LAKE_SINK;
  }

  @Override
  protected int batchSize() {
    if (commandConfig != null) {
      DeltaLakeSinkDto.Config config = (DeltaLakeSinkDto.Config) commandConfig;
      return config.getBatchSize();
    }
    return 1000; // Fallback default if config not yet parsed
  }
}
