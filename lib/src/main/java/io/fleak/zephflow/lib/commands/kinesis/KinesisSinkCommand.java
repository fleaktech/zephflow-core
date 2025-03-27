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
package io.fleak.zephflow.lib.commands.kinesis;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.JsonConfigParser;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;

public class KinesisSinkCommand extends SimpleSinkCommand<RecordFleakData> {
  private static final int KINESIS_SINK_BATCH_SIZE = 100;

  protected KinesisSinkCommand(
      String nodeId,
      JobContext jobContext,
      JsonConfigParser<KinesisSinkDto.Config> configParser,
      KinesisSinkConfigValidator configValidator,
      KinesisSinkCommandInitializerFactory sinkCommandInitializerFactory) {
    super(nodeId, jobContext, configParser, configValidator, sinkCommandInitializerFactory);
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_KINESIS_SINK;
  }

  @Override
  protected int batchSize() {
    return KINESIS_SINK_BATCH_SIZE;
  }
}
