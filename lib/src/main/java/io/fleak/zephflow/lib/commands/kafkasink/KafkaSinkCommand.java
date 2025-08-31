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
package io.fleak.zephflow.lib.commands.kafkasink;

import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_KAFKA_SINK;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;

public class KafkaSinkCommand extends SimpleSinkCommand<RecordFleakData> {

  protected KafkaSinkCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      KafkaSinkCommandInitializerFactory initializerFactory) {
    super(nodeId, jobContext, configParser, configValidator, initializerFactory);
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_KAFKA_SINK;
  }

  @Override
  protected int batchSize() {
    // Use very large batch size to effectively disable SimpleSinkCommand-level batching
    // Let BatchKafkaSinkFlusher handle all batching logic instead
    return Integer.MAX_VALUE;
  }
}
