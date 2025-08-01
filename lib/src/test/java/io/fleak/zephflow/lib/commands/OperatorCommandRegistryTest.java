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
package io.fleak.zephflow.lib.commands;

import static io.fleak.zephflow.lib.commands.OperatorCommandRegistry.OPERATOR_COMMANDS;
import static io.fleak.zephflow.lib.utils.MiscUtils.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableSet;

/** Created by bolei on 9/6/24 */
class OperatorCommandRegistryTest {
  @Test
  void testListAllCommands() {
    assertEquals(
        ImmutableSet.builder()
            .add(COMMAND_NAME_NOOP)
            .add(COMMAND_NAME_SQL_EVAL)
            .add(COMMAND_NAME_S3_SINK)
            .add(COMMAND_NAME_KINESIS_SINK)
            .add(COMMAND_NAME_KINESIS_SOURCE)
            .add(COMMAND_NAME_KAFKA_SOURCE)
            .add(COMMAND_NAME_KAFKA_SINK)
            .add(COMMAND_NAME_EVAL)
            .add(COMMAND_NAME_ASSERTION)
            .add(COMMAND_NAME_FILTER)
            .add(COMMAND_NAME_STDIN)
            .add(COMMAND_NAME_STDOUT)
            .add(COMMAND_NAME_PARSER)
            .add(COMMAND_NAME_FILE_SOURCE)
            .add(COMMAND_NAME_CLICK_HOUSE_SINK)
            .add(COMMAND_NAME_READER_SOURCE)
            .build(),
        OPERATOR_COMMANDS.keySet());
  }
}
