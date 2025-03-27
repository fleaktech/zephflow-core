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
package io.fleak.zephflow.lib.commands.stdin;

import static io.fleak.zephflow.lib.TestUtils.JOB_CONTEXT;
import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_STDIN;

import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.commands.OperatorCommandRegistry;
import io.fleak.zephflow.lib.commands.source.SimpleSourceCommand;
import io.fleak.zephflow.lib.serdes.EncodingType;

/** Created by bolei on 12/20/24 */
class StdInSourceCommandTest {

  public static void main(String[] args) {
    SourceEventAcceptor eventConsumer = new TestUtils.TestSourceEventAcceptor();
    SimpleSourceCommand command =
        (SimpleSourceCommand)
            OperatorCommandRegistry.OPERATOR_COMMANDS
                .get(COMMAND_NAME_STDIN)
                .createCommand("my_node", JOB_CONTEXT);
    command.parseAndValidateArg(
        toJsonString(
            StdInSourceDto.Config.builder().encodingType(EncodingType.JSON_OBJECT).build()));
    command.execute("my_user", new MetricClientProvider.NoopMetricClientProvider(), eventConsumer);
  }
}
