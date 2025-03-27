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
package io.fleak.zephflow.lib.commands.stdout;

import static io.fleak.zephflow.lib.TestUtils.JOB_CONTEXT;
import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_STDOUT;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.ScalarSinkCommand;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.OperatorCommandRegistry;
import io.fleak.zephflow.lib.commands.stdin.StdInSourceDto;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Created by bolei on 12/20/24 */
class StdOutSinkCommandTest {

  @Test
  public void test() throws IOException {
    ScalarSinkCommand command =
        (ScalarSinkCommand)
            OperatorCommandRegistry.OPERATOR_COMMANDS
                .get(COMMAND_NAME_STDOUT)
                .createCommand("my_node", JOB_CONTEXT);

    FleakData fleakData =
        JsonUtils.loadFleakDataFromJsonResource("/json/multiple_simple_events.json");
    List<RecordFleakData> inputEvents =
        fleakData.getArrayPayload().stream().map(fd -> ((RecordFleakData) fd)).toList();
    command.parseAndValidateArg(
        toJsonString(
            StdInSourceDto.Config.builder().encodingType(EncodingType.JSON_OBJECT).build()));
    command.writeToSink(
        inputEvents, "my_user", new MetricClientProvider.NoopMetricClientProvider());
  }
}
