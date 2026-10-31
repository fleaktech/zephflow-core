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
import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_STDOUT;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.ScalarSinkCommand;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.OperatorCommandRegistry;
import io.fleak.zephflow.lib.commands.stdin.StdInSourceDto;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
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
        OBJECT_MAPPER.convertValue(
            StdInSourceDto.Config.builder().encodingType(EncodingType.JSON_OBJECT).build(),
            new TypeReference<>() {}));
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
    var context = command.getExecutionContext();
    command.writeToSink(inputEvents, "my_user", context);
  }

  @Test
  public void encodingPrintsEachRecordInThatFormat() {
    ScalarSinkCommand command =
        (ScalarSinkCommand)
            OperatorCommandRegistry.OPERATOR_COMMANDS
                .get(COMMAND_NAME_STDOUT)
                .createCommand("my_node", JOB_CONTEXT);
    command.parseAndValidateArg(
        Map.of("encoding", Map.of("format", Map.of("type", "json", "mode", "object"))));
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());

    var bytes = new ByteArrayOutputStream();
    var original = System.out;
    try (var capture = new PrintStream(bytes, true, StandardCharsets.UTF_8)) {
      System.setOut(capture);
      command.writeToSink(
          List.of(
              (RecordFleakData) FleakData.wrap(Map.of("id", 1, "name", "alice")),
              (RecordFleakData) FleakData.wrap(Map.of("id", 2, "name", "bob"))),
          "my_user",
          command.getExecutionContext());
    } finally {
      System.setOut(original);
    }

    assertEquals(
        "{\"name\":\"alice\",\"id\":1}"
            + System.lineSeparator()
            + "{\"name\":\"bob\",\"id\":2}"
            + System.lineSeparator(),
        bytes.toString(StandardCharsets.UTF_8));
  }
}
