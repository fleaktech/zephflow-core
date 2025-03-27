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
package io.fleak.zephflow.lib.commands.filesource;

import static io.fleak.zephflow.lib.utils.JsonUtils.fromJsonResource;
import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.SourceCommand;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.commands.OperatorCommandRegistry;
import io.fleak.zephflow.lib.commands.kafkasource.KafkaSourceCommandTest;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.utils.MiscUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Created by bolei on 3/24/25 */
class FileSourceCommandTest {
  @Test
  public void test(@TempDir Path tempDir) throws Exception {
    Path inputFile = tempDir.resolve("test_input.csv");
    String content = MiscUtils.loadStringFromResource("/serdes/test_csv_input.csv");
    Files.writeString(inputFile, content);
    var fac = OperatorCommandRegistry.OPERATOR_COMMANDS.get(MiscUtils.COMMAND_NAME_FILE_SOURCE);
    SourceCommand command = (SourceCommand) fac.createCommand("test_node", TestUtils.JOB_CONTEXT);

    command.parseAndValidateArg(
        toJsonString(
            FileSourceDto.Config.builder()
                .encodingType(EncodingType.CSV)
                .filePath(inputFile.toString())
                .build()));
    KafkaSourceCommandTest.TestSourceEventAcceptor eventConsumer =
        new KafkaSourceCommandTest.TestSourceEventAcceptor();
    command.execute(
        "test_user", new MetricClientProvider.NoopMetricClientProvider(), eventConsumer);
    List<RecordFleakData> expected = fromJsonResource("/serdes/test_csv_expected_output.json", new TypeReference<>() {});
    assertEquals(expected, eventConsumer.getReceivedEvents());
  }
}
