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

import static io.fleak.zephflow.lib.TestUtils.JOB_CONTEXT;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

/** Created by bolei on 3/18/25 */
class ParserCommandTest {
  @Test
  public void testParse() throws IOException {
    Map<String, Object> parserConfigMap = JsonUtils.fromJsonResource("/parser/cisco_asa_config.json", new TypeReference<>() {});
    ParserCommand command =
        (ParserCommand) new ParserCommandFactory().createCommand("my_node", JOB_CONTEXT);
    command.parseAndValidateArg(parserConfigMap);

    DeserializerFactory<?> deserializerFactory =
        DeserializerFactory.createDeserializerFactory(EncodingType.STRING_LINE);
    FleakDeserializer<?> deserializer = deserializerFactory.createDeserializer();

    DeserializerFactory<?> expectedDataDeserializerFactory =
        DeserializerFactory.createDeserializerFactory(EncodingType.JSON_ARRAY);
    FleakDeserializer<?> expectedDataDeserializer =
        expectedDataDeserializerFactory.createDeserializer();

    try (InputStream testInput = this.getClass().getResourceAsStream("/parser/cisco_asa_data.txt");
        InputStream expectedIn =
            this.getClass().getResourceAsStream("/parser/cisco_asa_parsed.json")) {
      byte[] input = IOUtils.toByteArray(Objects.requireNonNull(testInput));
      SerializedEvent serializedEvent = new SerializedEvent(null, input, null);

      byte[] expectedOutput = IOUtils.toByteArray(Objects.requireNonNull(expectedIn));
      SerializedEvent expectedDataSerializedEvent = new SerializedEvent(null, expectedOutput, null);

      List<RecordFleakData> inputEvents = deserializer.deserialize(serializedEvent);
      var context = command.initialize(new MetricClientProvider.NoopMetricClientProvider());
      var processResult = command.process(inputEvents, "test_user", context);

      List<RecordFleakData> parsedEvents = processResult.getOutput();

      List<RecordFleakData> expected =
          expectedDataDeserializer.deserialize(expectedDataSerializedEvent);
      assertEquals(expected, parsedEvents);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
