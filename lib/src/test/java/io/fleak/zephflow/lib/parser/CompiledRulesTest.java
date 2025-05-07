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
package io.fleak.zephflow.lib.parser;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.parser.extractions.JsonExtractionConfig;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

/** Created by bolei on 10/14/24 */
class CompiledRulesTest {
  @Test
  public void testParseCiscoAsaLog() throws Exception {
    ParserConfigCompiler parserConfigCompiler = new ParserConfigCompiler();

    ParserConfigs.ParserConfig parserConfig =
        JsonUtils.fromJsonResource("/parser/cisco_asa_config.json", new TypeReference<>() {});
    CompiledRules.ParseRule parseRule = parserConfigCompiler.compile(parserConfig);

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
      List<RecordFleakData> parsedEvents = inputEvents.stream().map(parseRule::parse).toList();

      List<RecordFleakData> expected =
          expectedDataDeserializer.deserialize(expectedDataSerializedEvent);
      assertEquals(expected, parsedEvents);
    }
  }

  @Test
  public void testJsonExtractionRule() {
    RecordFleakData inputEvent = (RecordFleakData) FleakData.wrap(Map.of("k", "{\"a\":100}"));
    ParserConfigs.ParserConfig parserConfig =
        ParserConfigs.ParserConfig.builder()
            .targetField("k")
            .extractionConfig(new JsonExtractionConfig())
            .removeTargetField(true)
            .build();
    ParserConfigCompiler parserConfigCompiler = new ParserConfigCompiler();
    CompiledRules.ParseRule parseRule = parserConfigCompiler.compile(parserConfig);
    assertNotNull(inputEvent);
    RecordFleakData output = parseRule.parse(inputEvent);
    assertEquals(Map.of("a", 100), output.unwrap());
  }
}
