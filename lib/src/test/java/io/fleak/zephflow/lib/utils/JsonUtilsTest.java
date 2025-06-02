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
package io.fleak.zephflow.lib.utils;

import static io.fleak.zephflow.lib.utils.JsonUtils.*;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NumericNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Created by bolei on 4/9/24 */
class JsonUtilsTest {
  @Test
  public void testDeserializeInstant() {
    String jsonStr =
        """
        {
          "instant": 3
        }
        """;
    Foo foo = JsonUtils.fromJsonString(jsonStr, new TypeReference<>() {});
    assertNotNull(foo);
    assertEquals(3000L, foo.instant.toEpochMilli());
  }

  public static class Foo {
    public Instant instant;
  }

  @Test
  public void testParseBadJson() {
    var badJsons =
        List.of(
            "{\"foo\": bar}", // value missing quotes
            "{\"k1\": \"v1\"}, {\"k2\", \"v2\"}", // array missing square brackets
            "{\"k\", 1, \"k\", 2}" // duplicate keys
            );
    badJsons.forEach(
        badJsonStr -> {
          System.out.println(badJsonStr);
          Exception e =
              assertThrows(JsonProcessingException.class, () -> OBJECT_MAPPER.readTree(badJsonStr));
          System.out.println(e.getMessage());
        });
  }

  @Test
  public void testToJsonPayload() throws IOException {
    ObjectNode objectNode =
        JsonUtils.fromJsonResource("/json/record_event_1.json", new TypeReference<>() {});
    RecordFleakData input = fromJsonPayload(objectNode);
    ObjectNode output = toJsonPayload(input);
    Assertions.assertEquals(objectNode, output);
  }

  @Test
  public void testToArrayNode() throws IOException {
    ArrayNode arrayNode =
        JsonUtils.fromJsonResource("/json/arr_num_event.json", new TypeReference<>() {});
    ArrayFleakData input = fromJsonArray(arrayNode);
    ArrayNode output = toArrayNode(input);

    ArrayNode expected = JsonUtils.OBJECT_MAPPER.createArrayNode();
    expected.add(100);
    expected.add(1000000000000L);
    expected.add(3.0);
    expected.add(3.14159);

    Assertions.assertEquals(expected, output);
  }

  @Test
  public void testToJsonNumber() throws IOException {
    ArrayNode arrayNode =
        JsonUtils.fromJsonResource("/json/arr_num_event.json", new TypeReference<>() {});
    NumericNode numericNode = (NumericNode) arrayNode.get(0);
    NumberPrimitiveFleakData input = fromJsonNumber(numericNode);
    NumericNode output = toJsonNumber(input);
    Assertions.assertEquals(100, output.numberValue());
    Assertions.assertEquals(JsonParser.NumberType.INT, output.numberType());
  }

  @Test
  public void testFromJsonPayload() throws IOException {
    ObjectNode input =
        JsonUtils.fromJsonResource("/json/record_event_1.json", new TypeReference<>() {});
    RecordFleakData output = fromJsonPayload(input);
    Assertions.assertEquals("0001", output.getPayload().get("id").getStringValue());
    Assertions.assertEquals(0.55d, output.getPayload().get("ppu").getNumberValue());
    Assertions.assertEquals(
        NumberPrimitiveFleakData.NumberType.DOUBLE, output.getPayload().get("ppu").getNumberType());
    Assertions.assertEquals(
        4, output.getPayload().get("batters").getPayload().get("batter").getArrayPayload().size());
    Assertions.assertEquals(
        "1004",
        output
            .getPayload()
            .get("batters")
            .getPayload()
            .get("batter")
            .getArrayPayload()
            .get(3)
            .getPayload()
            .get("id")
            .getStringValue());

    Assertions.assertTrue(output.getPayload().get("b").isTrueValue());
  }

  @Test
  public void testFromJsonNode() throws IOException {
    JsonNode input =
        JsonUtils.fromJsonResource("/json/arr_event_smiple.json", new TypeReference<>() {});
    FleakData output = fromJsonNode(input);
    Assertions.assertNotNull(output);
    Assertions.assertEquals(5, output.getArrayPayload().size());
    Assertions.assertEquals(400, output.getArrayPayload().get(4).getNumberValue());
    Assertions.assertEquals(
        NumberPrimitiveFleakData.NumberType.INT, output.getArrayPayload().get(4).getNumberType());
  }

  @Test
  public void testFromJsonArray() throws IOException {
    ArrayFleakData arrayFleakData =
        fromJsonArray(
            JsonUtils.fromJsonResource("/json/arr_num_event.json", new TypeReference<>() {}));
    Assertions.assertNotNull(arrayFleakData);
    Assertions.assertEquals(4, arrayFleakData.getArrayPayload().size());
    Assertions.assertEquals(100, arrayFleakData.getArrayPayload().get(0).getNumberValue());
    Assertions.assertEquals(
        NumberPrimitiveFleakData.NumberType.INT,
        arrayFleakData.getArrayPayload().get(0).getNumberType());
  }

  @Test
  public void testFromJsonNumber() throws IOException {
    ArrayFleakData arrayFleakData =
        fromJsonArray(
            JsonUtils.fromJsonResource("/json/arr_num_event.json", new TypeReference<>() {}));
    Assertions.assertNotNull(arrayFleakData);
    Assertions.assertEquals(4, arrayFleakData.getArrayPayload().size());
    Assertions.assertEquals(
        NumberPrimitiveFleakData.NumberType.INT,
        arrayFleakData.getArrayPayload().get(0).getNumberType());
    Assertions.assertEquals(
        NumberPrimitiveFleakData.NumberType.LONG,
        arrayFleakData.getArrayPayload().get(1).getNumberType());
    Assertions.assertEquals(
        NumberPrimitiveFleakData.NumberType.DOUBLE,
        arrayFleakData.getArrayPayload().get(3).getNumberType());
  }

  @Test
  public void testNullValues() {
    Map<String, Object> payload = new HashMap<>();
    payload.put("k1", 1);
    payload.put("k2", null);
    String jsonStr = toJsonString(payload);

    Map<String, Object> deserialized = fromJsonString(jsonStr, new TypeReference<>() {});
    assertEquals(payload, deserialized);
  }
}
