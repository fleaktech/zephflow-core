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
package io.fleak.zephflow.lib.pathselect;

import static io.fleak.zephflow.lib.utils.JsonUtils.loadFleakDataFromJsonResource;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Created by bolei on 5/23/24 */
class PathExpressionTest {

  @Test
  public void fromStringSpecialChars() {
    String input = "$[\"@a$ \\\"space\\\"\"].b[6][\"c%$$ sp\"]";
    PathExpression pathExpression = PathExpression.fromString(input);
    assertEquals(input, pathExpression.toString());
  }

  @Test
  public void testToString() {
    String input = "$[\"@a$ space\"].b[6][\"c%$$ sp\"]";
    PathExpression pathExpression = PathExpression.fromString(input);
    assertEquals(input, pathExpression.toString());
  }

  @Test
  public void fromString() {
    String input = "$.a[\"b\"][6].c";
    PathExpression pathExpression = PathExpression.fromString(input);
    assertEquals(input, pathExpression.toString());
  }

  @Test
  public void fromString2() {
    String input = "$.choices[0].message.content";
    PathExpression.fromString(input); // shouldn't throw error
  }

  @Test
  public void fromString_invalid() {
    String input = "$.batters.\"field with \\\"special\\\" char$\"[0].key";
    Exception e = assertThrows(Exception.class, () -> PathExpression.fromString(input));
    assertTrue(
        e.getMessage().startsWith(String.format("failed to parse path expression: {%s}", input)));
  }

  @Test
  public void testCalculateValue() throws IOException {
    RecordFleakData recordFleakData =
        (RecordFleakData) loadFleakDataFromJsonResource("/json/record_event_1.json");
    PathExpression pathExpression;
    // select the root event
    pathExpression = PathExpression.fromString("$");
    assertEquals(recordFleakData, pathExpression.calculateValue(recordFleakData));

    pathExpression = PathExpression.fromString("$.batters.batter[3].id");
    assertEquals(
        new StringPrimitiveFleakData("1004"), pathExpression.calculateValue(recordFleakData));

    pathExpression = PathExpression.fromString("$.c");
    assertNull(pathExpression.calculateValue(recordFleakData));

    pathExpression = PathExpression.fromString("$.non_existing_field");
    assertNull(pathExpression.calculateValue(recordFleakData));

    pathExpression =
        PathExpression.fromString("$.batters[\"field with \\\"special\\\" char$\"][0].key");
    FleakData value = pathExpression.calculateValue(recordFleakData);
    assertEquals("my special field value", value.unwrap());
    pathExpression = // equivalent path expression
        PathExpression.fromString(
            "$[\"batters\"][\"field with \\\"special\\\" char$\"][0][\"key\"]");
    value = pathExpression.calculateValue(recordFleakData);
    assertEquals("my special field value", value.unwrap());
  }

  @Test
  public void testCalculateValue_NullPayload() {
    RecordFleakData recordFleakData = new RecordFleakData();
    PathExpression pathExpression = PathExpression.fromString("$.foo.bar");
    assertNull(pathExpression.calculateValue(recordFleakData));
  }

  @Test
  public void testCalculateValue_selectObject() {
    RecordFleakData recordFleakData =
        (RecordFleakData)
            FleakData.wrap(
                Map.of(
                    "prompts",
                    List.of(Map.of("id", 1, "val", "best"), Map.of("id", 2, "val", "another"))));
    PathExpression pathExpression = PathExpression.fromString("$.prompts[0]");
    FleakData recordData = pathExpression.calculateValue(recordFleakData);
    assertEquals(Map.of("id", 1L, "val", "best"), recordData.unwrap());
  }

  @Test
  public void testCalculateValue_selectArray() {
    RecordFleakData recordFleakData =
        (RecordFleakData)
            FleakData.wrap(
                Map.of(
                    "prompts",
                    List.of(Map.of("id", 1, "val", "best"), Map.of("id", 2, "val", "another"))));
    PathExpression pathExpression = PathExpression.fromString("$.prompts");
    FleakData recordData = pathExpression.calculateValue(recordFleakData);
    assertEquals(
        List.of(Map.of("id", 1L, "val", "best"), Map.of("id", 2L, "val", "another")),
        recordData.unwrap());
  }
}
