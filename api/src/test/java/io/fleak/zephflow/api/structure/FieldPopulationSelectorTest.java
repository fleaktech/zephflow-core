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
package io.fleak.zephflow.api.structure;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FieldPopulationSelectorTest {
  private static final ObjectMapper JSON = new ObjectMapper();

  @Test
  void distinguishesPresenceScalarsAndEmptyContainers() throws Exception {
    String json =
        "{\"nil\":null,\"object\":{},\"array\":[],\"zero\":0,\"no\":false,\"empty\":\"\"}";
    FleakData record = record(json);
    for (String field : List.of("nil", "object", "array", "zero", "no", "empty")) {
      assertTrue(FieldPopulationSelector.isPresent(record, field), field);
      assertTrue(FieldPopulationSelector.visitJson(json, field, ignored -> {}), field);
    }
    assertFalse(FieldPopulationSelector.isPresent(record, "missing"));
    assertFalse(FieldPopulationSelector.matchesValue(record, "missing", "null"));
    assertTrue(FieldPopulationSelector.matchesValue(record, "nil", "null"));
    assertEquals(List.of(), values(json, "object"));
    assertEquals(List.of(), values(json, "array"));
    assertEquals(List.of("\"\""), values(json, "empty"));
  }

  @Test
  void traversesNestedArraysAndMergesLiteralAndNestedDottedNames() throws Exception {
    String json =
        "{\"a.b\":\"literal\",\"a\":[{\"b\":[[\"nested\",\"nested\"],null]},{\"b\":\"literal\"}]}";
    FleakData record = record(json);
    assertEquals(
        new LinkedHashSet<>(List.of("\"literal\"", "\"nested\"", "null")),
        new LinkedHashSet<>(values(json, "a.b")));
    assertEquals(5, values(json, "a.b").size());
    for (String scalar : List.of("\"literal\"", "\"nested\"", "null")) {
      assertTrue(FieldPopulationSelector.matchesValue(record, "a.b", scalar));
    }
    assertFalse(FieldPopulationSelector.matchesValue(record, "a.b", "\"new\""));
    assertFalse(FieldPopulationSelector.isPresent(record, "a.missing"));
  }

  @Test
  void preservesEmptyNamesEscapesUnicodeAndTypedScalarIdentity() throws Exception {
    String json =
        "{\"\":false,\"q\\\"\\\\\":\"v\\\"\\\\\\n\",\"żółć\":\"gęś\",\"v\":[200,\"200\",null,\"null\",\"{NULL}\"]}";
    FleakData record = record(json);
    assertTrue(FieldPopulationSelector.matchesValue(record, "", "false"));
    assertTrue(FieldPopulationSelector.matchesValue(record, "q\"\\", "\"v\\\"\\\\\\n\""));
    assertTrue(FieldPopulationSelector.matchesValue(record, "żółć", "\"gęś\""));
    assertEquals(List.of("200", "\"200\"", "null", "\"null\"", "\"{NULL}\""), values(json, "v"));
    for (String scalar : values(json, "v")) {
      assertTrue(FieldPopulationSelector.matchesValue(record, "v", scalar));
    }
  }

  @Test
  void retainsNumberTokensWithoutComparisonTolerance() throws Exception {
    String json = "{\"v\":1.0000001,\"large\":9223372036854775807}";
    assertEquals(List.of("1.0000001"), values(json, "v"));
    assertEquals(List.of("9223372036854775807"), values(json, "large"));
    FleakData record = record(json);
    assertFalse(FieldPopulationSelector.matchesValue(record, "v", "1"));
    assertTrue(FieldPopulationSelector.matchesValue(record, "v", "1.0000001"));
    assertTrue(FieldPopulationSelector.matchesValue(record, "large", "9223372036854775807"));
  }

  @Test
  void rejectsContainersMultipleValuesAndInvalidScalarJson() {
    for (String invalid : List.of("", "[]", "{}", "true false", "null 0", "\"unterminated")) {
      assertThrows(
          IllegalArgumentException.class, () -> FieldPopulationSelector.requireScalarJson(invalid));
    }
    assertEquals("\"a\"", FieldPopulationSelector.requireScalarJson(" \"\\u0061\" "));
    assertEquals("1.0", FieldPopulationSelector.requireScalarJson("1.0"));
    assertEquals("1", FieldPopulationSelector.requireScalarJson("1"));
  }

  @Test
  void visitsParentButDoesNotInventChildPresence() throws Exception {
    String json = "{\"a\":[{},[],{\"b\":[]}]}";
    assertTrue(FieldPopulationSelector.isPresent(record(json), "a.b"));
    assertTrue(FieldPopulationSelector.visitJson(json, "a.b", ignored -> fail()));
    assertFalse(FieldPopulationSelector.isPresent(record(json), "a.c"));
  }

  @Test
  void discoversRawNamesWithNulEmptyPrefixesAndNestedArrays() {
    String json = "{\"a\\u0000b\":1,\"a\\\\u0000b\":2,\"\":{\"x\":[{\"y\":3}]}}";
    assertEquals(
        "a\u0000b", FieldPopulationSelector.findFieldName(json, "a\u0000b"::equals).orElseThrow());
    assertEquals(
        "a\\u0000b",
        FieldPopulationSelector.findFieldName(json, "a\\u0000b"::equals).orElseThrow());
    assertEquals("", FieldPopulationSelector.findFieldName(json, ""::equals).orElseThrow());
    assertEquals("x.y", FieldPopulationSelector.findFieldName(json, "x.y"::equals).orElseThrow());
    assertTrue(FieldPopulationSelector.findFieldName(json, "missing"::equals).isEmpty());
  }

  private static List<String> values(String json, String field) {
    List<String> values = new ArrayList<>();
    FieldPopulationSelector.visitJson(json, field, values::add);
    return values;
  }

  private static FleakData record(String json) throws Exception {
    return FleakData.wrap(JSON.readValue(json, Map.class));
  }
}
