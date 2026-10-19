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
package io.fleak.zephflow.lib.serdes.ser.csv;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Created by bolei on 9/17/24 */
class CsvTypedSerializerTest {
  @Test
  void testSerializeToMultipleTypedEvent_withValidData() throws Exception {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> event1 = new LinkedHashMap<>();
    event1.put("id", 1);
    event1.put("name", "Alice");
    event1.put("age", 30);

    Map<String, Object> event2 = new LinkedHashMap<>();
    event2.put("id", 2);
    event2.put("name", "Bob");
    event2.put("city", "New York");

    events.add(event1);
    events.add(event2);

    CsvTypedSerializer serializer = new CsvTypedSerializer();

    byte[] csvBytes = serializer.serializeToMultipleTypedEvent(events);

    String csvOutput = new String(csvBytes, StandardCharsets.UTF_8);

    // Expected CSV content with CRLF line endings
    String expectedCsv =
        """
        id,name,age,city\r
        1,Alice,30,\r
        2,Bob,,New York\r
        """;

    // Verify the generated CSV matches the expected output
    assertEquals(expectedCsv, csvOutput);
  }

  @Test
  void testSerializeToMultipleTypedEvent_withEmptyList() throws Exception {
    // Create empty list of events
    List<Map<String, Object>> events = new ArrayList<>();

    // Create instance of CsvTypedSerializer
    CsvTypedSerializer serializer = new CsvTypedSerializer();

    // Call the method under test
    byte[] csvBytes = serializer.serializeToMultipleTypedEvent(events);

    // Convert byte array to string for validation
    String csvOutput = new String(csvBytes, StandardCharsets.UTF_8);

    // Expected output for an empty list is just an empty CSV (no header)
    assertTrue(csvOutput.isEmpty());
  }

  @Test
  void testSerializeToMultipleTypedEvent_withMixedKeys() throws Exception {
    List<Map<String, Object>> events = new ArrayList<>();

    Map<String, Object> event1 = new LinkedHashMap<>();
    event1.put("id", 1);
    event1.put("name", "Alice");

    Map<String, Object> event2 = new LinkedHashMap<>();
    event2.put("age", 25);
    event2.put("city", "London");

    events.add(event1);
    events.add(event2);

    // Create instance of CsvTypedSerializer
    CsvTypedSerializer serializer = new CsvTypedSerializer();

    // Call the method under test
    byte[] csvBytes = serializer.serializeToMultipleTypedEvent(events);

    // Convert byte array to string for validation
    String csvOutput = new String(csvBytes, StandardCharsets.UTF_8);

    // Expected CSV content
    String expectedCsv =
        """
                id,name,age,city\r
                1,Alice,,\r
                ,,25,London\r
                """;

    // Verify the generated CSV matches the expected output
    assertEquals(expectedCsv, csvOutput);
  }

  @Test
  void testPinnedColumns_fixesOrderDropsExtrasAndBlanksMissing() throws Exception {
    Map<String, Object> event1 = new LinkedHashMap<>();
    event1.put("action", "allow");
    event1.put("src_ip", "1.2.3.4");
    event1.put("time", "2026-10-19");
    event1.put("dst_ip", "5.6.7.8");
    event1.put("extra", "dropped");

    Map<String, Object> event2 = new LinkedHashMap<>();
    event2.put("time", "2026-10-19");
    event2.put("src_ip", "1.2.3.9");
    event2.put("dst_ip", null);
    event2.put("action", "deny");

    CsvTypedSerializer serializer =
        new CsvTypedSerializer(
            new CsvSerializerOptions(List.of("time", "src_ip", "dst_ip", "action"), true));

    String csvOutput =
        new String(
            serializer.serializeToMultipleTypedEvent(List.of(event1, event2)),
            StandardCharsets.UTF_8);

    String expectedCsv =
        """
        time,src_ip,dst_ip,action\r
        2026-10-19,1.2.3.4,5.6.7.8,allow\r
        2026-10-19,1.2.3.9,,deny\r
        """;
    assertEquals(expectedCsv, csvOutput);
  }

  @Test
  void testPinnedColumns_withoutHeader() throws Exception {
    Map<String, Object> event = new LinkedHashMap<>();
    event.put("b", 2);
    event.put("a", 1);

    CsvTypedSerializer serializer =
        new CsvTypedSerializer(new CsvSerializerOptions(List.of("a", "b", "c"), false));

    String csvOutput =
        new String(
            serializer.serializeToMultipleTypedEvent(List.of(event)), StandardCharsets.UTF_8);

    assertEquals("1,2,\r\n", csvOutput);
  }

  @Test
  void testPinnedColumns_withEmptyList() throws Exception {
    CsvTypedSerializer serializer =
        new CsvTypedSerializer(new CsvSerializerOptions(List.of("a", "b"), true));

    assertEquals(0, serializer.serializeToMultipleTypedEvent(List.of()).length);
  }

  @Test
  void testEmptyColumnList_fallsBackToDerivedColumns() throws Exception {
    Map<String, Object> event = new LinkedHashMap<>();
    event.put("id", 1);
    event.put("name", "Alice");

    CsvTypedSerializer serializer =
        new CsvTypedSerializer(new CsvSerializerOptions(List.of(), true));

    String csvOutput =
        new String(
            serializer.serializeToMultipleTypedEvent(List.of(event)), StandardCharsets.UTF_8);

    assertEquals("id,name\r\n1,Alice\r\n", csvOutput);
  }

  @Test
  void testOptions_rejectBlankAndDuplicateColumns() {
    assertThrows(
        IllegalArgumentException.class, () -> new CsvSerializerOptions(List.of("a", " "), true));
    assertThrows(
        IllegalArgumentException.class, () -> new CsvSerializerOptions(List.of("a", "a"), true));
    assertThrows(
        IllegalArgumentException.class, () -> new CsvSerializerOptions(List.of("a", " a "), true));
    assertThrows(
        IllegalArgumentException.class,
        () -> new CsvSerializerOptions(Arrays.asList("a", null), true));
  }

  @Test
  void testOptions_stripsUnicodeWhitespace() {
    CsvSerializerOptions options =
        new CsvSerializerOptions(List.of("\u00A0src_ip\u2003", "\uFEFFtime\u200B"), true);

    assertEquals(List.of("src_ip", "time"), options.columns());
  }

  @Test
  void testOptions_removesInteriorFormatCharsButKeepsSpaces() {
    CsvSerializerOptions options =
        new CsvSerializerOptions(List.of("src\u200B_ip", "dst ip"), true);

    assertEquals(List.of("src_ip", "dst ip"), options.columns());
    assertThrows(
        IllegalArgumentException.class,
        () -> new CsvSerializerOptions(List.of("a", "\u200B\u2003"), true));
  }

  @Test
  void testOptions_headerOffRequiresColumns() {
    assertThrows(IllegalArgumentException.class, () -> new CsvSerializerOptions(null, false));
    assertThrows(IllegalArgumentException.class, () -> new CsvSerializerOptions(List.of(), false));
  }

  @Test
  void testPinnedColumns_namesAreTrimmed() throws Exception {
    Map<String, Object> event = new LinkedHashMap<>();
    event.put("time", "2026-10-19");
    event.put("src_ip", "1.2.3.4");

    CsvSerializerOptions options = new CsvSerializerOptions(List.of("time", " src_ip "), false);
    CsvTypedSerializer serializer = new CsvTypedSerializer(options);

    assertEquals(List.of("time", "src_ip"), options.columns());
    assertEquals(
        "2026-10-19,1.2.3.4\r\n",
        new String(
            serializer.serializeToMultipleTypedEvent(List.of(event)), StandardCharsets.UTF_8));
  }

  @Test
  void testNullOptions_fallBackToDefault() throws Exception {
    Map<String, Object> event = new LinkedHashMap<>();
    event.put("id", 1);

    CsvTypedSerializer serializer = new CsvTypedSerializer(null);

    assertEquals(
        "id\r\n1\r\n",
        new String(
            serializer.serializeToMultipleTypedEvent(List.of(event)), StandardCharsets.UTF_8));
  }

  @Test
  void testPinnedColumns_withoutHeader_escapesSpecialCharacters() throws Exception {
    Map<String, Object> event = new LinkedHashMap<>();
    event.put("city", "New, York");
    event.put("msg", "say \"hi\"\nbye");
    event.put("id", 1);

    CsvTypedSerializer serializer =
        new CsvTypedSerializer(new CsvSerializerOptions(List.of("id", "city", "msg"), false));

    String csvOutput =
        new String(
            serializer.serializeToMultipleTypedEvent(List.of(event)), StandardCharsets.UTF_8);

    assertEquals("1,\"New, York\",\"say \"\"hi\"\"\nbye\"\r\n", csvOutput);
  }

  @Test
  void testOptions_copiesColumnList() {
    List<String> columns = new ArrayList<>(List.of("a", "b"));
    CsvSerializerOptions options = new CsvSerializerOptions(columns, false);

    columns.add("c");

    assertEquals(List.of("a", "b"), options.columns());
    assertThrows(UnsupportedOperationException.class, () -> options.columns().add("d"));
  }

  @Test
  void testSinglePinnedColumn_missingValueWritesQuotedEmpty() throws Exception {
    Map<String, Object> present = new LinkedHashMap<>();
    present.put("x", "1");
    Map<String, Object> missing = new LinkedHashMap<>();
    Map<String, Object> empty = new LinkedHashMap<>();
    empty.put("x", "");

    CsvTypedSerializer serializer =
        new CsvTypedSerializer(new CsvSerializerOptions(List.of("x"), false));

    String csvOutput =
        new String(
            serializer.serializeToMultipleTypedEvent(List.of(present, missing, empty)),
            StandardCharsets.UTF_8);

    assertEquals("1\r\n\"\"\r\n\"\"\r\n", csvOutput);
  }

  @Test
  void testPinnedColumns_multiColumnMissingFirstValueStaysUnquoted() throws Exception {
    Map<String, Object> event = new LinkedHashMap<>();
    event.put("src_ip", "1.2.3.4");

    CsvTypedSerializer serializer =
        new CsvTypedSerializer(new CsvSerializerOptions(List.of("user", "src_ip"), false));

    String csvOutput =
        new String(
            serializer.serializeToMultipleTypedEvent(List.of(event)), StandardCharsets.UTF_8);

    assertEquals(",1.2.3.4\r\n", csvOutput);
  }
}
