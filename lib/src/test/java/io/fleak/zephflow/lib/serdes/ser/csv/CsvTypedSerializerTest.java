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
}
