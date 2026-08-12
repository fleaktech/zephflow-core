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
package io.fleak.zephflow.lib.serdes.des.jsonobjline;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializationOutcome;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import org.junit.jupiter.api.Test;

class JsonObjectLineTypedDeserializerTest {

  private final FleakDeserializer<?> deserializer =
      new JsonObjectLineDeserializerFactory().createDeserializer();

  private static SerializedEvent event(String payload) {
    return new SerializedEvent(null, payload.getBytes(StandardCharsets.UTF_8), null);
  }

  private static List<String> fieldA(List<RecordFleakData> records) {
    return records.stream().map(r -> String.valueOf(r.unwrap().get("a"))).toList();
  }

  @Test
  void objectPerLine_oneEventPerLine() throws Exception {
    List<RecordFleakData> records = deserializer.deserialize(event("{\"a\":1}\n{\"a\":2}"));
    assertEquals(List.of("1", "2"), fieldA(records));
  }

  @Test
  void arrayPerLine_isFlattenedIntoOneEventPerElement() throws Exception {
    List<RecordFleakData> records =
        deserializer.deserialize(
            event(
                """
                [{"device_id":"sensor-119","value":69.4},{"device_id":"sensor-111","value":64.9}]
                [{"device_id":"sensor-102","value":62.7}]
                """));
    assertEquals(
        List.of("sensor-119", "sensor-111", "sensor-102"),
        records.stream().map(r -> r.unwrap().get("device_id")).toList());
  }

  @Test
  void mixedObjectAndArrayLines_bothParse() throws Exception {
    List<RecordFleakData> records =
        deserializer.deserialize(event("{\"a\":1}\n[{\"a\":2},{\"a\":3}]"));
    assertEquals(3, records.size());
  }

  @Test
  void blankLinesAreIgnored() throws Exception {
    List<RecordFleakData> records =
        deserializer.deserialize(event("{\"a\":1}\n\n   \n{\"a\":2}\n"));
    assertEquals(2, records.size());
  }

  @Test
  void nonObjectLine_reportsWhatItGotInsteadOfAClassCastException() {
    Exception exception =
        assertThrows(Exception.class, () -> deserializer.deserialize(event("{\"a\":1}\n42")));
    assertFalse(exception instanceof ClassCastException, "should not surface a raw cast failure");
    assertTrue(
        exception.getMessage().contains("NUMBER"),
        "message should name the offending node type, got: " + exception.getMessage());
  }

  @Test
  void arrayOfNonObjects_isReported() {
    Exception exception =
        assertThrows(Exception.class, () -> deserializer.deserialize(event("[{\"a\":1},7]")));
    assertTrue(
        exception.getMessage().contains("element 1"),
        "message should locate the offending element, got: " + exception.getMessage());
  }

  @Test
  void deserializeWithErrors_keepsGoodLinesAndReportsBadOnesByLineNumber() {
    // line 3 is truncated, the rest are well-formed
    DeserializationOutcome outcome =
        deserializer.deserializeWithErrors(
            event(
                """
                [{"a":1},{"a":2}]
                [{"a":3}]
                [{"a":4},
                [{"a":5}]
                """));

    assertEquals(List.of("1", "2", "3", "5"), fieldA(outcome.records()));
    assertTrue(outcome.hasErrors());
    assertEquals(1, outcome.errors().size());

    DeserializationOutcome.RecordError error = outcome.errors().getFirst();
    assertEquals(3, error.recordIndex(), "should report the 1-based line number");
    assertEquals(
        "[{\"a\":4},",
        new String(error.rawRecord(), StandardCharsets.UTF_8),
        "should quarantine the raw failing line");
  }

  /** The file shape from the original fs_source failure report: 25 array lines, 75 objects. */
  @Test
  void realWorldArrayPerLineFile_parsesEveryObject() throws Exception {
    byte[] payload;
    try (var in = getClass().getResourceAsStream("/serdes/json_array_per_line_input.jsonl")) {
      payload = Objects.requireNonNull(in).readAllBytes();
    }
    List<RecordFleakData> records =
        deserializer.deserialize(new SerializedEvent(null, payload, null));
    assertEquals(75, records.size());
    assertEquals("sensor-119", records.getFirst().unwrap().get("device_id"));
    assertEquals("sensor-112", records.getLast().unwrap().get("device_id"));
  }

  @Test
  void deserializeWithErrors_onCleanPayload_reportsNoErrors() {
    DeserializationOutcome outcome = deserializer.deserializeWithErrors(event("[{\"a\":1}]"));
    assertEquals(1, outcome.records().size());
    assertFalse(outcome.hasErrors());
  }
}
