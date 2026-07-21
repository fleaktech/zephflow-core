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
package io.fleak.zephflow.lib.commands.influxdbsink;

import static org.junit.jupiter.api.Assertions.*;

import com.influxdb.client.domain.WritePrecision;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class InfluxDbSinkMessageProcessorTest {

  private static final long TS = 1_700_000_000_000L;

  private static RecordFleakData record(Map<String, Object> m) {
    return (RecordFleakData) FleakData.wrap(m);
  }

  private String lineProtocol(InfluxDbSinkMessageProcessor processor, RecordFleakData event)
      throws Exception {
    return processor.preprocess(event, TS).toLineProtocol();
  }

  @Test
  void mapsScalarFieldsByType() throws Exception {
    var processor =
        new InfluxDbSinkMessageProcessor(
            "m", null, List.of(), List.of("count", "ratio", "name", "ok"), null, WritePrecision.MS);
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("count", 200L);
    payload.put("ratio", 12.5);
    payload.put("name", "abc");
    payload.put("ok", true);

    String lp = lineProtocol(processor, record(payload));

    assertTrue(lp.contains("count=200i"), lp); // long -> integer field
    assertTrue(lp.contains("ratio=12.5"), lp); // double -> float field
    assertTrue(lp.contains("name=\"abc\""), lp); // string -> quoted string field
    assertTrue(lp.contains("ok=true"), lp); // boolean field
    assertTrue(lp.startsWith("m "), lp); // measurement, no tags
  }

  @Test
  void jsonStringifiesNestedFieldValue() throws Exception {
    var processor =
        new InfluxDbSinkMessageProcessor(
            "m", null, List.of(), List.of("meta"), null, WritePrecision.MS);
    var event = record(Map.of("meta", Map.of("k", "v", "n", 3)));

    String lp = lineProtocol(processor, event);

    // nested object becomes a JSON string field
    assertTrue(lp.contains("meta=\""), lp);
    assertTrue(lp.contains("\\\"k\\\":\\\"v\\\""), lp);
  }

  @Test
  void writesTagsAsStrings() throws Exception {
    var processor =
        new InfluxDbSinkMessageProcessor(
            "m", null, List.of("host", "code"), List.of("v"), null, WritePrecision.MS);
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("host", "server-a");
    payload.put("code", 500L); // non-string tag value is coerced to string
    payload.put("v", 1L);

    String lp = lineProtocol(processor, record(payload));

    assertTrue(lp.startsWith("m,"), lp); // has a tag set
    assertTrue(lp.contains("host=server-a"), lp);
    assertTrue(lp.contains("code=500"), lp); // tag, no 'i' suffix
    assertTrue(lp.contains("v=1i"), lp); // still a field
  }

  @Test
  void selectsAllRemainingFieldsWhenFieldFieldsUnset() throws Exception {
    // tags + timestamp excluded; everything else becomes a field
    var processor =
        new InfluxDbSinkMessageProcessor("m", null, List.of("host"), null, "ts", WritePrecision.MS);
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("host", "h"); // tag -> not a field
    payload.put("ts", TS); // timestamp -> not a field
    payload.put("a", 1L);
    payload.put("b", 2L);

    String lp = lineProtocol(processor, record(payload));

    // line protocol: "measurement,<tags> <fields> <timestamp>"
    String tagSection = lp.split(" ")[0];
    String fieldSection = lp.split(" ")[1];
    assertTrue(fieldSection.contains("a=1i"), lp);
    assertTrue(fieldSection.contains("b=2i"), lp);
    assertTrue(tagSection.contains("host=h"), "host should be a tag: " + lp);
    assertFalse(fieldSection.contains("host"), "host should not be a field: " + lp);
  }

  @Test
  void resolvesMeasurementFromField() throws Exception {
    var processor =
        new InfluxDbSinkMessageProcessor(
            null, "type", List.of(), List.of("v"), null, WritePrecision.MS);
    var event = record(Map.of("type", "cpu", "v", 1L));

    String lp = lineProtocol(processor, event);

    assertTrue(lp.startsWith("cpu "), lp);
  }

  @Test
  void usesNumericTimestampFieldAtConfiguredPrecision() throws Exception {
    var processor =
        new InfluxDbSinkMessageProcessor(
            "m", null, List.of(), List.of("v"), "ts", WritePrecision.MS);
    var event = record(Map.of("v", 1L, "ts", 1_699_999_999_000L));

    String lp = lineProtocol(processor, event);

    assertTrue(lp.endsWith(" 1699999999000"), lp);
  }

  @Test
  void parsesIsoStringTimestamp() throws Exception {
    var processor =
        new InfluxDbSinkMessageProcessor(
            "m", null, List.of(), List.of("v"), "ts", WritePrecision.MS);
    var event = record(Map.of("v", 1L, "ts", "2023-11-15T22:13:20Z"));

    String lp = lineProtocol(processor, event);

    assertTrue(lp.endsWith(" 1700086400000"), lp); // 2023-11-15T22:13:20Z in millis
  }

  @Test
  void defaultsToProvidedNowWhenNoTimestampField() throws Exception {
    var processor =
        new InfluxDbSinkMessageProcessor(
            "m", null, List.of(), List.of("v"), null, WritePrecision.MS);
    var event = record(Map.of("v", 1L));

    String lp = lineProtocol(processor, event);

    assertTrue(lp.endsWith(" " + TS), lp);
  }

  @Test
  void throwsWhenNoFieldsResolved() {
    var processor =
        new InfluxDbSinkMessageProcessor(
            "m", null, List.of(), List.of("missing"), null, WritePrecision.MS);
    var event = record(Map.of("other", 1L));

    assertThrows(Exception.class, () -> processor.preprocess(event, TS));
  }

  @Test
  void throwsWhenMeasurementFieldValueMissing() {
    var processor =
        new InfluxDbSinkMessageProcessor(
            null, "type", List.of(), List.of("v"), null, WritePrecision.MS);
    var event = record(Map.of("v", 1L));

    assertThrows(Exception.class, () -> processor.preprocess(event, TS));
  }

  @Test
  void throwsWhenConfiguredTimestampMissing() {
    var processor =
        new InfluxDbSinkMessageProcessor(
            "m", null, List.of(), List.of("v"), "ts", WritePrecision.MS);
    var event = record(Map.of("v", 1L));

    assertThrows(Exception.class, () -> processor.preprocess(event, TS));
  }
}
