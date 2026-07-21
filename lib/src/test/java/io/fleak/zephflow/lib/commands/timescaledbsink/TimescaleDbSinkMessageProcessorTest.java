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
package io.fleak.zephflow.lib.commands.timescaledbsink;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class TimescaleDbSinkMessageProcessorTest {

  private static RecordFleakData record(Map<String, Object> m) {
    return (RecordFleakData) FleakData.wrap(m);
  }

  @Test
  void coercesNumericEpochMillisTimeColumnToUtcTimestamp() throws Exception {
    var processor = new TimescaleDbSinkMessageProcessor("ts", TimeUnit.MILLISECONDS);
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("ts", 1_700_000_000_000L);
    payload.put("host", "a");
    payload.put("val", 12.5);

    Map<String, Object> row = processor.preprocess(record(payload), 0L);

    assertEquals(
        OffsetDateTime.ofInstant(
            java.time.Instant.ofEpochMilli(1_700_000_000_000L), ZoneOffset.UTC),
        row.get("ts"));
    assertEquals("a", row.get("host"));
    assertEquals(12.5, row.get("val"));
  }

  @Test
  void coercesNumericEpochSecondsWhenTimeUnitIsSeconds() throws Exception {
    var processor = new TimescaleDbSinkMessageProcessor("ts", TimeUnit.SECONDS);
    var row = processor.preprocess(record(Map.of("ts", 1_700_000_000L, "val", 1.0)), 0L);

    assertEquals(
        OffsetDateTime.ofInstant(java.time.Instant.ofEpochSecond(1_700_000_000L), ZoneOffset.UTC),
        row.get("ts"));
  }

  @Test
  void parsesIsoStringTimeColumn() throws Exception {
    var processor = new TimescaleDbSinkMessageProcessor("ts", TimeUnit.MILLISECONDS);
    var row = processor.preprocess(record(Map.of("ts", "2023-11-15T22:13:20Z", "val", 1.0)), 0L);

    assertEquals(OffsetDateTime.of(2023, 11, 15, 22, 13, 20, 0, ZoneOffset.UTC), row.get("ts"));
  }

  @Test
  void parsesIsoStringWithOffset() throws Exception {
    var processor = new TimescaleDbSinkMessageProcessor("ts", TimeUnit.MILLISECONDS);
    var row =
        processor.preprocess(record(Map.of("ts", "2023-11-15T22:13:20+02:00", "val", 1.0)), 0L);

    assertEquals(OffsetDateTime.of(2023, 11, 15, 20, 13, 20, 0, ZoneOffset.UTC), row.get("ts"));
  }

  @Test
  void throwsWhenTimeColumnMissing() {
    var processor = new TimescaleDbSinkMessageProcessor("ts", TimeUnit.MILLISECONDS);
    assertThrows(Exception.class, () -> processor.preprocess(record(Map.of("val", 1.0)), 0L));
  }

  @Test
  void throwsWhenTimeColumnUnparseable() {
    var processor = new TimescaleDbSinkMessageProcessor("ts", TimeUnit.MILLISECONDS);
    assertThrows(
        Exception.class,
        () -> processor.preprocess(record(Map.of("ts", "not-a-timestamp", "val", 1.0)), 0L));
  }
}
