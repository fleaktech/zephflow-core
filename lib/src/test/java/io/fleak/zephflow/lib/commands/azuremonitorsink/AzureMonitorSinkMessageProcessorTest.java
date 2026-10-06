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
package io.fleak.zephflow.lib.commands.azuremonitorsink;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AzureMonitorSinkMessageProcessorTest {

  private static final String EVENT_TIME = "2026-09-28T18:30:00Z";

  private Map<String, Object> process(String timeGeneratedField, Map<String, Object> record)
      throws Exception {
    var processor = new AzureMonitorSinkMessageProcessor(timeGeneratedField);
    var event = processor.preprocess((RecordFleakData) FleakData.wrap(record), 0L);
    return OBJECT_MAPPER.readValue(event.jsonPayload(), Map.class);
  }

  @Test
  void configuredFieldPopulatesTimeGenerated() throws Exception {
    var payload = process("event_time", Map.of("event_time", EVENT_TIME, "msg", "hello"));

    assertEquals(EVENT_TIME, payload.get("TimeGenerated"));
    assertEquals(EVENT_TIME, payload.get("event_time"));
    assertEquals("hello", payload.get("msg"));
  }

  @Test
  void defaultFieldKeepsExistingTimeGenerated() throws Exception {
    var payload = process("TimeGenerated", Map.of("TimeGenerated", EVENT_TIME));

    assertEquals(EVENT_TIME, payload.get("TimeGenerated"));
  }

  @Test
  void missingConfiguredFieldStampsIngestionTime() throws Exception {
    Instant before = Instant.now();
    var payload = process("event_time", Map.of("msg", "hello"));

    Instant stamped = Instant.parse((String) payload.get("TimeGenerated"));
    assertFalse(stamped.isBefore(before));
    assertFalse(payload.containsKey("event_time"));
  }

  @Test
  void missingConfiguredFieldKeepsExistingTimeGenerated() throws Exception {
    var payload = process("event_time", Map.of("TimeGenerated", EVENT_TIME));

    assertEquals(EVENT_TIME, payload.get("TimeGenerated"));
  }
}
