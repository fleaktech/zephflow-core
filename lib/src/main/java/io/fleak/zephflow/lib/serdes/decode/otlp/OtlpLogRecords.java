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
package io.fleak.zephflow.lib.serdes.decode.otlp;

import static io.fleak.zephflow.lib.serdes.decode.otlp.OtlpJson.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Flattens an OTLP/JSON logs payload into one flat map per log record.
 *
 * <p>{@code severityNumber} stays numeric, unlike the enums the other walkers normalize to names.
 * OTLP defines it as an ordered scale where the number is semantically comparable ({@code >= 17} is
 * an error), so a name would destroy an ordering the spec guarantees — and {@code severityText}
 * already carries the human form.
 */
final class OtlpLogRecords {

  private OtlpLogRecords() {}

  static List<Map<String, Object>> flatten(Map<String, Object> payload) {
    List<Map<String, Object>> records = new ArrayList<>();
    for (Map<String, Object> resourceLogs : mapList(payload.get("resourceLogs"))) {
      for (Map<String, Object> scopeLogs : mapList(resourceLogs.get("scopeLogs"))) {
        OtlpEnvelope envelope = OtlpEnvelope.of(resourceLogs, scopeLogs);
        for (Map<String, Object> logRecord : mapList(scopeLogs.get("logRecords"))) {
          records.add(toRecord(logRecord, envelope));
        }
      }
    }
    return records;
  }

  static Map<String, Object> toRecord(Map<String, Object> logRecord, OtlpEnvelope envelope) {
    Map<String, Object> record = new LinkedHashMap<>();

    String timeUnixNano = int64String(logRecord.get("timeUnixNano"));
    put(record, "timeUnixNano", timeUnixNano);
    put(record, "timeMillis", millis(timeUnixNano));
    String observedTimeUnixNano = int64String(logRecord.get("observedTimeUnixNano"));
    put(record, "observedTimeUnixNano", observedTimeUnixNano);
    put(record, "observedTimeMillis", millis(observedTimeUnixNano));
    put(record, "severityNumber", logRecord.get("severityNumber"));
    put(record, "severityText", logRecord.get("severityText"));
    put(record, "body", anyValue(logRecord.get("body")));
    put(record, "traceId", logRecord.get("traceId"));
    put(record, "spanId", logRecord.get("spanId"));
    // Low 8 bits are the W3C trace flags; bit 0 is "sampled", without which a log record cannot be
    // joined to the trace it names.
    put(record, "flags", logRecord.get("flags"));
    put(record, "droppedAttributesCount", logRecord.get("droppedAttributesCount"));

    putIfNotEmpty(record, "attributes", attributes(logRecord.get("attributes")));
    envelope.applyTo(record);
    return record;
  }
}
