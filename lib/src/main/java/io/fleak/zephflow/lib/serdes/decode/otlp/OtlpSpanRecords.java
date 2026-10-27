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
 * Flattens an OTLP/JSON traces payload into one flat map per span.
 *
 * <p>{@code events} and {@code links} nest rather than fanning out: the payload's leaf is the span,
 * and fanning them out would force either duplicating the span envelope onto every event or losing
 * the event-to-span binding.
 */
final class OtlpSpanRecords {

  /** {@code SpanKind} by proto number. */
  private static final List<String> SPAN_KINDS =
      List.of(
          "SPAN_KIND_UNSPECIFIED",
          "SPAN_KIND_INTERNAL",
          "SPAN_KIND_SERVER",
          "SPAN_KIND_CLIENT",
          "SPAN_KIND_PRODUCER",
          "SPAN_KIND_CONSUMER");

  /** {@code Status.StatusCode} by proto number. */
  private static final List<String> STATUS_CODES =
      List.of("STATUS_CODE_UNSET", "STATUS_CODE_OK", "STATUS_CODE_ERROR");

  private OtlpSpanRecords() {}

  static List<Map<String, Object>> flatten(Map<String, Object> payload) {
    List<Map<String, Object>> records = new ArrayList<>();
    for (Map<String, Object> resourceSpans : mapList(payload.get("resourceSpans"))) {
      for (Map<String, Object> scopeSpans : mapList(resourceSpans.get("scopeSpans"))) {
        OtlpEnvelope envelope = OtlpEnvelope.of(resourceSpans, scopeSpans);
        for (Map<String, Object> span : mapList(scopeSpans.get("spans"))) {
          records.add(toRecord(span, envelope));
        }
      }
    }
    return records;
  }

  static Map<String, Object> toRecord(Map<String, Object> span, OtlpEnvelope envelope) {
    Map<String, Object> record = new LinkedHashMap<>();

    put(record, "traceId", span.get("traceId"));
    put(record, "spanId", span.get("spanId"));
    put(record, "traceState", span.get("traceState"));
    put(record, "parentSpanId", span.get("parentSpanId"));
    put(record, "flags", span.get("flags"));
    put(record, "name", span.get("name"));
    put(record, "kind", enumName(span.get("kind"), SPAN_KINDS));

    String startTimeUnixNano = int64String(span.get("startTimeUnixNano"));
    put(record, "startTimeUnixNano", startTimeUnixNano);
    put(record, "startTimeMillis", millis(startTimeUnixNano));
    String endTimeUnixNano = int64String(span.get("endTimeUnixNano"));
    put(record, "endTimeUnixNano", endTimeUnixNano);
    put(record, "endTimeMillis", millis(endTimeUnixNano));

    putIfNotEmpty(record, "attributes", attributes(span.get("attributes")));
    put(record, "droppedAttributesCount", span.get("droppedAttributesCount"));

    putIfNotEmpty(record, "events", events(span.get("events")));
    put(record, "droppedEventsCount", span.get("droppedEventsCount"));

    putIfNotEmpty(record, "links", links(span.get("links")));
    put(record, "droppedLinksCount", span.get("droppedLinksCount"));

    Map<String, Object> status = status(span.get("status"));
    if (status != null) {
      record.put("status", status);
    }

    envelope.applyTo(record);
    return record;
  }

  private static List<Map<String, Object>> events(Object raw) {
    List<Map<String, Object>> events = new ArrayList<>();
    for (Map<String, Object> rawEvent : mapList(raw)) {
      Map<String, Object> event = new LinkedHashMap<>();
      String timeUnixNano = int64String(rawEvent.get("timeUnixNano"));
      put(event, "timeUnixNano", timeUnixNano);
      put(event, "timeMillis", millis(timeUnixNano));
      put(event, "name", rawEvent.get("name"));
      putIfNotEmpty(event, "attributes", attributes(rawEvent.get("attributes")));
      put(event, "droppedAttributesCount", rawEvent.get("droppedAttributesCount"));
      if (!event.isEmpty()) {
        events.add(event);
      }
    }
    return events;
  }

  private static List<Map<String, Object>> links(Object raw) {
    List<Map<String, Object>> links = new ArrayList<>();
    for (Map<String, Object> rawLink : mapList(raw)) {
      Map<String, Object> link = new LinkedHashMap<>();
      put(link, "traceId", rawLink.get("traceId"));
      put(link, "spanId", rawLink.get("spanId"));
      put(link, "traceState", rawLink.get("traceState"));
      putIfNotEmpty(link, "attributes", attributes(rawLink.get("attributes")));
      put(link, "droppedAttributesCount", rawLink.get("droppedAttributesCount"));
      put(link, "flags", rawLink.get("flags"));
      if (!link.isEmpty()) {
        links.add(link);
      }
    }
    return links;
  }

  /** Null when the span carried no status block; {@code code} omitted within it when UNSET. */
  private static Map<String, Object> status(Object raw) {
    Map<String, Object> rawStatus = map(raw);
    if (rawStatus == null || rawStatus.isEmpty()) {
      return null;
    }
    Map<String, Object> status = new LinkedHashMap<>();
    put(status, "code", enumName(rawStatus.get("code"), STATUS_CODES));
    put(status, "message", rawStatus.get("message"));
    return status.isEmpty() ? null : status;
  }
}
