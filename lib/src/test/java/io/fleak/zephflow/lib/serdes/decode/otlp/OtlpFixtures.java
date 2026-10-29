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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.IOException;
import java.io.InputStream;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The fixtures zephflow-plus otel_source is tested with, and the records they decode to. */
public final class OtlpFixtures {

  private static final String TRACE_ID = "5b8efff798038103d269b633813fc60c";
  private static final String SPAN_ID = "eee19b7ec3c1b174";

  private OtlpFixtures() {}

  public static Map<String, Object> json(String name) throws IOException {
    return JsonUtils.fromJsonResource("/otlp/" + name, new TypeReference<>() {});
  }

  /**
   * The fixture as protobuf. OTLP/JSON writes trace and span ids as hex, where protobuf's JSON
   * reader expects base64.
   */
  public static <T extends Message> T protobuf(String name, Message.Builder builder)
      throws IOException {
    JsonNode tree;
    try (InputStream in = OtlpFixtures.class.getResourceAsStream("/otlp/" + name)) {
      tree = JsonUtils.OBJECT_MAPPER.readTree(in);
    }
    convertIds(tree, true);
    JsonFormat.parser().merge(tree.toString(), builder);
    @SuppressWarnings("unchecked")
    T message = (T) builder.build();
    return message;
  }

  /** The message as protobuf's own JSON printer writes it, with ids in OTLP/JSON's hex. */
  public static Map<String, Object> printedAsOtlpJson(Message message) throws IOException {
    JsonNode tree =
        JsonUtils.OBJECT_MAPPER.readTree(JsonFormat.printer().printingEnumsAsInts().print(message));
    convertIds(tree, false);
    return JsonUtils.OBJECT_MAPPER.convertValue(tree, new TypeReference<>() {});
  }

  private static void convertIds(JsonNode node, boolean hexToBase64) {
    if (node instanceof ObjectNode object) {
      for (Iterator<Map.Entry<String, JsonNode>> fields = object.fields(); fields.hasNext(); ) {
        Map.Entry<String, JsonNode> field = fields.next();
        String key = field.getKey();
        if ((key.equals("traceId") || key.equals("spanId") || key.equals("parentSpanId"))
            && field.getValue().isTextual()) {
          String value = field.getValue().asText();
          object.put(
              key,
              hexToBase64
                  ? Base64.getEncoder().encodeToString(HexFormat.of().parseHex(value))
                  : HexFormat.of().formatHex(Base64.getDecoder().decode(value)));
        } else {
          convertIds(field.getValue(), hexToBase64);
        }
      }
    } else if (node != null && node.isArray()) {
      node.forEach(item -> convertIds(item, hexToBase64));
    }
  }

  /** Records as a pipeline sees them: numbers widened to Long or Double. */
  public static List<Map<String, Object>> asPipelineSeesThem(List<Map<String, Object>> records) {
    return records.stream()
        .map(record -> ((RecordFleakData) FleakData.wrap(record)).unwrap())
        .toList();
  }

  public static Map<String, Object> map(Object... keysAndValues) {
    Map<String, Object> map = new LinkedHashMap<>();
    for (int i = 0; i < keysAndValues.length; i += 2) {
      map.put((String) keysAndValues[i], keysAndValues[i + 1]);
    }
    return map;
  }

  public static List<Map<String, Object>> logs() {
    Map<String, Object> checkout =
        map("service.name", "checkout", "service.version", "1.4.2", "process.pid", 4211L);
    Map<String, Object> example =
        map(
            "name",
            "io.opentelemetry.example",
            "version",
            "1.0.0",
            "attributes",
            map("scope.tier", "backend"));
    return List.of(
        map(
            "timeUnixNano",
            "1750000000000000123",
            "timeMillis",
            1750000000000L,
            "observedTimeUnixNano",
            "1750000000000000456",
            "observedTimeMillis",
            1750000000000L,
            "severityNumber",
            9L,
            "severityText",
            "INFO",
            "body",
            "order placed",
            "traceId",
            TRACE_ID,
            "spanId",
            SPAN_ID,
            "flags",
            1L,
            "attributes",
            map(
                "order.id",
                "A-1001",
                "order.total",
                42.5,
                "order.items",
                3L,
                "order.expedited",
                true,
                "order.tags",
                List.of("gift", "priority"),
                "order.customer",
                map("id", "C-77", "vip", false)),
            "resource",
            checkout,
            "scope",
            example),
        map(
            "timeUnixNano",
            "1750000000000000789",
            "timeMillis",
            1750000000000L,
            "severityNumber",
            17L,
            "severityText",
            "ERROR",
            "body",
            "payment declined",
            "attributes",
            map("order.id", "A-1002"),
            "resource",
            checkout,
            "scope",
            example),
        map(
            "timeUnixNano",
            "1750000000000000999",
            "timeMillis",
            1750000000000L,
            "severityNumber",
            5L,
            "severityText",
            "DEBUG",
            "body",
            "cache warm",
            "resource",
            checkout,
            "scope",
            map("name", "io.opentelemetry.other")),
        map(
            "timeUnixNano",
            "1750000000000001111",
            "timeMillis",
            1750000000000L,
            "severityNumber",
            13L,
            "severityText",
            "WARN",
            "body",
            "carrier slow",
            "resource",
            map("service.name", "shipping")));
  }

  /**
   * {@code withZeroThreshold}: the fixture writes {@code "zeroThreshold": 0.0}; protobuf cannot
   * carry a value equal to its default, so the same metric from protobuf has no such field.
   */
  public static List<Map<String, Object>> metrics(boolean withZeroThreshold) {
    Map<String, Object> resource = map("service.name", "checkout");
    Map<String, Object> scope = map("name", "io.fleak.demo.metrics", "version", "1.0.0");
    Map<String, Object> exponential =
        map(
            "metricType",
            "exponentialHistogram",
            "name",
            "request.duration.exp",
            "startTimeUnixNano",
            "1749999000000000000",
            "startTimeMillis",
            1749999000000L,
            "timeUnixNano",
            "1750000000000000000",
            "timeMillis",
            1750000000000L,
            "resource",
            resource,
            "scope",
            scope,
            "count",
            "6",
            "sum",
            30.0,
            "scale",
            -2L,
            "zeroCount",
            "3",
            "zeroThreshold",
            0.0,
            "positive",
            map("offset", 4L, "bucketCounts", List.of("1", "0", "2")),
            "aggregationTemporality",
            "AGGREGATION_TEMPORALITY_DELTA");
    if (!withZeroThreshold) {
      exponential.remove("zeroThreshold");
    }
    return List.of(
        map(
            "metricType",
            "gauge",
            "name",
            "queue.depth",
            "description",
            "Items awaiting processing",
            "unit",
            "{item}",
            "timeUnixNano",
            "1750000000000000000",
            "timeMillis",
            1750000000000L,
            "attributes",
            map("queue", "orders"),
            "resource",
            resource,
            "scope",
            scope,
            "asDouble",
            3.5),
        map(
            "metricType", "gauge",
            "name", "queue.depth",
            "description", "Items awaiting processing",
            "unit", "{item}",
            "timeUnixNano", "1750000000000000000",
            "timeMillis", 1750000000000L,
            "resource", resource,
            "scope", scope,
            "asInt", "9007199254740993"),
        map(
            "metricType", "sum",
            "name", "requests.total",
            "unit", "{request}",
            "startTimeUnixNano", "1749999000000000000",
            "startTimeMillis", 1749999000000L,
            "timeUnixNano", "1750000000000000000",
            "timeMillis", 1750000000000L,
            "resource", resource,
            "scope", scope,
            "asInt", "421",
            "aggregationTemporality", "AGGREGATION_TEMPORALITY_CUMULATIVE",
            "isMonotonic", true),
        map(
            "metricType",
            "sum",
            "name",
            "requests.delta",
            "startTimeUnixNano",
            "1749999000000000000",
            "startTimeMillis",
            1749999000000L,
            "timeUnixNano",
            "1750000000000000000",
            "timeMillis",
            1750000000000L,
            "resource",
            resource,
            "scope",
            scope,
            "asDouble",
            0.0,
            "aggregationTemporality",
            "AGGREGATION_TEMPORALITY_DELTA"),
        map(
            "metricType",
            "histogram",
            "name",
            "request.duration",
            "unit",
            "ms",
            "startTimeUnixNano",
            "1749999000000000000",
            "startTimeMillis",
            1749999000000L,
            "timeUnixNano",
            "1750000000000000000",
            "timeMillis",
            1750000000000L,
            "exemplars",
            List.of(
                map(
                    "filteredAttributes",
                    map("http.route", "/checkout"),
                    "timeUnixNano",
                    "1750000000000000000",
                    "timeMillis",
                    1750000000000L,
                    "asDouble",
                    8.75,
                    "spanId",
                    SPAN_ID,
                    "traceId",
                    TRACE_ID)),
            "resource",
            resource,
            "scope",
            scope,
            "count",
            "7",
            "sum",
            12.5,
            "min",
            0.5,
            "max",
            9.0,
            "bucketCounts",
            List.of("1", "2", "4"),
            "explicitBounds",
            List.of(1.0, 5.0),
            "aggregationTemporality",
            "AGGREGATION_TEMPORALITY_CUMULATIVE"),
        exponential,
        map(
            "metricType",
            "summary",
            "name",
            "request.duration.summary",
            "startTimeUnixNano",
            "1749999000000000000",
            "startTimeMillis",
            1749999000000L,
            "timeUnixNano",
            "1750000000000000000",
            "timeMillis",
            1750000000000L,
            "resource",
            resource,
            "scope",
            scope,
            "count",
            "10",
            "sum",
            99.5,
            "quantileValues",
            List.of(
                map("value", 0.1),
                map("quantile", 0.5, "value", 1.0),
                map("quantile", 0.99, "value", 9.0))));
  }

  public static List<Map<String, Object>> traces() {
    Map<String, Object> checkout = map("service.name", "checkout", "process.pid", 4211L);
    String schemaUrl = "https://opentelemetry.io/schemas/1.24.0";
    Map<String, Object> checkoutScope =
        map(
            "name",
            "io.fleak.demo.checkout",
            "version",
            "1.0.0",
            "attributes",
            map("scope.tier", "backend"));
    return List.of(
        map(
            "traceId",
            TRACE_ID,
            "spanId",
            SPAN_ID,
            "traceState",
            "fleak=1",
            "name",
            "POST /checkout",
            "kind",
            "SPAN_KIND_SERVER",
            "startTimeUnixNano",
            "1750000000000000123",
            "startTimeMillis",
            1750000000000L,
            "endTimeUnixNano",
            "1750000000500000456",
            "endTimeMillis",
            1750000000500L,
            "attributes",
            map("http.route", "/checkout", "http.status_code", 500L),
            "droppedAttributesCount",
            1L,
            "events",
            List.of(
                map(
                    "timeUnixNano",
                    "1750000000100000000",
                    "timeMillis",
                    1750000000100L,
                    "name",
                    "exception",
                    "attributes",
                    map("exception.type", "TimeoutError"),
                    "droppedAttributesCount",
                    2L),
                map(
                    "timeUnixNano", "1750000000200000000",
                    "timeMillis", 1750000000200L,
                    "name", "retry")),
            "droppedEventsCount",
            3L,
            "links",
            List.of(
                map(
                    "traceId", "aaaefff798038103d269b633813fc60c",
                    "spanId", "bbb19b7ec3c1b174",
                    "traceState", "upstream=1",
                    "attributes", map("link.kind", "follows_from"),
                    "droppedAttributesCount", 4L)),
            "droppedLinksCount",
            5L,
            "status",
            map("code", "STATUS_CODE_ERROR", "message", "upstream timeout"),
            "resource",
            checkout,
            "scope",
            checkoutScope,
            "resourceSchemaUrl",
            schemaUrl),
        map(
            "traceId",
            TRACE_ID,
            "spanId",
            "ccc19b7ec3c1b174",
            "parentSpanId",
            SPAN_ID,
            "name",
            "GET /inventory",
            "kind",
            "SPAN_KIND_CLIENT",
            "startTimeUnixNano",
            "1750000000150000000",
            "startTimeMillis",
            1750000000150L,
            "endTimeUnixNano",
            "1750000000250000000",
            "endTimeMillis",
            1750000000250L,
            "status",
            map("code", "STATUS_CODE_OK"),
            "resource",
            checkout,
            "scope",
            checkoutScope,
            "resourceSchemaUrl",
            schemaUrl),
        map(
            "traceId",
            TRACE_ID,
            "spanId",
            "ddd19b7ec3c1b174",
            "name",
            "cache.get",
            "startTimeUnixNano",
            "1750000000300000000",
            "startTimeMillis",
            1750000000300L,
            "endTimeUnixNano",
            "1750000000310000000",
            "endTimeMillis",
            1750000000310L,
            "resource",
            checkout,
            "scope",
            map("name", "io.fleak.demo.cache"),
            "resourceSchemaUrl",
            schemaUrl),
        map(
            "traceId", "9998efff798038103d269b633813fc6c",
            "spanId", "fff19b7ec3c1b174",
            "name", "carrier.quote",
            "kind", "SPAN_KIND_PRODUCER",
            "startTimeUnixNano", "1750000000400000000",
            "startTimeMillis", 1750000000400L,
            "endTimeUnixNano", "1750000000900000000",
            "endTimeMillis", 1750000000900L,
            "resource", map("service.name", "shipping")));
  }
}
