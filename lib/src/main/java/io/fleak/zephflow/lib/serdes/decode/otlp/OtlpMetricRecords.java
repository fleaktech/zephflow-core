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
 * Flattens an OTLP/JSON metrics payload into one flat map per data point, resolving the {@code
 * gauge|sum|histogram|exponentialHistogram|summary} oneof to reach the leaves.
 *
 * <p>{@code metricType} names the member the point came from. That tag is on the wire, encoded
 * structurally as which key exists, and after fan-out it is the only thing distinguishing a gauge
 * point from a sum point whose {@code aggregationTemporality} and {@code isMonotonic} were omitted
 * at their defaults — so without it the walker would be dropping information. It also settles a
 * naming quirk: a {@code histogram} record carries a {@code sum} key holding the aggregate, while a
 * {@code sum} record has no {@code sum} key at all.
 */
final class OtlpMetricRecords {

  /** {@code AggregationTemporality} by proto number. */
  private static final List<String> AGGREGATION_TEMPORALITIES =
      List.of(
          "AGGREGATION_TEMPORALITY_UNSPECIFIED",
          "AGGREGATION_TEMPORALITY_DELTA",
          "AGGREGATION_TEMPORALITY_CUMULATIVE");

  private static final List<String> METRIC_TYPES =
      List.of("gauge", "sum", "histogram", "exponentialHistogram", "summary");

  private OtlpMetricRecords() {}

  static List<Map<String, Object>> flatten(Map<String, Object> payload) {
    List<Map<String, Object>> records = new ArrayList<>();
    for (Map<String, Object> resourceMetrics : mapList(payload.get("resourceMetrics"))) {
      for (Map<String, Object> scopeMetrics : mapList(resourceMetrics.get("scopeMetrics"))) {
        OtlpEnvelope envelope = OtlpEnvelope.of(resourceMetrics, scopeMetrics);
        for (Map<String, Object> metric : mapList(scopeMetrics.get("metrics"))) {
          appendPoints(records, metric, envelope);
        }
      }
    }
    return records;
  }

  /**
   * Known loss: a future sixth oneof member is skipped silently. Throwing instead would dead-letter
   * a whole batch over one unrecognized metric, which is worse.
   */
  private static void appendPoints(
      List<Map<String, Object>> records, Map<String, Object> metric, OtlpEnvelope envelope) {
    String metricType = metricType(metric);
    if (metricType == null) {
      return;
    }
    Map<String, Object> data = map(metric.get(metricType));
    if (data == null) {
      return;
    }
    for (Map<String, Object> point : mapList(data.get("dataPoints"))) {
      records.add(point(metricType, metric, data, point, envelope));
    }
  }

  /**
   * One data point's record. {@code data} is the metric's {@code metricType} member, whose {@code
   * dataPoints} are not read here.
   */
  static Map<String, Object> point(
      String metricType,
      Map<String, Object> metric,
      Map<String, Object> data,
      Map<String, Object> point,
      OtlpEnvelope envelope) {
    String temporality = enumName(data.get("aggregationTemporality"), AGGREGATION_TEMPORALITIES);
    Map<String, Object> record = basePoint(metricType, point, metric, envelope);
    switch (metricType) {
      case "gauge" -> numberValue(record, point);
      case "sum" -> {
        numberValue(record, point);
        put(record, "aggregationTemporality", temporality);
        put(record, "isMonotonic", data.get("isMonotonic"));
      }
      case "histogram" -> {
        put(record, "count", int64String(point.get("count")));
        put(record, "sum", asDouble(point.get("sum")));
        put(record, "min", asDouble(point.get("min")));
        put(record, "max", asDouble(point.get("max")));
        putIfNotEmpty(record, "bucketCounts", int64List(point.get("bucketCounts")));
        putIfNotEmpty(record, "explicitBounds", doubleList(point.get("explicitBounds")));
        put(record, "aggregationTemporality", temporality);
      }
      case "exponentialHistogram" -> {
        put(record, "count", int64String(point.get("count")));
        put(record, "sum", asDouble(point.get("sum")));
        put(record, "min", asDouble(point.get("min")));
        put(record, "max", asDouble(point.get("max")));
        put(record, "scale", point.get("scale"));
        put(record, "zeroCount", int64String(point.get("zeroCount")));
        put(record, "zeroThreshold", asDouble(point.get("zeroThreshold")));
        putIfNotEmpty(record, "positive", buckets(point.get("positive")));
        putIfNotEmpty(record, "negative", buckets(point.get("negative")));
        put(record, "aggregationTemporality", temporality);
      }
      case "summary" -> {
        put(record, "count", int64String(point.get("count")));
        put(record, "sum", asDouble(point.get("sum")));
        putIfNotEmpty(record, "quantileValues", quantileValues(point.get("quantileValues")));
      }
      default -> throw new IllegalStateException(metricType);
    }
    return record;
  }

  /** The first oneof member present, or null when the metric carries no data block. */
  static String metricType(Map<String, Object> metric) {
    for (String candidate : METRIC_TYPES) {
      if (metric.containsKey(candidate)) {
        return candidate;
      }
    }
    return null;
  }

  /**
   * Hoists the enclosing {@code Metric}'s identity, without which a point cannot say what it
   * measures.
   */
  private static Map<String, Object> basePoint(
      String metricType,
      Map<String, Object> point,
      Map<String, Object> metric,
      OtlpEnvelope envelope) {
    Map<String, Object> record = new LinkedHashMap<>();
    record.put("metricType", metricType);
    put(record, "name", metric.get("name"));
    put(record, "description", metric.get("description"));
    put(record, "unit", metric.get("unit"));
    putIfNotEmpty(record, "metadata", attributes(metric.get("metadata")));

    String startTimeUnixNano = int64String(point.get("startTimeUnixNano"));
    put(record, "startTimeUnixNano", startTimeUnixNano);
    put(record, "startTimeMillis", millis(startTimeUnixNano));
    String timeUnixNano = int64String(point.get("timeUnixNano"));
    put(record, "timeUnixNano", timeUnixNano);
    put(record, "timeMillis", millis(timeUnixNano));
    put(record, "flags", point.get("flags"));

    putIfNotEmpty(record, "attributes", attributes(point.get("attributes")));
    putIfNotEmpty(record, "exemplars", exemplars(point.get("exemplars")));
    envelope.applyTo(record);
    return record;
  }

  /**
   * Keyed on {@code containsKey} rather than a null check: {@code "asDouble": 0} is a legitimate
   * observation and must not fall through to the {@code asInt} branch.
   */
  private static void numberValue(Map<String, Object> record, Map<String, Object> point) {
    if (point.containsKey("asDouble")) {
      put(record, "asDouble", asDouble(point.get("asDouble")));
    } else if (point.containsKey("asInt")) {
      put(record, "asInt", int64String(point.get("asInt")));
    }
  }

  /**
   * Left nested: {@code Buckets} is a struct, so flattening to {@code positiveOffset} is a rename.
   */
  private static Map<String, Object> buckets(Object raw) {
    Map<String, Object> rawBuckets = map(raw);
    if (rawBuckets == null || rawBuckets.isEmpty()) {
      return null;
    }
    Map<String, Object> buckets = new LinkedHashMap<>();
    put(buckets, "offset", rawBuckets.get("offset"));
    putIfNotEmpty(buckets, "bucketCounts", int64List(rawBuckets.get("bucketCounts")));
    return buckets.isEmpty() ? null : buckets;
  }

  private static List<Map<String, Object>> quantileValues(Object raw) {
    List<Map<String, Object>> quantileValues = new ArrayList<>();
    for (Map<String, Object> rawQuantile : mapList(raw)) {
      Map<String, Object> quantile = new LinkedHashMap<>();
      put(quantile, "quantile", asDouble(rawQuantile.get("quantile")));
      put(quantile, "value", asDouble(rawQuantile.get("value")));
      if (!quantile.isEmpty()) {
        quantileValues.add(quantile);
      }
    }
    return quantileValues;
  }

  /** The metric-to-trace join key, so carried rather than dropped. */
  private static List<Map<String, Object>> exemplars(Object raw) {
    List<Map<String, Object>> exemplars = new ArrayList<>();
    for (Map<String, Object> rawExemplar : mapList(raw)) {
      Map<String, Object> exemplar = new LinkedHashMap<>();
      putIfNotEmpty(
          exemplar, "filteredAttributes", attributes(rawExemplar.get("filteredAttributes")));
      String timeUnixNano = int64String(rawExemplar.get("timeUnixNano"));
      put(exemplar, "timeUnixNano", timeUnixNano);
      put(exemplar, "timeMillis", millis(timeUnixNano));
      numberValue(exemplar, rawExemplar);
      put(exemplar, "spanId", rawExemplar.get("spanId"));
      put(exemplar, "traceId", rawExemplar.get("traceId"));
      if (!exemplar.isEmpty()) {
        exemplars.add(exemplar);
      }
    }
    return exemplars;
  }
}
