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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reading primitives for OTLP/JSON: the parts identical for logs, metrics and traces. These undo
 * protobuf-JSON encoding artifacts and nothing more; deriving fields and collapsing oneofs is the
 * DAG's job.
 *
 * <p>int64 values stay strings. A nanosecond epoch is ~1.75e18, past the 2^53 a double holds
 * exactly, and {@code NumberPrimitiveFleakData} is double-backed — so a numeric {@code
 * timeUnixNano} or metric {@code asInt} would silently lose digits. {@link #millis(String)} is the
 * queryable companion.
 */
final class OtlpJson {

  private OtlpJson() {}

  /** Collapses OTLP's {@code [{key, value:{<type>: ...}}]} list into a plain map. */
  static Map<String, Object> attributes(Object raw) {
    Map<String, Object> attributes = new LinkedHashMap<>();
    for (Map<String, Object> keyValue : mapList(raw)) {
      Object key = keyValue.get("key");
      if (key != null) {
        attributes.put(key.toString(), anyValue(keyValue.get("value")));
      }
    }
    return attributes;
  }

  /** Unwraps an OTLP {@code AnyValue} union into its plain Java equivalent. */
  static Object anyValue(Object raw) {
    Map<String, Object> value = map(raw);
    if (value == null || value.isEmpty()) {
      return null;
    }
    if (value.containsKey("stringValue")) {
      return value.get("stringValue");
    }
    if (value.containsKey("boolValue")) {
      return value.get("boolValue");
    }
    if (value.containsKey("intValue")) {
      return asLong(value.get("intValue"));
    }
    if (value.containsKey("doubleValue")) {
      return asDouble(value.get("doubleValue"));
    }
    if (value.containsKey("bytesValue")) {
      // Base64 on the wire; left encoded rather than guessing at a charset.
      return value.get("bytesValue");
    }
    if (value.containsKey("arrayValue")) {
      List<Object> items = new ArrayList<>();
      for (Object item : list(field(value.get("arrayValue"), "values"))) {
        items.add(anyValue(item));
      }
      return items;
    }
    if (value.containsKey("kvlistValue")) {
      return attributes(field(value.get("kvlistValue"), "values"));
    }
    return null;
  }

  /**
   * Proto3 JSON encodes int64 as a string, but some emitters send a bare number — and one large
   * enough arrives already widened to scientific notation, hence BigDecimal over toString().
   */
  static String int64String(Object raw) {
    if (raw == null) {
      return null;
    }
    String text = raw.toString().trim();
    if (text.isEmpty()) {
      return null;
    }
    if (text.indexOf('E') >= 0 || text.indexOf('e') >= 0 || text.indexOf('.') >= 0) {
      try {
        return new BigDecimal(text).toBigInteger().toString();
      } catch (NumberFormatException e) {
        return text;
      }
    }
    return text;
  }

  static List<String> int64List(Object raw) {
    List<String> values = new ArrayList<>();
    for (Object item : list(raw)) {
      values.add(int64String(item));
    }
    return values;
  }

  static List<Double> doubleList(Object raw) {
    List<Double> values = new ArrayList<>();
    for (Object item : list(raw)) {
      values.add(asDouble(item));
    }
    return values;
  }

  /**
   * Proto3 JSON permits an enum as its name or its number, so settling on the name is normalization
   * rather than interpretation. A number outside the known range passes through as text, so a
   * future OTLP member survives as data rather than becoming null.
   */
  static String enumName(Object raw, List<String> namesByNumber) {
    if (raw == null) {
      return null;
    }
    String text = raw.toString().trim();
    if (text.isEmpty()) {
      return null;
    }
    int number;
    try {
      number = Integer.parseInt(text);
    } catch (NumberFormatException e) {
      return text;
    }
    return number >= 0 && number < namesByNumber.size() ? namesByNumber.get(number) : text;
  }

  /** Milliseconds from an already-normalized nanosecond string; null when unparseable. */
  static Long millis(String int64Nanos) {
    if (int64Nanos == null) {
      return null;
    }
    try {
      return Long.parseLong(int64Nanos) / 1_000_000L;
    } catch (NumberFormatException e) {
      return null;
    }
  }

  static Long asLong(Object raw) {
    if (raw instanceof Number number) {
      return number.longValue();
    }
    if (raw == null) {
      return null;
    }
    try {
      return Long.parseLong(raw.toString().trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  static Double asDouble(Object raw) {
    if (raw instanceof Number number) {
      return number.doubleValue();
    }
    if (raw == null) {
      return null;
    }
    try {
      return Double.parseDouble(raw.toString().trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /** Skips nulls, so a field absent from the wire stays absent from the record. */
  static void put(Map<String, Object> target, String key, Object value) {
    if (value != null) {
      target.put(key, value);
    }
  }

  /** Extends {@link #put}'s omit-on-null to collections, so an empty one is omitted too. */
  static void putIfNotEmpty(Map<String, Object> target, String key, Map<String, Object> value) {
    if (value != null && !value.isEmpty()) {
      target.put(key, value);
    }
  }

  static void putIfNotEmpty(Map<String, Object> target, String key, Collection<?> value) {
    if (value != null && !value.isEmpty()) {
      target.put(key, value);
    }
  }

  static Object field(Object raw, String key) {
    Map<String, Object> map = map(raw);
    return map == null ? null : map.get(key);
  }

  @SuppressWarnings("unchecked")
  static Map<String, Object> map(Object raw) {
    return raw instanceof Map<?, ?> map ? (Map<String, Object>) map : null;
  }

  static List<?> list(Object raw) {
    return raw instanceof List<?> list ? list : List.of();
  }

  @SuppressWarnings("unchecked")
  static List<Map<String, Object>> mapList(Object raw) {
    List<Map<String, Object>> maps = new ArrayList<>();
    for (Object item : list(raw)) {
      if (item instanceof Map<?, ?> map) {
        maps.add((Map<String, Object>) map);
      }
    }
    return maps;
  }
}
