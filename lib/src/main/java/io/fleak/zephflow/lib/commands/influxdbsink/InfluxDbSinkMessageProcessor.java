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

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.fleak.zephflow.api.structure.BooleanPrimitiveFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.time.Instant;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;

/**
 * Maps a {@link RecordFleakData} into an InfluxDB {@link Point} using the configured mapping.
 *
 * <p>Selectors are top-level record field names. A mapping problem (no measurement resolved, no
 * fields resolved, or a configured timestamp that is missing/unparseable) throws, so the enclosing
 * {@code SimpleSinkCommand} records only that single record as an error and continues the batch.
 */
public class InfluxDbSinkMessageProcessor
    implements SimpleSinkCommand.SinkMessagePreProcessor<Point> {

  private final String measurement;
  private final String measurementField;
  private final List<String> tagFields; // never null
  private final List<String> fieldFields; // nullable/empty => all remaining fields
  private final String timestampField; // nullable => write time
  private final WritePrecision precision;

  public InfluxDbSinkMessageProcessor(
      String measurement,
      String measurementField,
      List<String> tagFields,
      List<String> fieldFields,
      String timestampField,
      WritePrecision precision) {
    this.measurement = measurement;
    this.measurementField = measurementField;
    this.tagFields = tagFields == null ? List.of() : tagFields;
    this.fieldFields = fieldFields;
    this.timestampField = timestampField;
    this.precision = precision == null ? WritePrecision.MS : precision;
  }

  @Override
  public Point preprocess(RecordFleakData event, long ts) throws Exception {
    Map<String, FleakData> payload = event.getPayload();

    Point point = Point.measurement(resolveMeasurement(payload));

    for (String tag : tagFields) {
      FleakData value = payload.get(tag);
      if (value != null) {
        point.addTag(tag, stringify(value));
      }
    }

    int fieldsAdded = 0;
    for (String name : fieldNames(payload)) {
      FleakData value = payload.get(name);
      if (value == null) {
        continue;
      }
      addField(point, name, value);
      fieldsAdded++;
    }
    if (fieldsAdded == 0) {
      throw new IllegalArgumentException(
          "record produced no InfluxDB fields; InfluxDB requires at least one field per point");
    }

    applyTimestamp(point, payload, ts);
    return point;
  }

  private String resolveMeasurement(Map<String, FleakData> payload) {
    if (StringUtils.isNotBlank(measurement)) {
      return measurement;
    }
    FleakData value = payload.get(measurementField);
    if (value == null) {
      throw new IllegalArgumentException(
          "measurement field '" + measurementField + "' is missing from the record");
    }
    String resolved = stringify(value);
    if (StringUtils.isBlank(resolved)) {
      throw new IllegalArgumentException(
          "measurement field '" + measurementField + "' resolved to a blank value");
    }
    return resolved;
  }

  /**
   * Field names to write: the explicit {@code fieldFields}, or when unset every record key that is
   * not used as a tag, the timestamp, or the measurement source.
   */
  private Iterable<String> fieldNames(Map<String, FleakData> payload) {
    if (fieldFields != null && !fieldFields.isEmpty()) {
      return fieldFields;
    }
    Set<String> excluded = new LinkedHashSet<>(tagFields);
    if (timestampField != null) {
      excluded.add(timestampField);
    }
    if (measurementField != null) {
      excluded.add(measurementField);
    }
    Set<String> remaining = new LinkedHashSet<>(payload.keySet());
    remaining.removeAll(excluded);
    return remaining;
  }

  private void addField(Point point, String name, FleakData value) {
    switch (value) {
      case NumberPrimitiveFleakData n -> {
        if (n.getNumberType() == NumberPrimitiveFleakData.NumberType.LONG) {
          point.addField(name, (long) n.getNumberValue());
        } else {
          point.addField(name, n.getNumberValue());
        }
      }
      case BooleanPrimitiveFleakData b -> point.addField(name, b.isTrueValue());
      case StringPrimitiveFleakData s -> point.addField(name, s.getStringValue());
        // nested object/array: not a scalar, keep the data as a JSON string field
      default -> point.addField(name, JsonUtils.toJsonString(value));
    }
  }

  /** Coerces a value to a string for use as a tag value or measurement name. */
  private String stringify(FleakData value) {
    return switch (value) {
      case StringPrimitiveFleakData s -> s.getStringValue();
      case NumberPrimitiveFleakData n -> String.valueOf(n.unwrap());
      case BooleanPrimitiveFleakData b -> String.valueOf(b.isTrueValue());
      default -> JsonUtils.toJsonString(value);
    };
  }

  private void applyTimestamp(Point point, Map<String, FleakData> payload, long ts) {
    if (StringUtils.isBlank(timestampField)) {
      point.time(Instant.ofEpochMilli(ts), precision);
      return;
    }
    FleakData value = payload.get(timestampField);
    if (value == null) {
      throw new IllegalArgumentException(
          "timestamp field '" + timestampField + "' is missing from the record");
    }
    switch (value) {
      case NumberPrimitiveFleakData n ->
          // numeric epoch already expressed in the configured precision
          point.time((long) n.getNumberValue(), precision);
      case StringPrimitiveFleakData s -> point.time(Instant.parse(s.getStringValue()), precision);
      default ->
          throw new IllegalArgumentException(
              "timestamp field '"
                  + timestampField
                  + "' must be a numeric epoch or an ISO-8601 string");
    }
  }
}
