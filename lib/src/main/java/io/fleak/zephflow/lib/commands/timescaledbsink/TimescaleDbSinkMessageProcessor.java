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

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Turns a {@link RecordFleakData} into a column map for the JDBC writer, coercing the configured
 * time column to a UTC {@link OffsetDateTime} so it lands in a {@code TIMESTAMPTZ} hypertable
 * column (record numerics are all doubles and would otherwise be rejected by a timestamp column).
 *
 * <p>A missing or unparseable time value throws, so the enclosing {@code SimpleSinkCommand} records
 * only that single record as an error and continues the batch.
 */
public class TimescaleDbSinkMessageProcessor
    implements SimpleSinkCommand.SinkMessagePreProcessor<Map<String, Object>> {

  private final String timeColumn;
  private final TimeUnit timeUnit;

  public TimescaleDbSinkMessageProcessor(String timeColumn, TimeUnit timeUnit) {
    this.timeColumn = timeColumn;
    this.timeUnit = timeUnit;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Map<String, Object> preprocess(RecordFleakData event, long ts) {
    Map<String, Object> row = new LinkedHashMap<>((Map<String, Object>) event.unwrap());

    if (!row.containsKey(timeColumn) || row.get(timeColumn) == null) {
      throw new IllegalArgumentException(
          "time column '" + timeColumn + "' is missing from the record");
    }
    row.put(timeColumn, toTimestamp(row.get(timeColumn)));
    return row;
  }

  private OffsetDateTime toTimestamp(Object value) {
    Instant instant =
        switch (value) {
          case Number n -> Instant.ofEpochSecond(0L, timeUnit.toNanos(n.longValue()));
          case String s -> parseInstant(s);
          default ->
              throw new IllegalArgumentException(
                  "time column '"
                      + timeColumn
                      + "' must be a numeric epoch or an ISO-8601 string, got: "
                      + value.getClass().getSimpleName());
        };
    return instant.atOffset(ZoneOffset.UTC);
  }

  private Instant parseInstant(String value) {
    try {
      return OffsetDateTime.parse(value).toInstant();
    } catch (Exception withOffset) {
      return Instant.parse(value);
    }
  }
}
