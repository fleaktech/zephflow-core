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
package io.fleak.zephflow.lib.commands.sink;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Estimates the number of bytes a sink wrote, for sinks whose transport does not report a wire
 * size.
 *
 * <p>JDBC drivers send batches over a driver-specific protocol and expose no byte count, so {@code
 * output_event_size} for the JDBC-family sinks is necessarily an estimate. It measures the bound
 * <em>values</em> only: column names travel once in the prepared SQL, not per row, so counting them
 * per row would inflate the metric in proportion to batch size.
 */
public final class SinkDataSizeEstimator {

  private SinkDataSizeEstimator() {}

  /** Estimated bytes of the values bound for one row. */
  public static long estimateRowBytes(Map<String, Object> row) {
    if (row == null) {
      return 0;
    }
    long size = 0;
    for (Object value : row.values()) {
      size += estimateValueBytes(value);
    }
    return size;
  }

  /**
   * Estimated bytes of a single bound value. Numeric and boolean types use their binary widths;
   * anything without a known width falls back to the UTF-8 length of its string form.
   */
  public static long estimateValueBytes(Object value) {
    return switch (value) {
      case null -> 0L;
      case String s -> s.getBytes(StandardCharsets.UTF_8).length;
      case byte[] b -> b.length;
      case Boolean ignored -> 1L;
      case Byte ignored -> 1L;
      case Short ignored -> 2L;
      case Integer ignored -> 4L;
      case Float ignored -> 4L;
      case Long ignored -> 8L;
      case Double ignored -> 8L;
      default -> String.valueOf(value).getBytes(StandardCharsets.UTF_8).length;
    };
  }
}
