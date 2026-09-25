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
package io.fleak.zephflow.lib.serdes.des.csv;

import io.fleak.zephflow.lib.serdes.des.IncrementalSupport;
import io.fleak.zephflow.lib.serdes.des.MultipleEventsTypedDeserializer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import org.apache.commons.csv.CSVException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/** Created by bolei on 9/16/24 */
public class CsvTypedDeserializer extends MultipleEventsTypedDeserializer<Map<String, Object>> {

  @Override
  public void deserializeIncrementally(
      byte[] value, Consumer<TypedOutcome<Map<String, Object>>> consumer, BooleanSupplier stop) {
    try {
      // Legacy CSV is an atomic payload on error. Validate before emitting, but retain no rows.
      scan(value, ignored -> {}, stop);
      int[] index = {0};
      scan(
          value,
          row ->
              IncrementalSupport.deliver(
                  consumer, new TypedOutcome<>(row, ++index[0], -1, -1, null)),
          stop);
    } catch (IncrementalSupport.Stopped ignored) {
      // Cancellation is not a malformed record.
    } catch (IncrementalSupport.SinkFailure e) {
      throw e.failure;
    } catch (Exception e) {
      if (!stop.getAsBoolean() && !Thread.currentThread().isInterrupted()) {
        consumer.accept(new TypedOutcome<>(null, -1, 0, value.length, e));
      }
    }
  }

  @Override
  public boolean supportsStreaming() {
    return true;
  }

  /**
   * Reads one row at a time, so only the current row is held in memory. The parser, not a newline
   * split, finds row boundaries, which keeps a quoted field that spans lines intact. A short row is
   * reported on its own and the read continues; malformed quoting leaves the parser with no way to
   * find the next row, so it ends the read.
   */
  @Override
  public void deserializeStream(
      InputStream input, Consumer<StreamedOutcome<Map<String, Object>>> consumer)
      throws IOException {
    CSVParser parser;
    try {
      parser = CSV_FORMAT.parse(new InputStreamReader(input, StandardCharsets.UTF_8));
    } catch (CSVException | IllegalArgumentException malformedHeader) {
      consumer.accept(malformed(-1, malformedHeader));
      return;
    }
    try (parser) {
      Iterator<CSVRecord> records = parser.iterator();
      int index = 0;
      while (true) {
        CSVRecord record;
        try {
          if (!records.hasNext()) {
            return;
          }
          record = records.next();
        } catch (UncheckedIOException e) {
          // The iterator wraps every failure; only a CSVException means the content is malformed.
          if (e.getCause() instanceof CSVException malformedInput) {
            consumer.accept(malformed(index + 1, malformedInput));
            return;
          }
          throw e.getCause();
        }
        index++;
        Map<String, Object> row;
        try {
          row = toRow(parser.getHeaderNames(), record);
        } catch (IllegalArgumentException shortRow) {
          consumer.accept(
              new StreamedOutcome<>(
                  null,
                  index,
                  CSVFormat.DEFAULT.format(record.values()).getBytes(StandardCharsets.UTF_8),
                  shortRow));
          continue;
        }
        consumer.accept(new StreamedOutcome<>(row, index, null, null));
      }
    }
  }

  private static StreamedOutcome<Map<String, Object>> malformed(int index, Exception error) {
    return new StreamedOutcome<>(null, index, new byte[0], error);
  }

  /** Throws {@link IllegalArgumentException} when the row has fewer values than the header. */
  private static Map<String, Object> toRow(List<String> headerNames, CSVRecord record) {
    Map<String, Object> row = new HashMap<>(headerNames.size());
    for (String header : headerNames) {
      String cell = record.get(header);
      if (cell != null) {
        row.put(header, cell);
      }
    }
    return row;
  }

  private void scan(byte[] value, Consumer<Map<String, Object>> consumer, BooleanSupplier stop)
      throws Exception {
    IncrementalSupport.checkStop(stop);
    try (InputStreamReader reader =
            new InputStreamReader(new ByteArrayInputStream(value), StandardCharsets.UTF_8);
        CSVParser parser = CSV_FORMAT.parse(reader)) {
      var iterator = parser.iterator();
      while (true) {
        IncrementalSupport.checkStop(stop);
        if (!iterator.hasNext()) {
          break;
        }
        CSVRecord record = iterator.next();
        Map<String, Object> row = new HashMap<>();
        for (String header : parser.getHeaderNames()) {
          IncrementalSupport.checkStop(stop);
          String cell = record.get(header);
          if (cell != null) {
            row.put(header, cell);
          }
        }
        IncrementalSupport.checkStop(stop);
        consumer.accept(row);
      }
    }
  }

  private static final CSVFormat CSV_FORMAT =
      CSVFormat.DEFAULT
          .builder()
          .setHeader()
          .setSkipHeaderRecord(true)
          .setIgnoreSurroundingSpaces(true)
          .build();

  @Override
  protected List<Map<String, Object>> deserializeToMultipleTypedEvent(byte[] value)
      throws Exception {

    List<Map<String, Object>> typedEvents = new ArrayList<>();

    try (InputStreamReader reader =
            new InputStreamReader(new ByteArrayInputStream(value), StandardCharsets.UTF_8);
        CSVParser parser = CSV_FORMAT.parse(reader)) {

      int headerCount = parser.getHeaderNames().size();

      for (CSVRecord record : parser) {
        Map<String, Object> payload = new HashMap<>(headerCount);

        for (String headerName : parser.getHeaderNames()) {
          String cellValue = record.get(headerName);
          if (cellValue != null) {
            payload.put(headerName, cellValue);
          }
        }

        typedEvents.add(payload);
      }
    }

    return typedEvents;
  }
}
