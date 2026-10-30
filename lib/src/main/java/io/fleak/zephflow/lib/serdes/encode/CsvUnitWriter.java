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
package io.fleak.zephflow.lib.serdes.encode;

import io.fleak.zephflow.api.structure.RecordFleakData;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

/**
 * A header row when {@code header} is on, then one row per record, each ending CRLF. The columns
 * are {@code columns}, or the keys of the records in first-seen order when null.
 */
record CsvUnitWriter(CSVFormat format, List<String> columns, boolean header) implements UnitWriter {

  @Override
  public byte[] write(List<RecordFleakData> records) throws IOException {
    if (records.isEmpty()) {
      return new byte[0];
    }
    List<Map<String, Object>> rows = records.stream().map(RecordFleakData::unwrap).toList();
    Collection<String> names = columns != null ? columns : keys(rows);
    if (names.isEmpty()) {
      return new byte[0];
    }
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (CSVPrinter printer =
        new CSVPrinter(new OutputStreamWriter(bytes, StandardCharsets.UTF_8), format)) {
      if (header) {
        printer.printRecord(names);
      }
      boolean singleColumn = names.size() == 1;
      for (Map<String, Object> row : rows) {
        List<Object> values = names.stream().map(row::get).toList();
        if (singleColumn && values.getFirst() == null) {
          // A lone empty value is written quoted; a blank line would be skipped on read.
          values = List.of("");
        }
        printer.printRecord(values);
      }
    }
    return bytes.toByteArray();
  }

  private static Set<String> keys(List<Map<String, Object>> rows) {
    Set<String> keys = new LinkedHashSet<>();
    rows.forEach(row -> keys.addAll(row.keySet()));
    return keys;
  }
}
