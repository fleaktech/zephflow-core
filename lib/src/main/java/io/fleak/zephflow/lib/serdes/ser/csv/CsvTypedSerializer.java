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
package io.fleak.zephflow.lib.serdes.ser.csv;

import io.fleak.zephflow.lib.serdes.ser.MultipleEventsTypedSerializer;
import java.io.ByteArrayOutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

/** Created by bolei on 9/16/24 */
public class CsvTypedSerializer extends MultipleEventsTypedSerializer<Map<String, Object>> {
  private final CsvSerializerOptions options;

  public CsvTypedSerializer() {
    this(CsvSerializerOptions.DEFAULT);
  }

  public CsvTypedSerializer(CsvSerializerOptions options) {
    this.options = Objects.requireNonNullElse(options, CsvSerializerOptions.DEFAULT);
  }

  @Override
  protected byte[] serializeToMultipleTypedEvent(List<Map<String, Object>> typedValues)
      throws Exception {
    if (CollectionUtils.isEmpty(typedValues)) {
      return new byte[0];
    }
    Collection<String> header = resolveHeader(typedValues);
    if (CollectionUtils.isEmpty(header)) {
      return new byte[0];
    }
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

    try (OutputStreamWriter out =
            new OutputStreamWriter(byteArrayOutputStream, StandardCharsets.UTF_8);
        CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT)) {
      if (options.includeHeader()) {
        printer.printRecord(header);
      }

      boolean singleColumn = header.size() == 1;
      for (Map<String, Object> event : typedValues) {
        List<Object> values = header.stream().map(event::get).toList();
        if (singleColumn && values.get(0) == null) {
          values = List.of("");
        }
        printer.printRecord(values);
      }
    }

    return byteArrayOutputStream.toByteArray();
  }

  private Collection<String> resolveHeader(List<Map<String, Object>> typedValues) {
    if (options.hasPinnedColumns()) {
      return options.columns();
    }
    // Keys may differ between events, so we need to collect all keys first in order to generate the
    // CSV header
    Set<String> header = new LinkedHashSet<>();
    for (Map<String, Object> event : typedValues) {
      header.addAll(event.keySet());
    }
    return header;
  }
}
