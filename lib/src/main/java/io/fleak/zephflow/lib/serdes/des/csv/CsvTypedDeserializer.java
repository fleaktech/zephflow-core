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

import io.fleak.zephflow.lib.serdes.des.MultipleEventsTypedDeserializer;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

/** Created by bolei on 9/16/24 */
public class CsvTypedDeserializer extends MultipleEventsTypedDeserializer<Map<String, Object>> {

  private static final CSVFormat CSV_FORMAT =
      CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build();

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
