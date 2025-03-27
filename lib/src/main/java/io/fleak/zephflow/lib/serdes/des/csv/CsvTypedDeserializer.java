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

import com.opencsv.CSVReader;
import io.fleak.zephflow.lib.serdes.des.MultipleEventsTypedDeserializer;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Created by bolei on 9/16/24 */
public class CsvTypedDeserializer extends MultipleEventsTypedDeserializer<Map<String, Object>> {
  @Override
  protected List<Map<String, Object>> deserializeToMultipleTypedEvent(byte[] value)
      throws Exception {

    List<Map<String, Object>> typedEvents = new ArrayList<>();

    try (CSVReader reader =
        new CSVReader(
            new InputStreamReader(new ByteArrayInputStream(value), StandardCharsets.UTF_8))) {
      String[] headerLine = null;
      String[] nextLine;
      while ((nextLine = reader.readNext()) != null) {
        if (headerLine == null) {
          headerLine = nextLine;
          continue;
        }
        Map<String, Object> payload = new HashMap<>();
        for (int i = 0; i < nextLine.length; ++i) {
          String key = headerLine[i];
          if (nextLine[i] == null) {
            continue;
          }
          payload.put(key, nextLine[i]);
        }
        typedEvents.add(payload);
      }
    }
    return typedEvents;
  }
}
