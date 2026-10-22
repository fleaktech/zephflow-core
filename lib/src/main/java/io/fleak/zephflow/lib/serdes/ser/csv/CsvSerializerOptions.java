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

import io.fleak.zephflow.lib.serdes.ser.FieldNames;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public record CsvSerializerOptions(List<String> columns, boolean includeHeader) {

  public static final CsvSerializerOptions DEFAULT = new CsvSerializerOptions(null, true);

  public CsvSerializerOptions {
    if (columns != null) {
      Set<String> names = new LinkedHashSet<>();
      for (String column : columns) {
        String name = FieldNames.normalize(column);
        if (name.isEmpty()) {
          throw new IllegalArgumentException("CSV column names must not be blank");
        }
        if (!names.add(name)) {
          throw new IllegalArgumentException("Duplicate CSV column: " + name);
        }
      }
      columns = List.copyOf(names);
    }
    if (!includeHeader && !hasColumns(columns)) {
      throw new IllegalArgumentException(
          "CSV columns must be set when the header row is turned off");
    }
  }

  public boolean hasPinnedColumns() {
    return hasColumns(columns);
  }

  private static boolean hasColumns(List<String> columns) {
    return columns != null && !columns.isEmpty();
  }
}
