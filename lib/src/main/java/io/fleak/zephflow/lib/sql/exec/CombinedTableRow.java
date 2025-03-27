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
package io.fleak.zephflow.lib.sql.exec;

import com.google.common.base.Strings;
import io.fleak.zephflow.lib.sql.exec.errors.RelationNotInRowException;
import io.fleak.zephflow.lib.sql.rel.Algebra;
import java.util.*;

public class CombinedTableRow extends Row {

  final Map<String, Row> rowMap;

  Map<String, List<Algebra.RenameField>> renameFieldsMap;

  public CombinedTableRow(Set<String> relationNames, Map<String, Row> rowMap) {
    super(relationNames);
    this.rowMap = rowMap;
  }

  @Override
  public <T> T resolve(String rel, String col, Class<T> clz) {
    if (!relationNames.contains(rel))
      throw new RelationNotInRowException(
          rel + " is not found in " + String.join(",", relationNames));

    Row row = rowMap.get(rel);
    if (row == null) return null;

    return row.resolve(rel, col, clz);
  }

  @Override
  public Map<String, Row> rowMap() {
    return Collections.unmodifiableMap(rowMap);
  }

  @Override
  public Row assocEntries(Entry[] entries) {
    var m = new LinkedHashMap<>(rowMap);
    for (var entry : entries) {
      var v = m.get(entry.key().rel());
      if (v != null) {
        m.put(entry.key().rel(), v.assocEntries(new Entry[] {entry}));
      } else {
        throw new RuntimeException("cannot add entries here");
      }
    }
    return new CombinedTableRow(relationNames, m);
  }

  public CombinedTableRow assocColumn(Key key, Value value) {
    if (Strings.isNullOrEmpty(key.rel())) {
      throw new RuntimeException("key relation cannot be null or empty: " + key);
    }
    var row = rowMap.get(key.rel());
    if (row == null) throw new RuntimeException("no table found for " + key.rel());
    var m2 = new LinkedHashMap<>(rowMap);
    m2.put(key.rel(), row.assocColumn(key, value));
    return new CombinedTableRow(relationNames, m2);
  }

  @Override
  public Entry[] getEntries() {
    return rowMap.entrySet().stream()
        .flatMap(t -> Arrays.stream(t.getValue().getEntries()))
        .toArray(Entry[]::new);
  }

  @Override
  public Row renameColumns(List<Algebra.RenameField> renames) {

    if (renameFieldsMap == null) {
      final Map<String, List<Algebra.RenameField>> tableReNameFields = new HashMap<>();
      for (var tbl : rowMap.keySet()) {
        tableReNameFields.put(tbl, new ArrayList<>());
      }

      for (var rn : renames) {
        var ls = tableReNameFields.get(rn.getColumn().getRelName());
        if (ls == null)
          throw new RuntimeException(
              "internal error, wrong rename relation here "
                  + rn
                  + " relations available: "
                  + String.join(",", rowMap.keySet()));
        ls.add(rn);
      }
      renameFieldsMap = tableReNameFields;
    }

    Map<String, Row> newRowMap = new HashMap<>();

    for (var entry : rowMap.entrySet()) {
      var relRenames = renameFieldsMap.get(entry.getKey());
      if (relRenames != null && !relRenames.isEmpty()) {
        newRowMap.put(entry.getKey(), entry.getValue().renameColumns(relRenames));
      } else {
        newRowMap.put(entry.getKey(), entry.getValue());
      }
    }

    return new CombinedTableRow(relationNames, newRowMap);
  }
}
