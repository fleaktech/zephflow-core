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

import io.fleak.zephflow.lib.sql.exec.errors.RelationNotInRowException;
import io.fleak.zephflow.lib.sql.exec.types.TypeSystem;
import io.fleak.zephflow.lib.sql.rel.Algebra;
import java.util.*;

/** */
public class MapRow extends Row {
  final LinkedHashMap<Row.Key, Row.Value> rowData;
  final TypeSystem typeSystem;

  public MapRow(TypeSystem typeSystem, Set<String> relsName, LinkedHashMap<Row.Key, Row.Value> m) {
    super(relsName);
    this.typeSystem = typeSystem;
    this.rowData = m;
  }

  @Override
  public Row assocEntries(Entry[] entries) {
    var m = new LinkedHashMap<>(rowData);
    for (var entry : entries) {
      m.put(entry.key(), entry.value());
    }

    return new MapRow(typeSystem, getRelationNames(), m);
  }

  @Override
  public <T> T resolve(String rel, String col, Class<T> clz) {
    if (!getRelationNames().contains(rel)) {
      throw new RelationNotInRowException(
          "Cannot resolve " + rel + " from " + String.join(",", getRelationNames()));
    }
    Row.Value v = rowData.get(Row.asKey(rel, col, -1));
    if (v == null) return null;

    return typeSystem.lookupTypeCast(clz).cast(v.asObject());
  }

  @Override
  public Map<String, Row> rowMap() {
    if (relationNames.size() > 1)
      throw new RuntimeException(
          "required a single relation name but got " + String.join(",", relationNames));
    return Map.of(relationNames.iterator().next(), this);
  }

  @Override
  public Row assocColumn(Key key, Value value) {
    if (!relationNames.contains(key.rel()))
      throw new RuntimeException(
          "cannot add a column to row with rel= "
              + String.join(",", relationNames)
              + " for key "
              + key.rel());

    // @TODO see if using persistent data structures work better
    var m2 = new LinkedHashMap<>(rowData);
    m2.put(key, value);

    return new MapRow(typeSystem, relationNames, m2);
  }

  @Override
  public Row.Entry[] getEntries() {
    Row.Entry[] entries = new Row.Entry[rowData.size()];
    int i = 0;
    for (var entry : rowData.entrySet()) {
      entries[i++] = Row.asEntry(entry.getKey(), entry.getValue());
    }
    return entries;
  }

  @Override
  public Row renameColumns(List<Algebra.RenameField> renames) {

    var renameMap = new HashMap<Row.Key, Algebra.RenameField>();
    for (var rn : renames) {
      var col = rn.getColumn();
      renameMap.put(Row.asKey(col.getRelName(), col.getName(), col.getSelectPosition()), rn);
    }

    var m = new LinkedHashMap<Row.Key, Row.Value>();
    for (var entry : rowData.entrySet()) {
      var k = entry.getKey();
      var rn = renameMap.get(k);
      if (rn == null) {
        m.put(k, entry.getValue());
      } else {
        m.put(Row.asKey(k.rel(), rn.getNewName(), k.relativePosition()), entry.getValue());
      }
    }

    return Row.wrapMap(typeSystem, getRelationNames(), m);
  }
}
