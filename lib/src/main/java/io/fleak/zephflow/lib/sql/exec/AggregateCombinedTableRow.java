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

import io.fleak.zephflow.lib.sql.exec.types.TypeSystem;
import io.fleak.zephflow.lib.sql.rel.Algebra;
import java.util.*;

public class AggregateCombinedTableRow extends AggregateRow {

  final List<Entry> groupKeys;
  final Iterable<Row> groupedRows;
  final TypeSystem typeSystem;

  final Row groupKeysRow;

  public AggregateCombinedTableRow(
      TypeSystem typeSystem,
      Set<String> relationNames,
      List<Entry> groupKeys,
      Iterable<Row> groupedRows) {
    super(relationNames);
    this.typeSystem = typeSystem;
    this.groupKeys = groupKeys;
    this.groupedRows = groupedRows;
    this.groupKeysRow = Row.wrap(typeSystem, relationNames, groupKeys);
  }

  @Override
  public List<Entry> getGroupKey() {
    return groupKeys;
  }

  @Override
  public Iterator<Row> groupedRows() {
    return groupedRows.iterator();
  }

  @Override
  public Map<String, Row> rowMap() {
    return groupKeysRow.rowMap();
  }

  @Override
  public Row assocEntries(Entry[] entries) {
    return groupKeysRow.assocEntries(entries);
  }

  @Override
  public Row assocColumn(Key key, Value value) {
    return groupKeysRow.assocColumn(key, value);
  }

  @Override
  public Entry[] getEntries() {
    return groupKeys.toArray(new Entry[0]);
  }

  @Override
  public Row renameColumns(List<Algebra.RenameField> renames) {
    return groupKeysRow.renameColumns(renames);
  }

  @Override
  public <T> T resolve(String rel, String col, Class<T> clz) {
    return groupKeysRow.resolve(rel, col, clz);
  }
}
