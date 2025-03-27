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
import io.fleak.zephflow.lib.sql.rel.Algebras;
import java.util.*;
import javax.annotation.Nonnull;

/** A single map table with a single row */
public class MapTable extends Table<MapRow> {

  final TypeSystem typeSystem;
  final String relName;
  final LinkedHashMap<Row.Key, Row.Value> data;

  List<Algebra.Column> header;

  public MapTable(TypeSystem typeSystem, String relName, SortedMap<String, Object> data) {
    this.typeSystem = typeSystem;
    this.relName = relName;

    this.data = new LinkedHashMap<>();

    int i = 0;
    var columns = new ArrayList<Algebra.Column>();
    for (var entry : data.entrySet()) {
      var colName = entry.getKey();
      columns.add(Algebras.column(relName, colName, i));
      this.data.put(Row.asKey(relName, colName, i), Row.asValue(entry.getValue()));
      i++;
    }
    this.header = columns;
  }

  @Override
  public List<Algebra.Column> getHeader() {
    return List.copyOf(header);
  }

  @Override
  @Nonnull
  public Iterator<MapRow> iterator() {
    return List.of(new MapRow(typeSystem, Set.of(relName), data)).iterator();
  }
}
