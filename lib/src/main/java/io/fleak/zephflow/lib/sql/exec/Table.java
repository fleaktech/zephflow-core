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

public abstract class Table<T extends Row> implements Iterable<T> {

  public abstract List<Algebra.Column> getHeader();

  public static Table<Row> ofListOfMaps(
      TypeSystem typeSystem, String relName, List<Map<String, Object>> data) {
    int i = 0;
    var columns = new ArrayList<Algebra.Column>();
    var rows = new ArrayList<Row>();

    // create columns
    for (var item : data) {
      for (var entry : item.entrySet()) {
        var colName = entry.getKey();
        columns.add(Algebras.column(relName, colName, i));
        i++;
      }
      break;
    }

    // create rows
    i = 0;
    for (var item : data) {
      var map = new LinkedHashMap<Row.Key, Row.Value>();
      for (var entry : item.entrySet()) {
        var colName = entry.getKey();
        map.put(Row.asKey(relName, colName, i), Row.asValue(entry.getValue()));
        i++;
      }
      rows.add(Row.wrapMap(typeSystem, Set.of(relName), map));
    }

    return new ListMapTable(columns, rows);
  }

  public static Table<MapRow> ofMap(TypeSystem typeSystem, String relName, Map<String, ?> data) {
    return new MapTable(typeSystem, relName, new TreeMap<>(data));
  }
}
