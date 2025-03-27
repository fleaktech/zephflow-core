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
package io.fleak.zephflow.lib.sql.exec.operators;

import io.fleak.zephflow.lib.sql.exec.ExtTable;
import io.fleak.zephflow.lib.sql.exec.Row;
import io.fleak.zephflow.lib.sql.exec.Table;
import io.fleak.zephflow.lib.sql.exec.types.TypeSystem;
import io.fleak.zephflow.lib.sql.exec.utils.Streams;
import io.fleak.zephflow.lib.sql.rel.Algebra;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public final class ProjectExec extends ExtTable {
  final List<Algebra.Column> columns;
  final Set<String> relNames;

  public ProjectExec(TypeSystem typeSystem, Table<Row> source, List<Algebra.Column> columns) {
    super(typeSystem, source, columns);
    this.columns = columns;
    this.relNames = columns.stream().map(Algebra.Column::getRelName).collect(Collectors.toSet());
  }

  @Override
  @Nonnull
  public Iterator<Row> iterator() {
    return Streams.asStream(getSource().iterator()).map(r -> projectRow(r, columns)).iterator();
  }

  private Row projectRow(Row r, List<Algebra.Column> columns) {
    var mm = new LinkedHashMap<Row.Key, Row.Value>();

    for (var col : columns) {
      if (col.isAllColumn()) {
        throw new RuntimeException("* columns not supported yet");
      } else {
        mm.put(
            // we set the name here instead of alias, alias is handled in the Rename operator
            Row.asKey(col.getRelName(), col.getName(), col.getSelectPosition()),
            Row.asValue(r.resolve(col, Object.class)));
      }
    }
    return Row.wrapMultiRelMap(getTypeSystem(), relNames, mm);
  }
}
