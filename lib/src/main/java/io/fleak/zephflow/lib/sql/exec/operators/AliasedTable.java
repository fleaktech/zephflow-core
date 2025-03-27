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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class AliasedTable extends ExtTable {

  final String alias;
  final Table<Row> source;

  public AliasedTable(TypeSystem typeSystem, String alias, Table<Row> source) {
    super(
        typeSystem,
        source,
        source.getHeader().stream().map(h -> h.withAlias(alias)).collect(Collectors.toList()));
    this.alias = alias;
    this.source = source;
  }

  @Override
  @Nonnull
  public Iterator<Row> iterator() {
    return Streams.asStream(source.iterator()).map(this::renameAliases).iterator();
  }

  private Row renameAliases(Row r) {
    var m = new LinkedHashMap<Row.Key, Row.Value>();
    for (var entry : r.getEntries()) {
      var k = entry.key();
      m.put(Row.asKey(alias, k.name(), k.relativePosition()), entry.value());
    }
    return Row.wrapMap(getTypeSystem(), Set.of(alias), m);
  }
}
