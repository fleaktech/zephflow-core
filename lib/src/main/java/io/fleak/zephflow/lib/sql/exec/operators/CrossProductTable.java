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

import io.fleak.zephflow.lib.sql.exec.Row;
import io.fleak.zephflow.lib.sql.exec.Table;
import io.fleak.zephflow.lib.sql.exec.utils.Streams;
import io.fleak.zephflow.lib.sql.rel.Algebra;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

// TODO Test cross product
public class CrossProductTable extends Table<Row> {

  final List<Table<Row>> childSources;
  private final List<Algebra.Column> header;

  public CrossProductTable(List<Table<Row>> childSources) {
    this.childSources = childSources;
    this.header =
        childSources.stream().flatMap(s -> s.getHeader().stream()).collect(Collectors.toList());
  }

  @Override
  public List<Algebra.Column> getHeader() {
    return header;
  }

  @Override
  @Nonnull
  public Iterator<Row> iterator() {
    if (childSources.isEmpty()) return Collections.emptyIterator();

    return crossProduct(childSources.get(0), childSources.subList(1, childSources.size()))
        .iterator();
  }

  private Stream<Row> crossProduct(Table<Row> tbl1, List<Table<Row>> tbls) {
    var relationNames = new HashSet<String>();
    tbl1.getHeader().forEach(h -> relationNames.add(h.getRelName()));
    tbls.forEach(
        tbl ->
            relationNames.addAll(
                tbl.getHeader().stream()
                    .map(Algebra.Column::getRelName)
                    .collect(Collectors.toSet())));

    if (!tbls.isEmpty()) {
      return Streams.asStream(tbl1.iterator())
          .flatMap(
              r ->
                  mergeToRow(
                      relationNames, r, crossProduct(tbls.get(0), tbls.subList(1, tbls.size()))));
    }

    return Streams.asStream(tbl1.iterator());
  }

  private Stream<Row> mergeToRow(Set<String> relationNames, Row r1, Stream<Row> rowStream) {
    return rowStream.map(r2 -> Row.combineRows(relationNames, r1, r2));
  }
}
