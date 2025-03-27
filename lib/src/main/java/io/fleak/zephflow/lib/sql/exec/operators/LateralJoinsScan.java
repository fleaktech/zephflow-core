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

import io.fleak.zephflow.lib.sql.exec.*;
import io.fleak.zephflow.lib.sql.exec.types.TypeSystem;
import io.fleak.zephflow.lib.sql.exec.utils.Streams;
import io.fleak.zephflow.lib.sql.exec.utils.ZipLongestIterator;
import io.fleak.zephflow.lib.sql.rel.Algebra;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;

public class LateralJoinsScan<E> extends ExtTable {

  final RelationalInterpreter<E> relationalInterpreter;
  final List<Algebra.Column> header;
  final List<Algebra.Relation> laterals;
  final Catalog catalog;

  public LateralJoinsScan(
      RelationalInterpreter<E> relationalInterpreter,
      TypeSystem typeSystem,
      Table<Row> childSource,
      List<Algebra.Column> header,
      List<Algebra.Relation> laterals,
      Catalog catalog) {
    super(typeSystem, childSource, header);
    this.relationalInterpreter = relationalInterpreter;
    this.header = header;
    this.laterals = laterals;
    this.catalog = catalog;
  }

  @Override
  public List<Algebra.Column> getHeader() {
    return header;
  }

  @Override
  @Nonnull
  public Iterator<Row> iterator() {
    // For each row in the childSource we:
    //      interpret the laterals in the relationalInterpreter using the row as a single row
    // relation
    //
    return Streams.asStream(getSource().iterator())
        .flatMap(r -> Streams.asStream(callLaterals(r)))
        .iterator();
  }

  /** For each leftRow combine using ZipLongestIterator and filling with null */
  private Iterator<Row> callLaterals(Row leftRow) {
    List<Iterator<Row>> its = new ArrayList<>(laterals.size());

    for (Algebra.Relation lateralRelation : laterals) {
      its.add(
          relationalInterpreter
              .buildIterable(Algebra.lateralRelation(lateralRelation, leftRow), catalog, leftRow)
              .iterator());
    }
    var relationNames = new HashSet<String>();
    getHeader().forEach(h -> relationNames.add(h.getRelName()));

    final List<Row> constantPrefix = List.of(leftRow);
    return new ZipLongestIterator<>(
        () -> constantPrefix, its, rows -> CombinedTableRow.combineRows(relationNames, rows), null);
  }
}
