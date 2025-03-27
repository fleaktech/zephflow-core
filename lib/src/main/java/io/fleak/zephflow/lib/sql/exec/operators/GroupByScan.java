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
import io.fleak.zephflow.lib.sql.rel.Algebra;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class GroupByScan<E> extends ExtTable {

  final RelationalInterpreter<E> relationalInterpreter;
  final List<Algebra.Column> header;
  final Algebra.Column aggregateColumn;
  final Algebra.GroupBy<E> groupByExprs;
  final Catalog catalog;

  final Interpreter<E> interpreter;
  final Set<String> relNames;

  public GroupByScan(
      RelationalInterpreter<E> relationalInterpreter,
      Interpreter<E> interpreter,
      TypeSystem typeSystem,
      Table<Row> childSource,
      List<Algebra.Column> header,
      Algebra.Column aggregateColumn,
      Algebra.GroupBy<E> groupByExprs,
      Catalog catalog) {
    super(typeSystem, childSource, header);
    this.relationalInterpreter = relationalInterpreter;
    this.interpreter = interpreter;
    this.header = header;
    this.aggregateColumn = aggregateColumn;
    this.groupByExprs = groupByExprs;
    this.catalog = catalog;

    //    if (header.size() != groupByExprs.getHeader().size() +
    // groupByExprs.getExpressions().size()) {
    //      throw new RuntimeException(
    //          "header and groupByExprs columns+expressions must have the same size");
    //    }
    relNames = new HashSet<>();
    header.forEach(h -> relNames.add(h.getRelName()));
  }

  @Override
  @Nonnull
  public Iterator<Row> iterator() {
    // For each row in source, create a Map with keys provided by buildKeys, then for each key+value
    // build a row.
    // the Row built contains a column for each header column and a list of rows in the aggregate
    // column
    return Streams.asStream(getSource().iterator())
        .collect(Collectors.groupingBy(this::buildKeys))
        .entrySet()
        .stream()
        .map(e -> buildRow(e.getKey(), e.getValue()))
        .iterator();
  }

  @Override
  public List<Algebra.Column> getHeader() {
    return header;
  }

  /**
   * Takes a single grouping entry's keys and the value that falls under the grouping, then builds
   * out a Row where the columns are the header columns and the aggregation column.
   */
  private Row buildRow(List<Object> keys, List<Row> value) {
    if (keys.size() != header.size()) {
      throw new RuntimeException("Aggregate keys must be the same as the header keys");
    }

    var groupByEntries = new ArrayList<Row.Entry>(keys.size());

    // add each grouping key to the row using its corresponding header column to name it
    for (int i = 0; i < header.size(); i++) {
      var h = header.get(i);
      var k = keys.get(i);
      groupByEntries.add(
          Row.asEntry(
              Row.asKey(h.getRelName(), h.getName(), h.getSelectPosition()), Row.asValue(k)));
    }

    return new AggregateCombinedTableRow(getTypeSystem(), relNames, groupByEntries, value);
  }

  @SuppressWarnings("unchecked")
  private List<Object> buildKeys(Row row) {
    var ks = new ArrayList<>(header.size());
    groupByExprs
        .getColumns()
        .forEach(c -> ks.add(row.resolve(c.getRelName(), c.getName(), Object.class)));

    groupByExprs
        .getExpressions()
        .forEach(e -> ks.add(interpreter.eval(row, (E) e.getDatum(), Object.class)));

    return ks;
  }
}
