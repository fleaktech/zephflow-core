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
import io.fleak.zephflow.lib.sql.exec.functions.AggregateFunction;
import io.fleak.zephflow.lib.sql.exec.types.TypeSystem;
import io.fleak.zephflow.lib.sql.exec.utils.Streams;
import io.fleak.zephflow.lib.sql.rel.Algebra;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

public class AggregateScan<E> extends ExtTable {

  final List<Algebra.Column> header;
  final Catalog catalog;
  final Interpreter<E> interpreter;
  final Algebra.Aggregate<E> aggregate;
  final Set<String> relNames;

  public AggregateScan(
      Interpreter<E> interpreter,
      TypeSystem typeSystem,
      Table<Row> childSource,
      List<Algebra.Column> header,
      Algebra.Aggregate<E> aggregate,
      Catalog catalog) {
    super(typeSystem, childSource, header);
    this.interpreter = interpreter;
    this.header = header;
    this.catalog = catalog;
    this.aggregate = aggregate;
    this.relNames = header.stream().map(Algebra.Column::getRelName).collect(Collectors.toSet());
  }

  @Override
  @Nonnull
  public Iterator<Row> iterator() {
    return Streams.asStream(getSource().iterator()).map(this::extendWithAggregations).iterator();
  }

  @SuppressWarnings("all")
  private Row extendWithAggregations(Row row) {
    // Note on distict:
    // we are applying distinct in the AggregateScan, this leaves the aggregate functions free to
    // just implement their logic
    // and not keep track of uniqueness. Also for more performant choices the Scanner is more
    // capable and hos access to the wider execution system.

    if (!(row instanceof AggregateRow)) {
      throw new RuntimeException("aggregates must be used together with group by clauses");
    }
    var resultRow = row;

    var aggRow = (AggregateRow) row;
    for (var fn : aggregate.getAggregateFuncs()) {
      /* ------- Evaluate Aggregate ---- */

      var aggFn =
          (AggregateFunction) getTypeSystem().lookupFunction(fn.getFuncCall().getFunctionName());

      var state = aggFn.initState();

      // we could add a performance option here if we only use column names,
      // but if expressions are used e.g. len(name) then we need to evaluate it first.
      var innerRowsIt = aggRow.groupedRows();

      var keysSeen = new HashSet<>();
      var isDistinct = fn.getFuncCall().isDistinct();

      while (innerRowsIt.hasNext()) {
        var innerRow = innerRowsIt.next();
        var data = evalArgs((List<E>) fn.getFuncCall().getArgs(), innerRow);
        // here we have the expression data
        // we can use it as a key to see if the row was seen or not
        var dataList = data.collect(Collectors.toList());
        if (!isDistinct || keysSeen.add(dataList)) {
          state = aggFn.update(state, dataList);
        }
      }

      var result = aggFn.getResult(state);
      /* ------- End Evaluate Aggregate ---- */

      // add the result to the row
      var aggEntry =
          Row.asEntry(Row.asKey(fn.getExtendRelName(), fn.getName(), -1), Row.asValue(result));
      resultRow =
          Row.combineRows(
              relNames, resultRow, Row.wrap(getTypeSystem(), relNames, List.of(aggEntry)));
    }

    return resultRow;
  }

  private Stream<Object> evalArgs(List<E> expressions, Row innerRow) {
    return expressions.stream().map(e -> interpreter.eval(innerRow, e, Object.class));
  }

  @Override
  public List<Algebra.Column> getHeader() {
    return header;
  }
}
