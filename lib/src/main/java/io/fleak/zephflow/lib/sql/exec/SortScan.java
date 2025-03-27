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

import io.fleak.zephflow.lib.sql.ast.QueryAST;
import io.fleak.zephflow.lib.sql.errors.SQLSyntaxError;
import io.fleak.zephflow.lib.sql.exec.types.TypeSystem;
import io.fleak.zephflow.lib.sql.exec.utils.Streams;
import io.fleak.zephflow.lib.sql.rel.Algebra;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class SortScan extends Table<Row> {

  private final Interpreter exprInterpreter;
  private final TypeSystem typeSystem;
  private final Table<Row> childSource;
  private final List<QueryAST.Datum> expressions;

  private final Comparator<Row> comparator;

  public SortScan(
      Interpreter exprInterpreter,
      TypeSystem typeSystem,
      Table<Row> childSource,
      List<QueryAST.Datum> expressions) {
    this.exprInterpreter = exprInterpreter;
    this.typeSystem = typeSystem;
    this.childSource = childSource;
    this.expressions = expressions;

    // create a chain of comparators, one for each expression in the order by
    Comparator<Row> comp = null;
    if (expressions.isEmpty()) {
      comp = Comparator.comparing((r) -> 1);
    } else {
      for (var expr : expressions) {
        var exprComp =
            Comparator.nullsFirst(Comparator.<Row, Comparable>comparing(row -> eval(row, expr)));
        if (!expr.isAsc()) {
          exprComp = exprComp.reversed();
        }
        comp = comp == null ? exprComp : comp.thenComparing(exprComp);
      }
    }

    comparator = comp;
  }

  private Comparable eval(Row row, QueryAST.Datum expr) {
    try {
      var v = (Comparable) exprInterpreter.eval(row, expr, Object.class);
      if (v == null) {
        return Integer.MAX_VALUE;
      }
      return v;
    } catch (ClassCastException e) {
      throw new SQLSyntaxError("order by columns can only be strings, numbers or booleans", e);
    }
  }

  @Override
  public List<Algebra.Column> getHeader() {
    return childSource.getHeader();
  }

  @Override
  public Iterator<Row> iterator() {
    return Streams.asStream(childSource.iterator()).sorted(comparator).iterator();
  }
}
