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
import io.fleak.zephflow.lib.sql.rel.Algebras;
import java.util.*;
import javax.annotation.Nonnull;

public class ExtendedTable<E> extends ExtTable {

  final Algebra.Expr<E> expr;
  final Interpreter<E> interpreter;

  final Algebra.Column column;
  final Lookup outerContext;

  public ExtendedTable(
      TypeSystem typeSystem,
      Table<Row> source,
      List<Algebra.Column> header,
      Interpreter<E> interpreter,
      String relName,
      String colName,
      int position,
      Algebra.Expr<E> expr,
      Lookup outerContext) {
    super(typeSystem, source, add(header, Algebras.column(relName, colName, position)));
    this.interpreter = interpreter;
    this.expr = expr;
    this.column = Algebras.column(relName, colName, position);
    this.outerContext = outerContext;
  }

  private static List<Algebra.Column> add(List<Algebra.Column> columns, Algebra.Column column) {
    var list = new ArrayList<Algebra.Column>(columns.size() + 1);
    list.addAll(columns);
    list.add(column);
    return list;
  }

  @Override
  @Nonnull
  public Iterator<Row> iterator() {
    return Streams.asStream(getSource().iterator())
        .map(
            r ->
                r.assocColumn(
                    Row.asKey(column.getRelName(), column.getName(), column.getSelectPosition()),
                    Row.asValue(
                        interpreter.eval(
                            new Lookup.MergedLookup(outerContext, r),
                            expr.getDatum(),
                            Object.class))))
        .iterator();
  }
}
