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

import io.fleak.zephflow.lib.sql.ast.QueryAST;
import io.fleak.zephflow.lib.sql.exec.*;
import io.fleak.zephflow.lib.sql.exec.types.TypeSystem;
import io.fleak.zephflow.lib.sql.exec.utils.Streams;
import io.fleak.zephflow.lib.sql.rel.Algebra;
import io.fleak.zephflow.lib.sql.rel.Algebras;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class CorrelatedFunctionCall<E> extends Table<Row> implements CorrelatedIterable<Row> {

  private final TypeSystem typeSystem;
  private final Algebra.SetReturningFunc srf;
  private final Interpreter<E> exprInterpreter;
  private final Set<String> relationNames;

  public CorrelatedFunctionCall(
      Algebra.SetReturningFunc srf, Interpreter<E> exprInterpreter, TypeSystem typeSystem) {
    this.srf = srf;
    this.exprInterpreter = exprInterpreter;
    this.typeSystem = typeSystem;
    this.relationNames =
        srf.getHeader().stream().map(Algebra.Column::getRelName).collect(Collectors.toSet());
  }

  @Override
  public List<Algebra.Column> getHeader() {
    return srf.getFuncFrom().getSchema().stream()
        .map(Algebras::column)
        .collect(Collectors.toList());
  }

  @Override
  @Nonnull
  public Iterator<Row> iterator() {
    throw new UnsupportedOperationException("correlated function calls are not supported");
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterator<Row> iterator(Row row) {
    var res = exprInterpreter.eval(row, (E) srf.getFuncFrom().getFuncCall(), Iterable.class);
    if (res == null) return Collections.emptyIterator();
    return Streams.asStream(res.iterator())
        .map(l -> wrapAsRow(l, srf.getFuncFrom().getSchema()))
        .iterator();
  }

  private Row wrapAsRow(Object obj, List<QueryAST.Column> schema) {

    if (obj instanceof Collection<?> objColl && schema.size() > 1) {
      // when we have a collection and multiple schema values, we try to fit the collection to the
      // schema in order
      int i = 0;
      var m = new LinkedHashMap<Row.Key, Row.Value>();
      for (Object o : objColl) {
        if (i < schema.size()) {
          var col = schema.get(i);
          m.put(Row.asKey(col.getRelName(), col.getName(), 0), Row.asValue(o));
        }
        i++;
      }
      return MapRow.wrapMap(typeSystem, relationNames, m);
    }

    if (schema.size() == 1) {
      // when a single schema column is provided we wrap the whole return value with the schema
      var m = new LinkedHashMap<Row.Key, Row.Value>();
      var col = schema.get(0);
      m.put(Row.asKey(col.getRelName(), col.getName(), 0), Row.asValue(obj));
      return MapRow.wrapMap(typeSystem, relationNames, m);
    }
    throw new RuntimeException("null schemas are not supported for lateral join function calls");
  }
}
