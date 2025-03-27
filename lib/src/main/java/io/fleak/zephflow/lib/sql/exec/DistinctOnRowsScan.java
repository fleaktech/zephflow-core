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
import io.fleak.zephflow.lib.sql.exec.types.TypeSystem;
import io.fleak.zephflow.lib.sql.rel.Algebra;
import java.util.*;
import org.apache.commons.collections4.IterableUtils;

public class DistinctOnRowsScan extends Table<Row> {
  private final Interpreter exprInterpreter;
  private final TypeSystem typeSystem;
  private final Table<Row> source;
  private final List<QueryAST.Datum> args;

  public <E> DistinctOnRowsScan(
      Interpreter exprInterpreter,
      TypeSystem typeSystem,
      Table<Row> source,
      List<QueryAST.Datum> args) {
    this.exprInterpreter = exprInterpreter;
    this.typeSystem = typeSystem;
    this.source = source;
    this.args = Objects.requireNonNull(args);
  }

  @Override
  public List<Algebra.Column> getHeader() {
    return source.getHeader();
  }

  @Override
  public Iterator<Row> iterator() {

    Set hashSet = new HashSet<>();

    return IterableUtils.filteredIterable(source, (row) -> hashSet.add(evalArgs(row, args)))
        .iterator();
  }

  private List<Object> evalArgs(Row row, List<QueryAST.Datum> args) {
    var argValues = new ArrayList(args.size());

    for (int i = 0; i < args.size(); i++) {
      argValues.add(exprInterpreter.eval(row, args.get(i), Object.class));
    }
    return argValues;
  }
}
