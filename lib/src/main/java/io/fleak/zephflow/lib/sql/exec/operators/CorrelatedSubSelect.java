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
import io.fleak.zephflow.lib.sql.rel.Algebra;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class CorrelatedSubSelect<E> extends Table<Row> implements CorrelatedIterable<Row> {

  private final TypeSystem typeSystem;
  private final RelationalInterpreter<E> interpreter;
  private final Algebra.LateralViewTableRelation relation;
  private final Set<String> relationNames;
  private final Catalog catalog;

  public CorrelatedSubSelect(
      Algebra.LateralViewTableRelation relation,
      RelationalInterpreter<E> interpreter,
      TypeSystem typeSystem,
      Catalog catalog) {
    this.relation = relation;
    this.interpreter = interpreter;
    this.typeSystem = typeSystem;
    this.catalog = catalog;
    this.relationNames =
        relation.getHeader().stream().map(Algebra.Column::getRelName).collect(Collectors.toSet());
  }

  @Override
  public List<Algebra.Column> getHeader() {
    return relation.getHeader();
  }

  @Override
  @Nonnull
  public Iterator<Row> iterator() {
    throw new UnsupportedOperationException("correlated function calls are not supported");
  }

  @Override
  public Iterator<Row> iterator(Row row) {
    var execPlan = interpreter.buildIterable(relation.getRelation(), catalog, row);
    return execPlan.iterator();
  }
}
