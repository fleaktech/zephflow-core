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
import io.fleak.zephflow.lib.sql.exec.types.TypeSystem;
import io.fleak.zephflow.lib.sql.rel.Algebra;
import java.util.Iterator;
import java.util.List;

public class SingleEmptyRowExec extends Table<Row> {

  private final TypeSystem typeSystem;

  public SingleEmptyRowExec(TypeSystem typeSystem) {
    this.typeSystem = typeSystem;
  }

  @Override
  public List<Algebra.Column> getHeader() {
    return List.of();
  }

  @Override
  public Iterator<Row> iterator() {
    return List.of(Row.empty(typeSystem)).iterator();
  }
}
