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

import io.fleak.zephflow.lib.sql.exec.CorrelatedIterable;
import io.fleak.zephflow.lib.sql.exec.Row;
import io.fleak.zephflow.lib.sql.exec.Table;
import io.fleak.zephflow.lib.sql.rel.Algebra;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;

public class CorrelatedTableScan extends Table<Row> {

  final String name;
  final List<Algebra.Column> header;
  final Row row;
  final CorrelatedIterable<Row> it;

  public CorrelatedTableScan(
      String name, List<Algebra.Column> header, Row row, CorrelatedIterable<Row> it) {
    this.name = name;
    this.header = header;
    this.row = row;
    this.it = it;
  }

  @Override
  @Nonnull
  public Iterator<Row> iterator() {
    return it.iterator(row);
  }

  @Override
  public List<Algebra.Column> getHeader() {
    return header;
  }
}
