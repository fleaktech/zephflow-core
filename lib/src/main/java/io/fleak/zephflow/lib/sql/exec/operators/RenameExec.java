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

import io.fleak.zephflow.lib.sql.exec.ExtTable;
import io.fleak.zephflow.lib.sql.exec.Row;
import io.fleak.zephflow.lib.sql.exec.Table;
import io.fleak.zephflow.lib.sql.exec.types.TypeSystem;
import io.fleak.zephflow.lib.sql.exec.utils.Streams;
import io.fleak.zephflow.lib.sql.rel.Algebra;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;

public final class RenameExec extends ExtTable {

  final List<Algebra.RenameField> renames;
  final List<Algebra.Column> oldHeader;

  public RenameExec(TypeSystem typeSystem, Table<Row> source, List<Algebra.RenameField> renames) {
    super(typeSystem, source, renameHeader(source.getHeader(), renames));
    this.renames = renames;
    this.oldHeader = getHeader();
  }

  private static List<Algebra.Column> renameHeader(
      List<Algebra.Column> header, List<Algebra.RenameField> renames) {
    // @TODO find a more efficient way to identify the columns to rename
    List<Algebra.Column> newColumns = new ArrayList<>();

    for (var headerCol : header) {
      Algebra.RenameField renameField = findRenameField(headerCol, renames);
      if (renameField != null) newColumns.add(headerCol.rename(renameField));
      else newColumns.add(headerCol);
    }
    return newColumns;
  }

  private static Algebra.RenameField findRenameField(
      Algebra.Column col, List<Algebra.RenameField> renames) {
    for (var f : renames) {
      if (col.canRename(f)) return f;
    }

    return null;
  }

  @Override
  @Nonnull
  public Iterator<Row> iterator() {
    return Streams.asStream(getSource().iterator()).map(r -> r.renameColumns(renames)).iterator();
  }
}
