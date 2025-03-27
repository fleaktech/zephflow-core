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
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;

public class RestrictTable<E> extends ExtTable {

  final Algebra.Expr<E> expr;
  final Interpreter<E> interpreter;
  final Lookup outerContext;

  public RestrictTable(
      TypeSystem typeSystem,
      Table<Row> source,
      List<Algebra.Column> header,
      Interpreter<E> interpreter,
      Algebra.Expr<E> expr,
      Lookup outerContext) {
    super(typeSystem, source, header);
    this.interpreter = interpreter;
    this.expr = expr;
    this.outerContext = outerContext;
  }

  @Override
  @Nonnull
  public Iterator<Row> iterator() {
    return Streams.asStream(getSource().iterator())
        .filter(
            r ->
                nullAsFalse(
                    interpreter.eval(
                        new Lookup.MergedLookup(outerContext, r), expr.getDatum(), Boolean.class)))
        .iterator();
  }

  private static boolean nullAsFalse(Boolean eval) {
    return eval != null && eval;
  }
}
