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
import io.fleak.zephflow.lib.sql.rel.Algebra;

public interface Lookup {

  /**
   * Lookup a column value in a relation. The type is cast to a type of clz. If the cast cannot be
   * done we raise a ClassCastException.
   *
   * @param rel The relation the column belongs to
   * @param col The column name
   * @param clz The type expected
   * @return null if the value cannot be found
   */
  <T> T resolve(String rel, String col, Class<T> clz);

  default <T> T resolve(Algebra.Column column, Class<T> clz) {
    return resolve(column.getRelName(), column.getName(), clz);
  }

  default <T> T resolve(QueryAST.Column column, Class<T> clz) {
    return resolve(column.getRelName(), column.getName(), clz);
  }

  class MergedLookup implements Lookup {
    final Lookup outer;
    final Lookup inner;

    public MergedLookup(Lookup outer, Lookup inner) {
      this.outer = outer;
      this.inner = inner;
    }

    public <T> T resolve(String rel, String col, Class<T> clz) {
      T ret = null;
      if (outer == null) return inner.resolve(rel, col, clz);

      try {
        ret = inner.resolve(rel, col, clz);
      } catch (Throwable t) {
      }

      if (ret == null) {
        ret = outer.resolve(rel, col, clz);
      }

      return ret;
    }
  }
}
