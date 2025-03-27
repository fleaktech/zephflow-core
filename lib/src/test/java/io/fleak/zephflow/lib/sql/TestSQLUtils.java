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
package io.fleak.zephflow.lib.sql;

import io.fleak.zephflow.lib.sql.exec.Catalog;
import io.fleak.zephflow.lib.sql.exec.Row;
import io.fleak.zephflow.lib.sql.exec.Table;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class TestSQLUtils {

  /**
   * Uses the default interpreter and type system to run sql queries
   */
  public static Stream<Row> runSQL(Catalog catalog, String sql) {
    var sqlInterpreter = SQLInterpreter.defaultInterpreter();
    var typeSystem = sqlInterpreter.getTypeSystem();

    var query = sqlInterpreter.compileQuery(sql);

    return sqlInterpreter.eval(catalog, query);
  }
}
