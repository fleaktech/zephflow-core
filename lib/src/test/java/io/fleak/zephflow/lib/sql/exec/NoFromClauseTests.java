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

import io.fleak.zephflow.lib.sql.SQLInterpreter;
import io.fleak.zephflow.lib.sql.TestSQLUtils;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NoFromClauseTests {

  @Test
  public void expressionsAndNoFromClause() {

    var rows = TestSQLUtils.runSQL(Catalog.fromMap(Map.of()), "select 1+1, 2*3 as b;").toList();

    Assertions.assertFalse(rows.isEmpty());

    var row = rows.get(0);
    var rowMap = row.asMap();

    Assertions.assertEquals(2L, rowMap.get("col_1"));
    Assertions.assertEquals(6L, rowMap.get("b"));
  }

  @Test
  public void constantNoFromClause() {
    var rows = TestSQLUtils.runSQL(Catalog.fromMap(Map.of()), "select 1;").toList();
    Assertions.assertFalse(rows.isEmpty());
    var row = rows.get(0);
  }

  private static Stream<Row> runSQL(String sql) {
    var sqlInterpreter = SQLInterpreter.defaultInterpreter();
    var typeSystem = sqlInterpreter.getTypeSystem();

    return TestSQLUtils.runSQL(
        Catalog.fromMap(
            Map.of(
                "events",
                Table.ofListOfMaps(
                    typeSystem, "events", List.of(Map.of("name", "abc", "age", 30))))),
        sql);
  }
}
