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
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class CaseSensitivityTest {

  @Test
  public void caseTables() {
    var rows = runSQL("select id col1 from EventS").toList();
    Assertions.assertEquals(3, rows.size());

    var resultSet = Set.of(1, 2, 3);
    for (var row : rows) {
      Assertions.assertTrue(resultSet.contains(row.asMap().get("col1")));
    }
  }

  @Test
  public void caseColumns() {
    var rows = runSQL("select Id col1 from events").toList();
    Assertions.assertEquals(3, rows.size());

    var resultSet = Set.of(1, 2, 3);
    for (var row : rows) {
      Assertions.assertTrue(resultSet.contains(row.asMap().get("col1")));
    }
  }

  private static Stream<Row> runSQL(String sql) {
    var sqlInterpreter = SQLInterpreter.defaultInterpreter();
    var typeSystem = sqlInterpreter.getTypeSystem();

    return TestSQLUtils.runSQL(
        Catalog.fromMap(
            Map.of(
                "events",
                Table.ofListOfMaps(
                    typeSystem,
                    "events",
                    List.of(
                        Map.of("name", "abc", "id", 1),
                        Map.of("name", "edf", "id", 1),
                        Map.of("name", "ghi", "id", 1))),
                "ids",
                Table.ofListOfMaps(typeSystem, "ids", List.of(Map.of("id", 1), Map.of("id", 2))))),
        sql);
  }
}
