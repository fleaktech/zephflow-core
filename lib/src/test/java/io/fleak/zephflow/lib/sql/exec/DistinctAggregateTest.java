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

public class DistinctAggregateTest {

  @Test
  public void uniqueAggregates() {
    var rows =
        runSQL(
                "select sum(distinct id) uq_sum, count(distinct id) uq_count, sum(id) sum, count(id) count from records group by 1")
            .toList();
    Assertions.assertEquals(1, rows.size());

    for (var row : rows) {

      var m = row.asMap();
      var uq_sum = m.get("uq_sum");
      var uq_count = m.get("uq_count");
      var sum = m.get("sum");
      var count = m.get("count");

      Assertions.assertEquals(3L, uq_sum);
      Assertions.assertEquals(2L, uq_count);
      Assertions.assertEquals(3L, count);
      Assertions.assertEquals(5L, sum);
    }
  }

  @Test
  public void distinctGlobal() {
    var rows = runSQL("select distinct id from records").toList();
    Assertions.assertEquals(2, rows.size());

    for (var row : rows) {

      var m = row.asMap();
      var id = ((Number) m.get("id")).intValue();
      Assertions.assertTrue(id == 1 || id == 2);
    }
  }

  @Test
  public void distinctOn1() {
    var rows = runSQL("select distinct on (id) id, name from records").toList();
    Assertions.assertEquals(2, rows.size());

    for (var row : rows) {

      var m = row.asMap();
      var id = ((Number) m.get("id")).intValue();
      Assertions.assertTrue(id == 1 || id == 2);
    }
  }

  @Test
  public void distinctOn2() {
    var rows = runSQL("select distinct on (id, name) id, name from records").toList();
    Assertions.assertEquals(3, rows.size());

    for (var row : rows) {

      var m = row.asMap();
      var id = ((Number) m.get("id")).intValue();
      Assertions.assertTrue(id == 1 || id == 2);
    }
  }

  private static Stream<Row> runSQL(String sql) {
    var sqlInterpreter = SQLInterpreter.defaultInterpreter();
    var typeSystem = sqlInterpreter.getTypeSystem();

    return TestSQLUtils.runSQL(
        Catalog.fromMap(
            Map.of(
                "records",
                Table.ofListOfMaps(
                    typeSystem,
                    "records",
                    List.of(
                        Map.of("name", "abc", "id", 1),
                        Map.of("name", "edf", "id", 2),
                        Map.of("name", "ghi", "id", 2))),
                "ids",
                Table.ofListOfMaps(typeSystem, "ids", List.of(Map.of("id", 1), Map.of("id", 2))))),
        sql);
  }
}
