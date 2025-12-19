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

public class JsonBuildObjectTest {

  @Test
  public void buildObject1() {
    var rows =
        runSQL(
                "select json_build_object('schedule_id', '1ffacd6c-c813-4f57-9dcb-ffcc2d208030', 'owner_team', '', 'rotations', 'test') from records")
            .toList();

    for (var row : rows) {
      System.out.println(row);
    }
  }

  @Test
  public void buildObject2() {
    var rows = runSQL("select json_build_object('user', user) user from records").toList();

    for (var row : rows) {
      Assertions.assertTrue(row.asMap().get("user") instanceof Map);
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
                        Map.of("name", "abc", "id", 1, "user", Map.of("name", "abc")),
                        Map.of("name", "edf", "id", 1, "user", Map.of("name", "edf")),
                        Map.of("name", "ghi", "id", 1, "user", Map.of("name", "ghi")))),
                "ids",
                Table.ofListOfMaps(typeSystem, "ids", List.of(Map.of("id", 1), Map.of("id", 2))))),
        sql);
  }
}
