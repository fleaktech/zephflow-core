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

import io.fleak.zephflow.lib.sql.errors.SQLRuntimeError;
import io.fleak.zephflow.lib.sql.errors.SQLSyntaxError;
import io.fleak.zephflow.lib.sql.exec.Catalog;
import io.fleak.zephflow.lib.sql.exec.Row;
import io.fleak.zephflow.lib.sql.exec.Table;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ErrorMessagesTest {

  @Test
  public void testRuntimeErrorMessage() {
    var sql = "select 1/0 from records;";
    Assertions.assertThrows(SQLRuntimeError.class, () -> runSQL(sql).forEach(System.out::println));
  }

  @Test
  public void testSyntaxErrorMessage() {
    var sql = "select substring(value from 'text') as family from records;";
    Assertions.assertThrows(SQLSyntaxError.class, () -> runSQL(sql));
  }

  private static Stream<Row> runSQL(String sql) {
    var sqlInterpreter = SQLInterpreter.defaultInterpreter();
    var typeSystem = sqlInterpreter.getTypeSystem();

    return TestSQLUtils.runSQL(
        Catalog.fromMap(
            Map.of(
                "records",
                Table.ofListOfMaps(
                    typeSystem, "records", List.of(Map.of("name", "abc", "age", 30))))),
        sql);
  }
}
