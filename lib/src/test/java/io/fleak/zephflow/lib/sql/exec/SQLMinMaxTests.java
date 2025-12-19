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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SQLMinMaxTests {

  @Test
  public void maxText() {
    var sqlInterpreter = SQLInterpreter.defaultInterpreter();
    var typeSystem = sqlInterpreter.getTypeSystem();

    var res =
        TestSQLUtils.runSQL(
            Catalog.fromMap(
                Map.of(
                    "records",
                    Table.ofListOfMaps(
                        typeSystem,
                        "records",
                        List.of(
                            Map.of("name", "abc", "age", 10),
                            Map.of("name", "abc", "age", 20),
                            Map.of("name", "abc", "age", 30))))),
            "select max(age) as age from records group by name");

    var maxAgeEntry =
        Arrays.stream(res.findFirst().get().getEntries())
            .filter(e -> e.key().name().equals("age"))
            .findFirst()
            .get();

    Assertions.assertNotNull(maxAgeEntry);
    Assertions.assertEquals(30L, ((Number) maxAgeEntry.value().asObject()).longValue());
  }

  @Test
  public void minText() {
    var sqlInterpreter = SQLInterpreter.defaultInterpreter();
    var typeSystem = sqlInterpreter.getTypeSystem();

    var res =
        TestSQLUtils.runSQL(
            Catalog.fromMap(
                Map.of(
                    "records",
                    Table.ofListOfMaps(
                        typeSystem,
                        "records",
                        List.of(
                            Map.of("name", "abc", "age", 10),
                            Map.of("name", "abc", "age", 20),
                            Map.of("name", "abc", "age", 30))))),
            "select min(age) as age from records group by name");

    var maxAgeEntry =
        Arrays.stream(res.findFirst().get().getEntries())
            .filter(e -> e.key().name().equals("age"))
            .findFirst()
            .get();

    Assertions.assertNotNull(maxAgeEntry);
    Assertions.assertEquals(10L, ((Number) maxAgeEntry.value().asObject()).longValue());
  }
}
