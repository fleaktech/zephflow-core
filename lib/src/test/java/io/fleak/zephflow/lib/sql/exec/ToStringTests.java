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

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.lib.utils.JsonUtils;
import io.fleak.zephflow.lib.sql.SQLInterpreter;
import io.fleak.zephflow.lib.sql.TestSQLUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class ToStringTests {

  @Test
  public void testMapToString() {
    var row = runSQL("select family::text as family from events").toList().get(0);

    var family = JsonUtils.fromJsonString(row.asMap().get("family").toString(), new TypeReference<>() {});
    var expected = Map.of("wife", "Diane Sanchez","daughter", "Beth Smith","grandson","Morty Smith");

    Assertions.assertEquals(expected, family);
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
                        Map.of(
                            "family",
                            Map.of(
                                "wife",
                                "Diane Sanchez",
                                "daughter",
                                "Beth Smith",
                                "grandson",
                                "Morty Smith")))))),
        sql);
  }
}
