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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class OrderByTest {

    @Test
    public void testOrderByNoDirection() {
        var rows = runSQL("select * from events order by name").toList();
        Assertions.assertEquals(3, rows.size());

        Assertions.assertEquals("abc", rows.get(0).asMap().get("name"));
        Assertions.assertEquals("edf", rows.get(1).asMap().get("name"));
        Assertions.assertEquals("ghi", rows.get(2).asMap().get("name"));
    }

    @Test
    public void testOrderByNoDirectionMultipleColumns() {
        var rows = runSQL("select * from events order by name, id + 1").toList();
        Assertions.assertEquals(3, rows.size());

        Assertions.assertEquals("abc", rows.get(0).asMap().get("name"));
        Assertions.assertEquals("edf", rows.get(1).asMap().get("name"));
        Assertions.assertEquals("ghi", rows.get(2).asMap().get("name"));
    }

    @Test
    public void testOrderByCol2DescMultipleColumns() {
        var rows = runSQL("select * from events order by name, id + 1 desc").toList();
        Assertions.assertEquals(3, rows.size());

        Assertions.assertEquals("abc", rows.get(0).asMap().get("name"));
        Assertions.assertEquals("edf", rows.get(1).asMap().get("name"));
        Assertions.assertEquals("ghi", rows.get(2).asMap().get("name"));
    }

    @Test
    public void testOrderByCol2AscMultipleColumns() {
        var rows = runSQL("select * from events order by name, id + 1 asc").toList();
        Assertions.assertEquals(3, rows.size());

        Assertions.assertEquals("abc", rows.get(0).asMap().get("name"));
        Assertions.assertEquals("edf", rows.get(1).asMap().get("name"));
        Assertions.assertEquals("ghi", rows.get(2).asMap().get("name"));
    }

    @Test
    public void testOrderByAsc() {
        var rows = runSQL("select * from events order by name asc").toList();
        Assertions.assertEquals(3, rows.size());

        Assertions.assertEquals("abc", rows.get(0).asMap().get("name"));
        Assertions.assertEquals("edf", rows.get(1).asMap().get("name"));
        Assertions.assertEquals("ghi", rows.get(2).asMap().get("name"));
    }

    @Test
    public void testOrderByASC() {
        var rows = runSQL("select * from events order by name desc").toList();
        Assertions.assertEquals(3, rows.size());

        Assertions.assertEquals("ghi", rows.get(0).asMap().get("name"));
        Assertions.assertEquals("edf", rows.get(1).asMap().get("name"));
        Assertions.assertEquals("abc", rows.get(2).asMap().get("name"));
    }

    @Test
    public void testOrderBySelectColumn() {
        var rows = runSQL("select name as n from events order by name desc").toList();
        Assertions.assertEquals(3, rows.size());

        Assertions.assertEquals("ghi", rows.get(0).asMap().get("n"));
        Assertions.assertEquals("edf", rows.get(1).asMap().get("n"));
        Assertions.assertEquals("abc", rows.get(2).asMap().get("n"));
    }

    @Test
    public void testOrderBySelectColumnAndNonSelect() {
        var rows = runSQL("select name as n from events order by name desc").toList();
        Assertions.assertEquals(3, rows.size());

        Assertions.assertEquals("ghi", rows.get(0).asMap().get("n"));
        Assertions.assertEquals("edf", rows.get(1).asMap().get("n"));
        Assertions.assertEquals("abc", rows.get(2).asMap().get("n"));
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
                                                Map.of("name", "edf", "id", 3),
                                                Map.of("name", "ghi", "id", 2)
                                        )),
                                "ids",
                                Table.ofListOfMaps(
                                        typeSystem,
                                        "ids",
                                        List.of(
                                                Map.of("id", 1),
                                                Map.of("id", 2)
                                        )))),
                sql);
    }
}
