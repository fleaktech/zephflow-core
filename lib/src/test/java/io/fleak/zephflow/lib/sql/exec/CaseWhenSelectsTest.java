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

import static java.util.Map.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.fleak.zephflow.lib.sql.SQLInterpreter;
import io.fleak.zephflow.lib.sql.TestSQLUtils;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CaseWhenSelectsTest {

  @ParameterizedTest
  @MethodSource("caseWhenSQLQueries")
  public void testCaseWhenSQLQueries(String sql) {
    var rows = runSQL(sql).toList();
    System.out.println(sql.lines().findFirst().get());

    for (var row : rows) {
      System.out.println(JsonUtils.toJsonString(row.asMap()));
    }
  }

  private static Stream<Arguments> caseWhenSQLQueries() {
    return Stream.of(
        Arguments.of(
            """
                        -- Test Case for Constants and Strings
                        SELECT
                            name,
                            id,
                            CASE
                                WHEN id = 1 THEN 'One'
                                ELSE 'Other'
                            END AS id_description
                        FROM events;
                        """),
        Arguments.of(
            """
                        -- Test Case Using Functions
                        SELECT
                            name,
                            id,
                            CASE
                                WHEN UPPER(name) = 'ABC' THEN 'Starts with ABC'
                                WHEN LENGTH(name) > 3 THEN 'Long Name'
                                ELSE 'Short or Unmatched'
                            END AS name_analysis
                        FROM events;
                        """),
        Arguments.of(
            """
                        -- Test Case Using Comparison Operators
                        SELECT
                            name,
                            id,
                            CASE
                                WHEN id > 1 OR name LIKE '%f' THEN 'ID greater than 1 or ends with f'
                            END AS comparison_test
                        FROM events;
                        """),
        Arguments.of(
            """
                        -- Test Case with Multiple Conditions in a Single WHEN Clause
                        SELECT
                            name,
                            id,
                            CASE
                                WHEN id = 1 AND LENGTH(name) = 3 AND name LIKE '%i' THEN 'Specific Match'
                                ELSE 'No Match'
                            END AS multi_condition_test
                        FROM events;
                        """),
        Arguments.of(
            """
                        -- Test Case with 4-5 WHEN Statements
                        SELECT
                            name,
                            id,
                            CASE
                                WHEN id = 1 AND name = 'abc' THEN 'Condition 1 Met'
                                WHEN id = 1 AND name = 'edf' THEN 'Condition 2 Met'
                                WHEN id = 1 AND name = 'ghi' THEN 'Condition 3 Met'
                                WHEN id > 1 THEN 'Condition 4 Met'
                                ELSE 'No Conditions Met'
                            END AS multiple_when_test
                        FROM events;
                        """),
        Arguments.of(
            """
                -- Edge Case: Complex Conditions with Arithmetic
                SELECT
                    name,
                    id,
                    CASE
                        WHEN id + 1 = 2 THEN 'ID plus 1 equals 2'
                        WHEN id * 2 = 4 THEN 'ID times 2 equals 4'
                        ELSE 'No Match'
                    END AS arithmetic_test
                FROM events;
                """),
        Arguments.of(
            """
                -- Edge Case: Columns not found
                SELECT
                    name,
                    id,
                    CASE
                        WHEN bla + 1 = 2 THEN 'ID plus 1 equals 2'
                        WHEN bla * 2 = 4 THEN 'ID times 2 equals 4'
                        ELSE 'No Match'
                    END AS arithmetic_test
                FROM events;
                """));
  }

  @Test
  public void caseWhenSimpleSelect() {
    var rows =
        runSQL(
                "select case when name like 'ab%' then 1 when name like 'ed%' then 2 else 3 end col1 from events")
            .toList();
    assertEquals(3, rows.size());

    var resultSet = Set.of(1L, 2L, 3L);
    for (var row : rows) {
      assertTrue(resultSet.contains(row.asMap().get("col1")));
    }
  }

  @Test
  public void caseWhenSimpleSelect2() {
    var rows = runSQL("select case when 1 then 0 end col1 from events").toList();
    assertEquals(3, rows.size());

    for (var row : rows) {
      assertEquals(0L, row.asMap().get("col1"));
    }
  }

  @Test
  public void caseWhenSimpleWhere() {
    var rows =
        runSQL("select 1 col1 from events where case when name like 'abc' then true else false end")
            .toList();
    assertEquals(1, rows.size());

    for (var row : rows) {
      assertEquals(1L, row.asMap().get("col1"));
    }
  }

  private static Stream<Row> runSQL(String sql) {
    var sqlInterpreter = SQLInterpreter.defaultInterpreter();
    var typeSystem = sqlInterpreter.getTypeSystem();

    return TestSQLUtils.runSQL(
        Catalog.fromMap(
            of(
                "events",
                Table.ofListOfMaps(
                    typeSystem,
                    "events",
                    List.of(
                        of("name", "abc", "id", 1),
                        of("name", "edf", "id", 1),
                        of("name", "ghi", "id", 1))),
                "ids",
                Table.ofListOfMaps(typeSystem, "ids", List.of(of("id", 1), of("id", 2))))),
        sql);
  }

  /** https://fleak.atlassian.net/browse/FLEAK-1739 */
  @Test
  public void testCaseWhenStringTypeFieldsBug() {
    var sqlInterpreter = SQLInterpreter.defaultInterpreter();
    var typeSystem = sqlInterpreter.getTypeSystem();

    Map<String, Object> data =
        ofEntries(
            entry("timestamp", "2024-08-14 02:24:25.000000 UTC"),
            entry("device_id", "device_12"),
            entry("transaction_id", "txn_10001"),
            entry("order_id", "order_20001"),
            entry("order_amount", "2583.55"),
            entry("order_firstname_hash", "hash_6314"),
            entry("account_hash", "acct_2"),
            entry("creditcard_hash", "cc_6"),
            entry("creditcard_name_hash", "ccname_7365"),
            entry("ip", "50.163.203.236"),
            entry("asn", "AS7922"),
            entry("device_age", "8247"),
            entry("zipcode", "39799"),
            entry("seller_ip", "50.163.203.236"));

    var sql =
        """
                SELECT
                  device_id,
                  ip,
                  transaction_id,
                  order_amount,
                  device_age,
                  case when seller_ip != ip then seller_ip else 'test' end as real_ip
                FROM events;
                """;
    var res =
        TestSQLUtils.runSQL(
            Catalog.fromMap(of("events", Table.ofListOfMaps(typeSystem, "events", List.of(data)))),
            sql);

    var realIp = res.map(r -> r.asMap().get("real_ip")).findFirst();
    assertTrue(realIp.isPresent());
    assertEquals("test", realIp.get());

    var sql2 =
        """
                SELECT
                  device_id,
                  ip,
                  transaction_id,
                  order_amount,
                  device_age,
                  CASE
                    WHEN seller_ip IS NOT NULL AND seller_ip != ip THEN seller_ip
                    ELSE ip
                  END as real_ip
                FROM events
                """;
    var res2 =
        TestSQLUtils.runSQL(
            Catalog.fromMap(of("events", Table.ofListOfMaps(typeSystem, "events", List.of(data)))),
            sql2);
    res2.map(row -> JsonUtils.toJsonString(row.asMap())).forEach(System.out::println);
  }
}
