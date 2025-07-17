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
package io.fleak.zephflow.lib.commands.clickhouse;

import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.utils.JsonUtils;
import lombok.SneakyThrows;
import org.junit.jupiter.api.*;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;


@Testcontainers
class ClickHouseSinkCommandTest {

  @Container
  static final ClickHouseContainer clickHouseContainer = new ClickHouseContainer("clickhouse/clickhouse-server:23.8");

  static final String DATABASE = "test_db";
  static final String TABLE = "test_table";

  static final List<RecordFleakData> SOURCE_EVENTS = new ArrayList<>();

  @BeforeAll
  static void setupSchema() {
    executeSQL(List.of(
            "CREATE DATABASE IF NOT EXISTS " + DATABASE,
            "CREATE TABLE IF NOT EXISTS " + DATABASE + "." + TABLE +
                    " (num UInt64) ENGINE = MergeTree() ORDER BY num"
    ));

    for (int i = 0; i < 10; i++) {
      SOURCE_EVENTS.add((RecordFleakData) FleakData.wrap(Map.of("num", i)));
    }
  }

  @AfterAll
  static void cleanup() {
    executeSQL(List.of(
            "DROP TABLE IF EXISTS " + DATABASE + "." + TABLE
    ));
  }

  @SneakyThrows
  private static void executeSQL(List<String> sqlStatements){
    try(var conn = getConnection()) {
      try(var stmt = conn.createStatement()) {
        sqlStatements.forEach( sql -> {
            try {
                stmt.execute(sql);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
      }
    }
  }

  @SneakyThrows
  private static <T> T selectSQL(String sql, Function<ResultSet, T> fn) {
    try(var conn = getConnection()) {
      try(var stmt = conn.createStatement()) {
        try(var rs = stmt.executeQuery(sql)) {
          return fn.apply(rs);
        }
      }
    }
  }

  private static Connection getConnection() throws SQLException {
    return DriverManager.getConnection(
            clickHouseContainer.getJdbcUrl(),
            clickHouseContainer.getUsername(), clickHouseContainer.getPassword());
  }

  @Test
  void testWriteToSink() {
    ClickHouseSinkCommand command = (ClickHouseSinkCommand) new ClickHouseSinkCommandFactory()
            .createCommand("test-node", TestUtils.JOB_CONTEXT);

    ClickHouseSinkDto.Config config =
            ClickHouseSinkDto.Config.builder()
                    .endpoint("http://" + clickHouseContainer.getHost() + ":" + clickHouseContainer.getMappedPort(8123))
                    .database(DATABASE)
                    .table(TABLE)
                    .username(clickHouseContainer.getUsername())
                    .password(clickHouseContainer.getPassword())
                    .credentialId(null)
                    .build();

    command.parseAndValidateArg(JsonUtils.toJsonString(config));
    command.writeToSink(SOURCE_EVENTS, "test_user", new MetricClientProvider.NoopMetricClientProvider());

    var rows =  selectSQL("select * from " + DATABASE + "." + TABLE, r -> {
      var i = 0;
      try {
        while (r.next()) {
          i++;
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
      return i;
    });
    assertEquals(SOURCE_EVENTS.size(), rows);

  }
}
