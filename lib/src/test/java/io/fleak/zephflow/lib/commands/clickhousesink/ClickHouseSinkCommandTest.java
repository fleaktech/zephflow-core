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
package io.fleak.zephflow.lib.commands.clickhousesink;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;
import lombok.SneakyThrows;
import org.junit.jupiter.api.*;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class ClickHouseSinkCommandTest {

  @Container
  static final ClickHouseContainer clickHouseContainer =
      new ClickHouseContainer("clickhouse/clickhouse-server:23.8");

  static final String DATABASE = "test_db";
  static final String TABLE = "test_table";

  static final List<RecordFleakData> SOURCE_EVENTS = new ArrayList<>();
  static final JobContext JOB_CONTEXT = new JobContext();

  @BeforeAll
  static void setupSchema() {
    executeSQL(
        List.of(
            "CREATE DATABASE IF NOT EXISTS " + DATABASE,
            "CREATE TABLE IF NOT EXISTS "
                + DATABASE
                + "."
                + TABLE
                + " (num UInt64) ENGINE = MergeTree() ORDER BY num"));

    for (int i = 0; i < 10; i++) {
      SOURCE_EVENTS.add((RecordFleakData) FleakData.wrap(Map.of("num", i)));
    }

    JOB_CONTEXT.setMetricTags(TestUtils.JOB_CONTEXT.getMetricTags());
    JOB_CONTEXT.setOtherProperties(
        Map.of(
            "clickhouse_credentials",
            new UsernamePasswordCredential(
                clickHouseContainer.getUsername(), clickHouseContainer.getPassword())));
  }

  @AfterAll
  static void cleanup() {
    executeSQL(List.of("DROP TABLE IF EXISTS " + DATABASE + "." + TABLE));
  }

  @SneakyThrows
  private static void executeSQL(List<String> sqlStatements) {
    try (var conn = getConnection()) {
      try (var stmt = conn.createStatement()) {
        sqlStatements.forEach(
            sql -> {
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
    try (var conn = getConnection()) {
      try (var stmt = conn.createStatement()) {
        try (var rs = stmt.executeQuery(sql)) {
          return fn.apply(rs);
        }
      }
    }
  }

  private static Connection getConnection() throws SQLException {
    return DriverManager.getConnection(
        clickHouseContainer.getJdbcUrl(),
        clickHouseContainer.getUsername(),
        clickHouseContainer.getPassword());
  }

  @Test
  void testWriteToSink() {
    ClickHouseSinkCommand command =
        (ClickHouseSinkCommand)
            new ClickHouseSinkCommandFactory().createCommand("test-node", JOB_CONTEXT);

    ClickHouseSinkDto.Config config =
        ClickHouseSinkDto.Config.builder()
            .endpoint(
                "http://"
                    + clickHouseContainer.getHost()
                    + ":"
                    + clickHouseContainer.getMappedPort(8123))
            .database(DATABASE)
            .table(TABLE)
            .credentialId("clickhouse_credentials")
            .build();

    command.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
      var context = command.getExecutionContext();
    command.writeToSink(SOURCE_EVENTS, "test_user", context);

    var rows =
        selectSQL(
            "select * from " + DATABASE + "." + TABLE,
            r -> {
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
