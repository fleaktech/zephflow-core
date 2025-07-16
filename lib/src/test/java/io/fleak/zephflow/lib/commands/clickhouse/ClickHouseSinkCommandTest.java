/**
 * Copyright 2025 Fleak Tech Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fleak.zephflow.lib.commands.clickhouse;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.utils.JsonUtils;
import org.junit.jupiter.api.*;
import org.testcontainers.clickhouse.ClickHouseContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class ClickHouseSinkCommandTest {

  @Container
  static final ClickHouseContainer clickHouseContainer = new ClickHouseContainer("clickhouse/clickhouse-server:23.8");

  static final String DATABASE = "test_db";
  static final String TABLE = "test_table";

  static final List<RecordFleakData> SOURCE_EVENTS = new ArrayList<>();

  @BeforeAll
  static void setupSchema() throws Exception {
    for (int i = 0; i < 10; i++) {
      SOURCE_EVENTS.add((RecordFleakData) FleakData.wrap(Map.of("num", i)));
    }

//    try (Connection conn = DriverManager.getConnection(clickHouseContainer.getJdbcUrl(), "default", "")) {
//      try (Statement stmt = conn.createStatement()) {
//        stmt.execute("CREATE DATABASE IF NOT EXISTS " + DATABASE);
//        stmt.execute(
//                "CREATE TABLE IF NOT EXISTS " + DATABASE + "." + TABLE +
//                        " (num UInt64) ENGINE = MergeTree() ORDER BY num"
//        );
//      }
//    }
  }

  @AfterAll
  static void cleanup() throws Exception {
  }

  @Test
  void testWriteToSink() throws Exception {
    ClickHouseSinkCommand command = (ClickHouseSinkCommand) new ClickHouseSinkCommandFactory()
            .createCommand("test-node", JobContext.builder().build());

    ClickHouseSinkDto.Config config =
            ClickHouseSinkDto.Config.builder()
                    .endpoint(clickHouseContainer.getHost() + ":" + clickHouseContainer.getMappedPort(8123))
                    .database(DATABASE)
                    .table(TABLE)
                    .username("default")
                    .password("") // default user has no password
                    .credentialId(null)
                    .build();

    command.parseAndValidateArg(JsonUtils.toJsonString(config));
    command.writeToSink(SOURCE_EVENTS, "test_user", new MetricClientProvider.NoopMetricClientProvider());

    List<Long> expected = SOURCE_EVENTS.stream()
            .map(e -> ((Number) e.unwrap().get("num")).longValue())
            .sorted()
            .toList();

  }
}
