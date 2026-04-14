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
package io.fleak.zephflow.lib.commands.jdbcsink;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.sql.*;
import java.util.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcSinkFlusherTest {

  private static final String JDBC_URL =
      "jdbc:h2:mem:flushertest;DB_CLOSE_DELAY=-1;DATABASE_TO_UPPER=FALSE";

  @BeforeEach
  void setUp() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS test_sink");
      stmt.execute("CREATE TABLE test_sink (id INT PRIMARY KEY, name VARCHAR(255), amount DOUBLE)");
    }
  }

  @AfterEach
  void tearDown() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS test_sink");
    }
  }

  @Test
  void testInsertFlush() throws Exception {
    JdbcSinkFlusher flusher =
        new JdbcSinkFlusher(
            JDBC_URL, null, null, "test_sink", null, JdbcSinkDto.WriteMode.INSERT, List.of());

    SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
        new SimpleSinkCommand.PreparedInputEvents<>();

    RecordFleakData record1 =
        (RecordFleakData) FleakData.wrap(Map.of("id", 1, "name", "alpha", "amount", 10.5));
    RecordFleakData record2 =
        (RecordFleakData) FleakData.wrap(Map.of("id", 2, "name", "beta", "amount", 20.0));

    events.add(record1, buildRow(1.0, "alpha", 10.5));
    events.add(record2, buildRow(2.0, "beta", 20.0));

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(2, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());

    try (Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM test_sink ORDER BY id")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt("id"));
      assertEquals("alpha", rs.getString("name"));

      assertTrue(rs.next());
      assertEquals(2, rs.getInt("id"));
      assertEquals("beta", rs.getString("name"));

      assertFalse(rs.next());
    }

    flusher.close();
  }

  @Test
  void testUpsertSqlGeneration() {
    JdbcSinkFlusher flusher =
        new JdbcSinkFlusher(
            JDBC_URL, null, null, "test_sink", null, JdbcSinkDto.WriteMode.UPSERT, List.of("id"));

    List<String> columns = List.of("id", "name", "amount");
    String sql = flusher.buildSql(columns);

    assertTrue(sql.contains("ON CONFLICT"));
    assertTrue(sql.contains("\"id\""));
    assertTrue(sql.contains("DO UPDATE SET"));
    assertTrue(sql.contains("EXCLUDED.\"name\""));
    assertTrue(sql.contains("EXCLUDED.\"amount\""));
    assertFalse(sql.contains("EXCLUDED.\"id\""));
  }

  @Test
  void testUpsertSqlGenerationAllKeysOnly() {
    JdbcSinkFlusher flusher =
        new JdbcSinkFlusher(
            JDBC_URL,
            null,
            null,
            "test_sink",
            null,
            JdbcSinkDto.WriteMode.UPSERT,
            List.of("id", "name", "amount"));

    List<String> columns = List.of("id", "name", "amount");
    String sql = flusher.buildSql(columns);

    assertTrue(sql.contains("ON CONFLICT"));
    assertTrue(sql.contains("DO NOTHING"));
  }

  @Test
  void testInsertSqlGeneration() {
    JdbcSinkFlusher flusher =
        new JdbcSinkFlusher(
            JDBC_URL, null, null, "test_sink", null, JdbcSinkDto.WriteMode.INSERT, List.of());

    List<String> columns = List.of("id", "name", "amount");
    String sql = flusher.buildSql(columns);

    assertEquals("INSERT INTO \"test_sink\" (\"id\", \"name\", \"amount\") VALUES (?, ?, ?)", sql);
  }

  @Test
  void testSqlGenerationWithSchema() {
    JdbcSinkFlusher flusher =
        new JdbcSinkFlusher(
            JDBC_URL, null, null, "events", "myschema", JdbcSinkDto.WriteMode.INSERT, List.of());

    List<String> columns = List.of("id", "name");
    String sql = flusher.buildSql(columns);

    assertTrue(sql.startsWith("INSERT INTO \"myschema\".\"events\""));
  }

  @Test
  void testEmptyFlush() throws Exception {
    JdbcSinkFlusher flusher =
        new JdbcSinkFlusher(
            JDBC_URL, null, null, "test_sink", null, JdbcSinkDto.WriteMode.INSERT, List.of());

    SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
        new SimpleSinkCommand.PreparedInputEvents<>();

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(0, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());

    flusher.close();
  }

  @Test
  void testInsertRollbackOnError() throws Exception {
    JdbcSinkFlusher flusher =
        new JdbcSinkFlusher(
            JDBC_URL, null, null, "test_sink", null, JdbcSinkDto.WriteMode.INSERT, List.of());

    SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
        new SimpleSinkCommand.PreparedInputEvents<>();

    RecordFleakData record1 =
        (RecordFleakData) FleakData.wrap(Map.of("id", 1, "name", "alpha", "amount", 10.5));
    RecordFleakData record2 =
        (RecordFleakData) FleakData.wrap(Map.of("id", 1, "name", "duplicate", "amount", 20.0));

    events.add(record1, buildRow(1.0, "alpha", 10.5));
    events.add(record2, buildRow(1.0, "duplicate", 20.0));

    assertThrows(Exception.class, () -> flusher.flush(events, Map.of()));

    try (Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM test_sink")) {
      assertTrue(rs.next());
      assertEquals(0, rs.getInt(1));
    }

    flusher.close();
  }

  @Test
  void testFlushWithSchemaName() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE SCHEMA IF NOT EXISTS myschema");
      stmt.execute("DROP TABLE IF EXISTS myschema.test_schema_sink");
      stmt.execute(
          "CREATE TABLE myschema.test_schema_sink (id INT PRIMARY KEY, name VARCHAR(255))");
    }

    JdbcSinkFlusher flusher =
        new JdbcSinkFlusher(
            JDBC_URL,
            null,
            null,
            "test_schema_sink",
            "myschema",
            JdbcSinkDto.WriteMode.INSERT,
            List.of());

    SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
        new SimpleSinkCommand.PreparedInputEvents<>();

    RecordFleakData record = (RecordFleakData) FleakData.wrap(Map.of("id", 1, "name", "test"));
    LinkedHashMap<String, Object> row = new LinkedHashMap<>();
    row.put("id", 1.0);
    row.put("name", "test");
    events.add(record, row);

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());
    assertEquals(1, result.successCount());

    try (Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM myschema.test_schema_sink")) {
      assertTrue(rs.next());
      assertEquals("test", rs.getString("name"));
    }

    try (Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS myschema.test_schema_sink");
      stmt.execute("DROP SCHEMA IF EXISTS myschema");
    }

    flusher.close();
  }

  private static Map<String, Object> buildRow(double id, String name, double amount) {
    LinkedHashMap<String, Object> row = new LinkedHashMap<>();
    row.put("id", id);
    row.put("name", name);
    row.put("amount", amount);
    return row;
  }
}
