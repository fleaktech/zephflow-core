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
package io.fleak.zephflow.lib.commands.jdbcsource;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JdbcSourceFetcherTest {

  private static final String JDBC_URL = "jdbc:h2:mem:fetchertest;DB_CLOSE_DELAY=-1";

  @BeforeEach
  void setUp() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS test_events");
      stmt.execute(
          "CREATE TABLE test_events (id INT PRIMARY KEY, name VARCHAR(255), amount DOUBLE)");
      stmt.execute("INSERT INTO test_events (id, name, amount) VALUES (1, 'alpha', 10.5)");
      stmt.execute("INSERT INTO test_events (id, name, amount) VALUES (2, 'beta', 20.0)");
      stmt.execute("INSERT INTO test_events (id, name, amount) VALUES (3, 'gamma', 30.7)");
    }
  }

  @AfterEach
  void tearDown() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS test_events");
    }
  }

  @Test
  void testFetchBatchMode() throws Exception {
    JdbcSourceFetcher fetcher =
        new JdbcSourceFetcher(JDBC_URL, null, null, "SELECT * FROM test_events", null, 1000, 0);

    assertFalse(fetcher.isExhausted());

    List<Map<String, Object>> results = fetcher.fetch();
    assertEquals(3, results.size());

    Map<String, Object> first = results.get(0);
    assertEquals(1, first.get("ID"));
    assertEquals("alpha", first.get("NAME"));
    assertEquals(10.5, first.get("AMOUNT"));

    assertFalse(fetcher.isExhausted());

    List<Map<String, Object>> secondFetch = fetcher.fetch();
    assertTrue(secondFetch.isEmpty());
    assertTrue(fetcher.isExhausted());
    fetcher.close();
  }

  @Test
  void testFetchBatchModeExhaustedOnEmpty() throws Exception {
    try (Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM test_events");
    }

    JdbcSourceFetcher fetcher =
        new JdbcSourceFetcher(JDBC_URL, null, null, "SELECT * FROM test_events", null, 1000, 0);

    List<Map<String, Object>> results = fetcher.fetch();
    assertTrue(results.isEmpty());
    assertTrue(fetcher.isExhausted());
    fetcher.close();
  }

  @Test
  void testFetchWithWatermark() throws Exception {
    JdbcSourceFetcher fetcher =
        new JdbcSourceFetcher(
            JDBC_URL,
            null,
            null,
            "SELECT * FROM test_events WHERE id > :watermark ORDER BY id",
            "ID",
            1000,
            0);

    assertFalse(fetcher.isExhausted());

    List<Map<String, Object>> results = fetcher.fetch();
    assertEquals(3, results.size());

    assertFalse(fetcher.isExhausted());

    List<Map<String, Object>> results2 = fetcher.fetch();
    assertTrue(results2.isEmpty());

    assertFalse(fetcher.isExhausted());

    fetcher.close();
  }

  @Test
  void testFetchWithWatermarkIncremental() throws Exception {
    JdbcSourceFetcher fetcher =
        new JdbcSourceFetcher(
            JDBC_URL,
            null,
            null,
            "SELECT * FROM test_events WHERE id > :watermark ORDER BY id",
            "ID",
            1000,
            0);

    List<Map<String, Object>> results1 = fetcher.fetch();
    assertEquals(3, results1.size());

    try (Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement()) {
      stmt.execute("INSERT INTO test_events (id, name, amount) VALUES (4, 'delta', 40.0)");
      stmt.execute("INSERT INTO test_events (id, name, amount) VALUES (5, 'epsilon', 50.0)");
    }

    List<Map<String, Object>> results2 = fetcher.fetch();
    assertEquals(2, results2.size());
    assertEquals(4, results2.get(0).get("ID"));
    assertEquals(5, results2.get(1).get("ID"));

    fetcher.close();
  }

  @Test
  void testStreamingWatermarkColumnMatchesCaseInsensitively() throws Exception {
    // User configures watermarkColumn lowercase 'id' but H2 returns 'ID' uppercase from
    // the result-set metadata. The watermark must still advance after the first fetch.
    JdbcSourceFetcher fetcher =
        new JdbcSourceFetcher(
            JDBC_URL,
            null,
            null,
            "SELECT * FROM test_events WHERE id > :watermark ORDER BY id",
            "id",
            1000,
            0);

    List<Map<String, Object>> first = fetcher.fetch();
    assertEquals(3, first.size());

    try (Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement()) {
      stmt.execute("INSERT INTO test_events (id, name, amount) VALUES (4, 'delta', 40.0)");
    }

    List<Map<String, Object>> second = fetcher.fetch();
    assertEquals(1, second.size(), "watermark should have advanced past id=3");
    assertEquals(4, second.get(0).get("ID"));
    fetcher.close();
  }

  @Test
  void testStreamingFailsFastWhenWatermarkColumnNotInSelect() throws Exception {
    // watermarkColumn 'amount' exists in the table but is not in the SELECT list,
    // so each fetched row has no amount value. The fetcher must fail loudly rather
    // than silently leaving the watermark unset and re-reading the same rows forever.
    JdbcSourceFetcher fetcher =
        new JdbcSourceFetcher(
            JDBC_URL,
            null,
            null,
            "SELECT id, name FROM test_events WHERE id > :watermark ORDER BY id",
            "amount",
            1000,
            0);

    IllegalStateException ex = assertThrows(IllegalStateException.class, fetcher::fetch);
    assertTrue(
        ex.getMessage().toLowerCase().contains("amount"),
        "Expected error message to mention the missing column, got: " + ex.getMessage());
    fetcher.close();
  }

  @Test
  void testStreamingExhaustsAfterMaxConsecutiveFailures() throws Exception {
    // A permanently broken query (table does not exist) used to spin forever logging
    // SQLException on every poll. After the failure budget is exhausted the source must
    // mark itself exhausted so the runner can shut it down.
    JdbcSourceFetcher fetcher =
        new JdbcSourceFetcher(
            JDBC_URL,
            null,
            null,
            "SELECT * FROM nonexistent_table WHERE id > :watermark",
            "id",
            1000,
            0,
            3);

    for (int i = 0; i < 3; i++) {
      assertFalse(fetcher.isExhausted(), "should not be exhausted before failure budget");
      assertTrue(fetcher.fetch().isEmpty());
    }
    assertTrue(
        fetcher.isExhausted(),
        "source must mark itself exhausted after maxConsecutiveFailures errors");
    fetcher.close();
  }

  @Test
  void testStreamingFailureCounterResetsOnSuccess() throws Exception {
    // Drop and recreate to force a transient failure window.
    try (Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS flaky_events");
    }

    JdbcSourceFetcher fetcher =
        new JdbcSourceFetcher(
            JDBC_URL,
            null,
            null,
            "SELECT * FROM flaky_events WHERE id > :watermark ORDER BY id",
            "id",
            1000,
            0,
            3);

    // 2 failures (table missing), still under budget.
    assertTrue(fetcher.fetch().isEmpty());
    assertTrue(fetcher.fetch().isEmpty());
    assertFalse(fetcher.isExhausted());

    // Recover. Successful fetch must reset the failure counter so a later transient failure
    // doesn't immediately exhaust the source.
    try (Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE flaky_events (id INT PRIMARY KEY)");
      stmt.execute("INSERT INTO flaky_events (id) VALUES (1)");
    }
    assertEquals(1, fetcher.fetch().size());
    assertFalse(fetcher.isExhausted());

    // Drop again. Need 3 more failures to exhaust — proves counter was reset.
    try (Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE flaky_events");
    }
    assertTrue(fetcher.fetch().isEmpty());
    assertTrue(fetcher.fetch().isEmpty());
    assertFalse(fetcher.isExhausted(), "counter should have reset on success");
    assertTrue(fetcher.fetch().isEmpty());
    assertTrue(fetcher.isExhausted());

    fetcher.close();
  }

  @Test
  void testStreamingWithTimestampWatermark() throws Exception {
    // Timestamp watermark exercises the PreparedStatement.setObject binding path
    // for non-Number types — the previous `Timestamp.toString()` interpolation
    // happened to work in H2 but was driver-specific and fragile.
    try (Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS ts_events");
      stmt.execute("CREATE TABLE ts_events (id INT PRIMARY KEY, updated_at TIMESTAMP)");
      stmt.execute(
          "INSERT INTO ts_events (id, updated_at) VALUES (1, TIMESTAMP '2024-01-01 10:00:00')");
      stmt.execute(
          "INSERT INTO ts_events (id, updated_at) VALUES (2, TIMESTAMP '2024-01-01 11:00:00')");
    }

    JdbcSourceFetcher fetcher =
        new JdbcSourceFetcher(
            JDBC_URL,
            null,
            null,
            "SELECT * FROM ts_events WHERE updated_at > :watermark ORDER BY updated_at",
            "updated_at",
            1000,
            0);

    List<Map<String, Object>> first = fetcher.fetch();
    assertEquals(2, first.size());

    try (Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement()) {
      stmt.execute(
          "INSERT INTO ts_events (id, updated_at) VALUES (3, TIMESTAMP '2024-01-01 12:00:00')");
    }

    List<Map<String, Object>> second = fetcher.fetch();
    assertEquals(1, second.size(), "watermark should advance past the latest timestamp");
    assertEquals(3, second.get(0).get("ID"));

    fetcher.close();
    try (Connection conn = DriverManager.getConnection(JDBC_URL);
        Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS ts_events");
    }
  }

  @Test
  void testStreamingReturnsAllRowsAboveWatermarkRegardlessOfFetchSize() throws Exception {
    // In streaming mode, fetchSize is a JDBC cursor-prefetch hint, NOT a row limit.
    // Capping with setMaxRows risks silent data loss: rows past the cap are skipped
    // when the watermark advances to whatever ordering happened to land first.
    JdbcSourceFetcher fetcher =
        new JdbcSourceFetcher(
            JDBC_URL,
            null,
            null,
            "SELECT * FROM test_events WHERE id > :watermark ORDER BY id",
            "id",
            2,
            0);

    List<Map<String, Object>> results = fetcher.fetch();
    assertEquals(
        3,
        results.size(),
        "streaming must return all rows above watermark, not be capped by fetchSize");
    fetcher.close();
  }

  @Test
  void testFetchWithFetchSizeLimit() throws Exception {
    JdbcSourceFetcher fetcher =
        new JdbcSourceFetcher(
            JDBC_URL, null, null, "SELECT * FROM test_events ORDER BY id", null, 2, 0);

    List<Map<String, Object>> results = fetcher.fetch();
    assertEquals(2, results.size());

    fetcher.close();
  }
}
