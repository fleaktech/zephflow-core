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
        new JdbcSourceFetcher(JDBC_URL, null, null, "SELECT * FROM test_events", null, 1000);

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
        new JdbcSourceFetcher(JDBC_URL, null, null, "SELECT * FROM test_events", null, 1000);

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
            "SELECT * FROM test_events WHERE :watermark IS NULL OR id > :watermark ORDER BY id",
            "ID",
            1000);

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
            "SELECT * FROM test_events WHERE :watermark IS NULL OR id > :watermark ORDER BY id",
            "ID",
            1000);

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
  void testFetchWithFetchSizeLimit() throws Exception {
    JdbcSourceFetcher fetcher =
        new JdbcSourceFetcher(
            JDBC_URL, null, null, "SELECT * FROM test_events ORDER BY id", null, 2);

    List<Map<String, Object>> results = fetcher.fetch();
    assertEquals(2, results.size());

    fetcher.close();
  }
}
