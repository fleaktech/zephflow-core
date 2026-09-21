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
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

class JdbcStrictFetchTest {
  @Test
  void strictFetchStreamsRowsStopsAndReleasesStatement() throws Exception {
    String url = "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1";
    try (Connection connection = DriverManager.getConnection(url);
        var statement = connection.createStatement()) {
      statement.execute("CREATE TABLE events(id INT)");
      statement.execute("INSERT INTO events VALUES (1),(2),(3)");
      try (var fetcher =
          new JdbcSourceFetcher(url, null, null, "SELECT * FROM events ORDER BY id", null, 10, 0)) {
        var rows = new ArrayList<Map<String, Object>>();
        var stopped = new AtomicBoolean();
        long count =
            fetcher.fetchStrict(
                row -> {
                  rows.add(row);
                  stopped.set(true);
                },
                stopped::get);
        assertEquals(1, count);
        assertEquals(1, rows.getFirst().get("ID"));
      }
      try (var fetcher =
          new JdbcSourceFetcher(url, null, null, "SELECT * FROM events WHERE 1=0", null, 10, 0)) {
        assertEquals(0, fetcher.fetchStrict(row -> fail("empty query"), () -> false));
        assertTrue(fetcher.isExhausted());
      }
      try (var fetcher =
          new JdbcSourceFetcher(url, null, null, "SELECT * FROM missing", null, 10, 0)) {
        assertThrows(SQLException.class, () -> fetcher.fetchStrict(row -> fail(), () -> false));
        assertFalse(
            fetcher.isExhausted(), "Strict failure must not become an exhausted empty result");
      }
      statement.execute("SHUTDOWN");
    }
  }

  @Test
  void cancelledBeforeStartDoesNotConnect() throws Exception {
    try (var fetcher =
        new JdbcSourceFetcher("jdbc:invalid:unused", null, null, "unused", null, 10, 0)) {
      assertEquals(0, fetcher.fetchStrict(row -> fail(), () -> true));
    }
  }

  @Test
  void laterSqlFailurePreservesDeliveredRowsAndPropagatesOriginalException() throws Exception {
    var connection = mock(Connection.class);
    var statement = mock(PreparedStatement.class);
    var resultSet = mock(ResultSet.class);
    var metadata = mock(ResultSetMetaData.class);
    when(connection.prepareStatement(anyString())).thenReturn(statement);
    when(statement.executeQuery()).thenReturn(resultSet);
    when(resultSet.getMetaData()).thenReturn(metadata);
    when(metadata.getColumnCount()).thenReturn(1);
    when(metadata.getColumnLabel(1)).thenReturn("id");
    when(resultSet.getObject(1)).thenReturn(1);
    SQLException failure = new SQLException("read failed");
    when(resultSet.next()).thenReturn(true).thenThrow(failure);
    try (var driverManager = mockStatic(DriverManager.class)) {
      driverManager
          .when(() -> DriverManager.getConnection("jdbc:strict-test"))
          .thenReturn(connection);
      var fetcher = new JdbcSourceFetcher("jdbc:strict-test", null, null, "SELECT id", null, 10, 0);
      var rows = new ArrayList<Map<String, Object>>();
      assertSame(
          failure,
          assertThrows(SQLException.class, () -> fetcher.fetchStrict(rows::add, () -> false)));
      assertEquals(1, rows.size());
      verify(statement).executeQuery();
      verify(resultSet).close();
      verify(statement).close();
      verify(connection).close();
    }
  }
}
