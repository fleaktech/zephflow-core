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

import io.fleak.zephflow.lib.commands.source.Fetcher;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcSourceFetcher implements Fetcher<Map<String, Object>> {

  static final int DEFAULT_MAX_CONSECUTIVE_FAILURES = 10;

  private final String jdbcUrl;
  private final String username;
  private final String password;
  private final String queryTemplate;
  private final String watermarkColumn;
  private final int fetchSize;
  private final long pollIntervalMs;
  private final int maxConsecutiveFailures;
  private final boolean streaming;

  private Connection connection;
  private Object lastWatermarkValue;
  private volatile boolean exhausted = false;
  private boolean batchFetchDone = false;
  private long lastQueryTime = 0;
  private int consecutiveFailures = 0;

  public JdbcSourceFetcher(
      String jdbcUrl,
      String username,
      String password,
      String queryTemplate,
      String watermarkColumn,
      int fetchSize,
      long pollIntervalMs) {
    this(
        jdbcUrl,
        username,
        password,
        queryTemplate,
        watermarkColumn,
        fetchSize,
        pollIntervalMs,
        DEFAULT_MAX_CONSECUTIVE_FAILURES);
  }

  JdbcSourceFetcher(
      String jdbcUrl,
      String username,
      String password,
      String queryTemplate,
      String watermarkColumn,
      int fetchSize,
      long pollIntervalMs,
      int maxConsecutiveFailures) {
    this.jdbcUrl = jdbcUrl;
    this.username = username;
    this.password = password;
    this.queryTemplate = queryTemplate;
    this.watermarkColumn = watermarkColumn;
    this.fetchSize = fetchSize;
    this.pollIntervalMs = pollIntervalMs;
    this.maxConsecutiveFailures = maxConsecutiveFailures;
    this.streaming = watermarkColumn != null && !watermarkColumn.isBlank();
  }

  @Override
  public List<Map<String, Object>> fetch() {
    if (!streaming && batchFetchDone) {
      exhausted = true;
      return List.of();
    }
    if (streaming && lastQueryTime > 0) {
      long elapsed = System.currentTimeMillis() - lastQueryTime;
      long toWait = pollIntervalMs - elapsed;
      if (toWait > 0) {
        try {
          Thread.sleep(toWait);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return List.of();
        }
      }
    }
    lastQueryTime = System.currentTimeMillis();
    try {
      ensureConnection();
      PreparedQuery query = buildQuery();
      List<Map<String, Object>> results = executeQuery(query);
      consecutiveFailures = 0;
      if (!streaming) {
        if (results.isEmpty()) {
          exhausted = true;
        } else {
          batchFetchDone = true;
        }
      }
      return results;
    } catch (SQLException e) {
      consecutiveFailures++;
      log.error(
          "Error fetching from JDBC source (consecutive failures: {}/{})",
          consecutiveFailures,
          maxConsecutiveFailures,
          e);
      closeConnection();
      if (consecutiveFailures >= maxConsecutiveFailures) {
        log.error(
            "JDBC source has failed {} times consecutively; marking as exhausted",
            consecutiveFailures);
        exhausted = true;
      }
      return List.of();
    }
  }

  @Override
  public boolean isExhausted() {
    return exhausted;
  }

  @Override
  public void close() throws IOException {
    closeConnection();
  }

  private static final int CONNECTION_VALIDATION_TIMEOUT_SECONDS = 5;

  private void ensureConnection() throws SQLException {
    if (connection != null && !isConnectionLive(connection)) {
      closeConnection();
    }
    if (connection == null) {
      JdbcDriverLoader.loadDriverForUrl(jdbcUrl);
      if (username != null) {
        connection = DriverManager.getConnection(jdbcUrl, username, password);
      } else {
        connection = DriverManager.getConnection(jdbcUrl);
      }
    }
  }

  private static boolean isConnectionLive(Connection conn) {
    try {
      return !conn.isClosed() && conn.isValid(CONNECTION_VALIDATION_TIMEOUT_SECONDS);
    } catch (SQLException e) {
      log.debug("Connection liveness check threw — treating as dead", e);
      return false;
    }
  }

  private record PreparedQuery(String sql, List<Object> params) {}

  private PreparedQuery buildQuery() {
    if (!streaming) {
      return new PreparedQuery(queryTemplate.replace(":watermark", "NULL"), List.of());
    }
    if (lastWatermarkValue == null) {
      String sql =
          queryTemplate
              .replaceAll("\\S+\\s*(?:>=|<=|!=|>|<|=)\\s*:watermark", "1=1")
              .replace(":watermark", "NULL");
      return new PreparedQuery(sql, List.of());
    }
    int placeholderCount = countOccurrences(queryTemplate, ":watermark");
    String sql = queryTemplate.replace(":watermark", "?");
    List<Object> params = new ArrayList<>(placeholderCount);
    for (int i = 0; i < placeholderCount; i++) {
      params.add(lastWatermarkValue);
    }
    return new PreparedQuery(sql, params);
  }

  private static int countOccurrences(String haystack, String needle) {
    int count = 0;
    int idx = 0;
    while ((idx = haystack.indexOf(needle, idx)) != -1) {
      count++;
      idx += needle.length();
    }
    return count;
  }

  private List<Map<String, Object>> executeQuery(PreparedQuery query) throws SQLException {
    List<Map<String, Object>> results = new ArrayList<>();
    try (PreparedStatement stmt = connection.prepareStatement(query.sql())) {
      stmt.setFetchSize(fetchSize);
      if (!streaming) {
        stmt.setMaxRows(fetchSize);
      }
      for (int i = 0; i < query.params().size(); i++) {
        stmt.setObject(i + 1, query.params().get(i));
      }
      try (ResultSet rs = stmt.executeQuery()) {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        String resolvedWatermarkColumn = resolveWatermarkColumn(metaData, columnCount);
        while (rs.next()) {
          Map<String, Object> row = new LinkedHashMap<>();
          for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnLabel(i);
            Object value = rs.getObject(i);
            row.put(columnName, value);
          }
          if (resolvedWatermarkColumn != null) {
            Object watermarkValue = row.get(resolvedWatermarkColumn);
            if (watermarkValue != null) {
              lastWatermarkValue = watermarkValue;
            }
          }
          results.add(row);
        }
      }
    }
    log.debug("Fetched {} rows from JDBC source", results.size());
    return results;
  }

  private String resolveWatermarkColumn(ResultSetMetaData metaData, int columnCount)
      throws SQLException {
    if (!streaming || watermarkColumn == null) {
      return null;
    }
    for (int i = 1; i <= columnCount; i++) {
      String label = metaData.getColumnLabel(i);
      if (label != null && label.equalsIgnoreCase(watermarkColumn)) {
        return label;
      }
    }
    throw new IllegalStateException(
        "Watermark column '"
            + watermarkColumn
            + "' is not present in the query result set. The configured watermarkColumn must"
            + " appear in the SELECT list (case-insensitive).");
  }

  private void closeConnection() {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        log.warn("Failed to close JDBC connection", e);
      }
      connection = null;
    }
  }
}
