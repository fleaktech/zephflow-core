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

  private final String jdbcUrl;
  private final String username;
  private final String password;
  private final String queryTemplate;
  private final String watermarkColumn;
  private final int fetchSize;
  private final boolean streaming;

  private Connection connection;
  private Object lastWatermarkValue;
  private volatile boolean exhausted = false;
  private boolean batchFetchDone = false;

  public JdbcSourceFetcher(
      String jdbcUrl,
      String username,
      String password,
      String queryTemplate,
      String watermarkColumn,
      int fetchSize) {
    this.jdbcUrl = jdbcUrl;
    this.username = username;
    this.password = password;
    this.queryTemplate = queryTemplate;
    this.watermarkColumn = watermarkColumn;
    this.fetchSize = fetchSize;
    this.streaming = watermarkColumn != null && !watermarkColumn.isBlank();
  }

  @Override
  public List<Map<String, Object>> fetch() {
    if (!streaming && batchFetchDone) {
      exhausted = true;
      return List.of();
    }
    try {
      ensureConnection();
      String sql = buildQuery();
      List<Map<String, Object>> results = executeQuery(sql);
      if (!streaming) {
        if (results.isEmpty()) {
          exhausted = true;
        } else {
          batchFetchDone = true;
        }
      }
      return results;
    } catch (SQLException e) {
      log.error("Error fetching from JDBC source", e);
      closeConnection();
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

  private void ensureConnection() throws SQLException {
    if (connection == null || connection.isClosed()) {
      if (username != null) {
        connection = DriverManager.getConnection(jdbcUrl, username, password);
      } else {
        connection = DriverManager.getConnection(jdbcUrl);
      }
    }
  }

  private String buildQuery() {
    if (!streaming || lastWatermarkValue == null) {
      return queryTemplate.replace(":watermark", "NULL");
    }
    String watermarkStr;
    if (lastWatermarkValue instanceof Number) {
      watermarkStr = lastWatermarkValue.toString();
    } else {
      watermarkStr = "'" + lastWatermarkValue.toString().replace("'", "''") + "'";
    }
    return queryTemplate.replace(":watermark", watermarkStr);
  }

  private List<Map<String, Object>> executeQuery(String sql) throws SQLException {
    List<Map<String, Object>> results = new ArrayList<>();
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setFetchSize(fetchSize);
      stmt.setMaxRows(fetchSize);
      try (ResultSet rs = stmt.executeQuery()) {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (rs.next()) {
          Map<String, Object> row = new LinkedHashMap<>();
          for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnLabel(i);
            Object value = rs.getObject(i);
            row.put(columnName, value);
          }
          if (streaming && watermarkColumn != null) {
            Object watermarkValue = row.get(watermarkColumn);
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
