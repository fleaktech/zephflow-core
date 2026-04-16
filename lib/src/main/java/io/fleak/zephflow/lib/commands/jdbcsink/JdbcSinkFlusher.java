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

import io.fleak.zephflow.lib.commands.jdbcsource.JdbcDriverLoader;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcSinkFlusher implements SimpleSinkCommand.Flusher<Map<String, Object>> {

  private final String jdbcUrl;
  private final String username;
  private final String password;
  private final String tableName;
  private final String schemaName;
  private final JdbcSinkDto.WriteMode writeMode;
  private final List<String> upsertKeyColumns;

  private Connection connection;

  public JdbcSinkFlusher(
      String jdbcUrl,
      String username,
      String password,
      String tableName,
      String schemaName,
      JdbcSinkDto.WriteMode writeMode,
      List<String> upsertKeyColumns) {
    this.jdbcUrl = jdbcUrl;
    this.username = username;
    this.password = password;
    this.tableName = tableName;
    this.schemaName = schemaName;
    this.writeMode = writeMode;
    this.upsertKeyColumns = upsertKeyColumns;
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> preparedInputEvents,
      Map<String, String> metricTags)
      throws Exception {
    List<Map<String, Object>> data = preparedInputEvents.preparedList();
    if (data.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    ensureConnection();
    boolean originalAutoCommit = connection.getAutoCommit();
    connection.setAutoCommit(false);
    try {
      List<String> columns = new ArrayList<>(data.getFirst().keySet());
      String sql = buildSql(columns);

      try (PreparedStatement stmt = connection.prepareStatement(sql)) {
        for (Map<String, Object> row : data) {
          for (int i = 0; i < columns.size(); i++) {
            Object value = row.get(columns.get(i));
            stmt.setObject(i + 1, value);
          }
          stmt.addBatch();
        }
        stmt.executeBatch();
      }

      connection.commit();
      log.debug("Flushed {} rows to JDBC sink", data.size());
      return new SimpleSinkCommand.FlushResult(data.size(), 0, List.of());
    } catch (Exception e) {
      try {
        connection.rollback();
      } catch (SQLException rollbackEx) {
        log.warn("Failed to rollback transaction", rollbackEx);
      }
      throw e;
    } finally {
      try {
        connection.setAutoCommit(originalAutoCommit);
      } catch (SQLException e) {
        log.warn("Failed to restore auto-commit", e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        throw new IOException("Failed to close JDBC connection", e);
      }
      connection = null;
    }
  }

  private void ensureConnection() throws SQLException {
    if (connection == null || connection.isClosed()) {
      JdbcDriverLoader.loadDriverForUrl(jdbcUrl);
      if (username != null) {
        connection = DriverManager.getConnection(jdbcUrl, username, password);
      } else {
        connection = DriverManager.getConnection(jdbcUrl);
      }
    }
  }

  String buildSql(List<String> columns) {
    String qualifiedTable = buildQualifiedTableName();
    String columnList =
        columns.stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));
    String placeholders = columns.stream().map(c -> "?").collect(Collectors.joining(", "));

    if (writeMode == JdbcSinkDto.WriteMode.UPSERT) {
      String conflictColumns =
          upsertKeyColumns.stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));
      List<String> nonKeyColumns =
          columns.stream().filter(c -> !upsertKeyColumns.contains(c)).toList();
      String updateSet =
          nonKeyColumns.stream()
              .map(c -> quoteIdentifier(c) + " = EXCLUDED." + quoteIdentifier(c))
              .collect(Collectors.joining(", "));

      if (updateSet.isEmpty()) {
        return String.format(
            "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO NOTHING",
            qualifiedTable, columnList, placeholders, conflictColumns);
      }
      return String.format(
          "INSERT INTO %s (%s) VALUES (%s) ON CONFLICT (%s) DO UPDATE SET %s",
          qualifiedTable, columnList, placeholders, conflictColumns, updateSet);
    }

    return String.format(
        "INSERT INTO %s (%s) VALUES (%s)", qualifiedTable, columnList, placeholders);
  }

  private String buildQualifiedTableName() {
    if (schemaName != null && !schemaName.isBlank()) {
      return quoteIdentifier(schemaName) + "." + quoteIdentifier(tableName);
    }
    return quoteIdentifier(tableName);
  }

  private String quoteIdentifier(String identifier) {
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }
}
