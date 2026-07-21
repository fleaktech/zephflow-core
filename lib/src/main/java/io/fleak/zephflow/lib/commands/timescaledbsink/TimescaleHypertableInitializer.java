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
package io.fleak.zephflow.lib.commands.timescaledbsink;

import io.fleak.zephflow.lib.commands.jdbcsource.JdbcDriverLoader;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.commons.lang3.StringUtils;

/**
 * Converts an existing table into a TimescaleDB hypertable via {@code create_hypertable(...,
 * if_not_exists => TRUE)}, which is idempotent: a no-op once the table is already a hypertable.
 * Serializable and injectable so the command can substitute a stub in unit tests.
 */
public class TimescaleHypertableInitializer implements Serializable {

  public void ensureHypertable(
      String jdbcUrl,
      String username,
      String password,
      String qualifiedTableName,
      String timeColumn) {
    String sql = buildCreateHypertableSql(qualifiedTableName, timeColumn);
    try (Connection connection = connect(jdbcUrl, username, password);
        Statement statement = connection.createStatement()) {
      statement.execute(sql);
    } catch (SQLException e) {
      throw new RuntimeException("failed to create hypertable for table " + qualifiedTableName, e);
    }
  }

  /**
   * Passes the table and time column as quoted string literals rather than bind parameters, the
   * form TimescaleDB documents; an untyped bind parameter does not resolve to {@code regclass}.
   */
  static String buildCreateHypertableSql(String qualifiedTableName, String timeColumn) {
    return "SELECT create_hypertable('"
        + qualifiedTableName.replace("'", "''")
        + "', '"
        + timeColumn.replace("'", "''")
        + "', if_not_exists => TRUE)";
  }

  private Connection connect(String jdbcUrl, String username, String password) throws SQLException {
    JdbcDriverLoader.loadDriverForUrl(jdbcUrl);
    if (StringUtils.isBlank(username)) {
      return DriverManager.getConnection(jdbcUrl);
    }
    return DriverManager.getConnection(jdbcUrl, username, password);
  }
}
