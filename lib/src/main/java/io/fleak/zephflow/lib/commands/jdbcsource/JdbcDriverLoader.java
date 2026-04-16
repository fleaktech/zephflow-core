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

import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcDriverLoader {

  private static final Map<String, String> URL_PREFIX_TO_DRIVER =
      Map.of(
          "jdbc:postgresql:", "org.postgresql.Driver",
          "jdbc:mysql:", "com.mysql.cj.jdbc.Driver",
          "jdbc:mariadb:", "org.mariadb.jdbc.Driver",
          "jdbc:h2:", "org.h2.Driver",
          "jdbc:sqlserver:", "com.microsoft.sqlserver.jdbc.SQLServerDriver",
          "jdbc:oracle:", "oracle.jdbc.OracleDriver");

  public static void loadDriverForUrl(String jdbcUrl) {
    if (jdbcUrl == null) return;
    URL_PREFIX_TO_DRIVER.entrySet().stream()
        .filter(e -> jdbcUrl.startsWith(e.getKey()))
        .map(Map.Entry::getValue)
        .findFirst()
        .ifPresent(JdbcDriverLoader::loadDriver);
  }

  private static void loadDriver(String driverClassName) {
    try {
      Class.forName(driverClassName);
    } catch (ClassNotFoundException e) {
      log.warn("JDBC driver class not found: {}", driverClassName);
    }
  }
}
