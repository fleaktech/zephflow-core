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
package io.fleak.zephflow.lib.sql.exec;

import io.fleak.zephflow.lib.sql.SQLInterpreter;
import io.fleak.zephflow.lib.sql.TestSQLUtils;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GrokTests {

  @Test
  public void testExtractIpAddress() {
    var rows =
        runSQL(
                "SELECT grok('%{IPV4:client_ip}', '192.168.1.1 - - [26/Mar/2023:10:00:00 +0000]') AS col1")
            .toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals(Map.of("client_ip", "192.168.1.1"), rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testExtractEmail() {
    var rows =
        runSQL(
                "SELECT grok('%{EMAILADDRESS:email}', 'Contact us at support@example.com for assistance')->'email' AS col1")
            .toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("support@example.com", rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testExtractDate() {
    var rows =
        runSQL(
                "SELECT grok('%{DATESTAMP:timestamp}', '2024-08-29 14:25:30,000 - INFO - User logged in')->'timestamp' AS col1")
            .toList();
    Assertions.assertEquals(1, rows.size());
    Assertions.assertEquals("24-08-29 14:25:30,000", rows.get(0).asMap().get("col1"));
  }

  @Test
  public void testComplexLogEntry() {
    var rows =
        runSQL(
                "SELECT grok('%{IPV4:client_ip} - %{WORD:method} %{URIPATHPARAM:request} HTTP/%{NUMBER:http_version}', '127.0.0.1 - GET /index.html HTTP/1.1') AS col1")
            .toList();
    Assertions.assertEquals(1, rows.size());
    //noinspection unchecked
    var resultMap = (Map<String, Object>) rows.get(0).asMap().get("col1");
    Assertions.assertEquals("127.0.0.1", resultMap.get("client_ip"));
    Assertions.assertEquals("GET", resultMap.get("method"));
    Assertions.assertEquals("/index.html", resultMap.get("request"));
    Assertions.assertEquals("1.1", resultMap.get("http_version"));
  }

  private static Stream<Row> runSQL(String sql) {
    var sqlInterpreter = SQLInterpreter.defaultInterpreter();
    var typeSystem = sqlInterpreter.getTypeSystem();

    return TestSQLUtils.runSQL(
        Catalog.fromMap(
            Map.of(
                "records",
                Table.ofListOfMaps(
                    typeSystem,
                    "records",
                    List.of(
                        Map.of("log_entry", "127.0.0.1 - GET /index.html HTTP/1.1"),
                        Map.of("log_entry", "192.168.1.1 - - [26/Mar/2023:10:00:00 +0000]"),
                        Map.of("log_entry", "Contact us at support@example.com for assistance"),
                        Map.of("log_entry", "2024-08-29 14:25:30,000 - INFO - User logged in"))))),
        sql);
  }
}
