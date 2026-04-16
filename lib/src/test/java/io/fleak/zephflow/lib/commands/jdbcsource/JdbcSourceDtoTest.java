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

import io.fleak.zephflow.lib.commands.JsonConfigParser;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JdbcSourceDtoTest {

  @Test
  void testConfigParsing() {
    JsonConfigParser<JdbcSourceDto.Config> parser =
        new JsonConfigParser<>(JdbcSourceDto.Config.class);

    Map<String, Object> raw = new HashMap<>();
    raw.put("jdbcUrl", "jdbc:postgresql://localhost:5432/mydb");
    raw.put("credentialId", "db_creds");
    raw.put("query", "SELECT * FROM events WHERE id > :watermark");
    raw.put("watermarkColumn", "id");
    raw.put("pollIntervalMs", 30000L);
    raw.put("fetchSize", 500);
    raw.put("driverClassName", "org.postgresql.Driver");

    JdbcSourceDto.Config config = parser.parseConfig(raw);

    assertEquals("jdbc:postgresql://localhost:5432/mydb", config.getJdbcUrl());
    assertEquals("db_creds", config.getCredentialId());
    assertEquals("SELECT * FROM events WHERE id > :watermark", config.getQuery());
    assertEquals("id", config.getWatermarkColumn());
    assertEquals(30000L, config.getPollIntervalMs());
    assertEquals(500, config.getFetchSize());
    assertEquals("org.postgresql.Driver", config.getDriverClassName());
  }

  @Test
  void testConfigDefaults() {
    JsonConfigParser<JdbcSourceDto.Config> parser =
        new JsonConfigParser<>(JdbcSourceDto.Config.class);

    Map<String, Object> raw = new HashMap<>();
    raw.put("jdbcUrl", "jdbc:h2:mem:test");
    raw.put("query", "SELECT * FROM test");

    JdbcSourceDto.Config config = parser.parseConfig(raw);

    assertEquals(JdbcSourceDto.DEFAULT_POLL_INTERVAL_MS, config.getPollIntervalMs());
    assertEquals(JdbcSourceDto.DEFAULT_FETCH_SIZE, config.getFetchSize());
    assertNull(config.getWatermarkColumn());
    assertNull(config.getCredentialId());
    assertNull(config.getDriverClassName());
  }
}
