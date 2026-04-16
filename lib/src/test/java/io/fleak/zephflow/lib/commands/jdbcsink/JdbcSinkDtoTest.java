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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.commands.JsonConfigParser;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JdbcSinkDtoTest {

  @Test
  void testConfigParsing() {
    JsonConfigParser<JdbcSinkDto.Config> parser = new JsonConfigParser<>(JdbcSinkDto.Config.class);

    Map<String, Object> raw = new HashMap<>();
    raw.put("jdbcUrl", "jdbc:postgresql://localhost:5432/mydb");
    raw.put("credentialId", "db_creds");
    raw.put("tableName", "events");
    raw.put("schemaName", "public");
    raw.put("writeMode", "UPSERT");
    raw.put("upsertKeyColumns", "id,name");
    raw.put("batchSize", 500);
    raw.put("driverClassName", "org.postgresql.Driver");

    JdbcSinkDto.Config config = parser.parseConfig(raw);

    assertEquals("jdbc:postgresql://localhost:5432/mydb", config.getJdbcUrl());
    assertEquals("db_creds", config.getCredentialId());
    assertEquals("events", config.getTableName());
    assertEquals("public", config.getSchemaName());
    assertEquals(JdbcSinkDto.WriteMode.UPSERT, config.getWriteMode());
    assertEquals("id,name", config.getUpsertKeyColumns());
    assertEquals(500, config.getBatchSize());
    assertEquals("org.postgresql.Driver", config.getDriverClassName());
  }

  @Test
  void testConfigDefaults() {
    JsonConfigParser<JdbcSinkDto.Config> parser = new JsonConfigParser<>(JdbcSinkDto.Config.class);

    Map<String, Object> raw = new HashMap<>();
    raw.put("jdbcUrl", "jdbc:h2:mem:test");
    raw.put("tableName", "test_table");

    JdbcSinkDto.Config config = parser.parseConfig(raw);

    assertEquals(JdbcSinkDto.DEFAULT_BATCH_SIZE, config.getBatchSize());
    assertEquals(JdbcSinkDto.WriteMode.INSERT, config.getWriteMode());
    assertNull(config.getSchemaName());
    assertNull(config.getCredentialId());
    assertNull(config.getUpsertKeyColumns());
    assertNull(config.getDriverClassName());
  }
}
