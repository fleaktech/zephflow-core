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

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.lib.commands.jdbcsink.JdbcSinkDto;
import java.util.concurrent.TimeUnit;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Configuration for the TimescaleDB sink connector. TimescaleDB is a PostgreSQL extension, so this
 * reuses the JDBC sink's batched write path and adds the time-series specifics: converting the
 * target table into a hypertable and coercing the time column to a timestamp.
 */
public interface TimescaleDbSinkDto {

  int DEFAULT_BATCH_SIZE = 1000;

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {

    /** PostgreSQL JDBC URL of the TimescaleDB instance, e.g. {@code jdbc:postgresql://host/db}. */
    private String jdbcUrl;

    /** Credential id resolving to a username/password for the connection. */
    private String credentialId;

    /** Target table. It must already exist (the user owns its column definitions). */
    private String tableName;

    /** Optional schema qualifier for the table. */
    private String schemaName;

    /**
     * Column that is the hypertable's time dimension. The record field of the same name supplies
     * the value, which is coerced to a timestamp before writing.
     */
    private String timeColumn;

    /** Unit of a numeric time value (epoch). ISO-8601 string time values ignore this. */
    @Builder.Default private TimeUnit timeUnit = TimeUnit.MILLISECONDS;

    /**
     * When true (default), run {@code create_hypertable(..., if_not_exists => TRUE)} on startup.
     */
    @Builder.Default private boolean createHypertable = true;

    /** INSERT or UPSERT, reusing the JDBC sink's write semantics. */
    @Builder.Default private JdbcSinkDto.WriteMode writeMode = JdbcSinkDto.WriteMode.INSERT;

    /** Comma-separated conflict-key columns; required when {@code writeMode} is UPSERT. */
    private String upsertKeyColumns;

    @Builder.Default private int batchSize = DEFAULT_BATCH_SIZE;
  }
}
