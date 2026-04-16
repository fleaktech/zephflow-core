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

import io.fleak.zephflow.api.CommandConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public class JdbcSinkDto {

  public static final int DEFAULT_BATCH_SIZE = 1000;

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Config implements CommandConfig {
    private String jdbcUrl;
    private String credentialId;
    private String tableName;
    private String schemaName;
    @Builder.Default private WriteMode writeMode = WriteMode.INSERT;
    private String upsertKeyColumns;
    @Builder.Default private int batchSize = DEFAULT_BATCH_SIZE;
    private String driverClassName;
  }

  public enum WriteMode {
    INSERT,
    UPSERT
  }
}
