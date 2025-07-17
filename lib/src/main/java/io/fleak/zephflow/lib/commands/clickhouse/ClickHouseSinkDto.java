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
package io.fleak.zephflow.lib.commands.clickhouse;

import io.fleak.zephflow.api.CommandConfig;
import java.util.HashMap;
import java.util.Map;
import lombok.*;

public class ClickHouseSinkDto {

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Config implements CommandConfig {
    private String username;
    private String password;
    @NonNull private String database;
    @NonNull private String table;
    @NonNull private String endpoint;
    @Builder.Default Map<String, Object> serverSettings = new HashMap<>();
    @Builder.Default private String clientName = "zephflow";
    private String credentialId;
    @Builder.Default private boolean compressServerResponse = true;
    @Builder.Default private boolean disableNativeCompression = false;
    @Builder.Default private boolean compressClientRequest = false;
  }
}
