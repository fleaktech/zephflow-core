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
package io.fleak.zephflow.lib.commands.databrickssink;

import io.fleak.zephflow.api.CommandConfig;
import java.util.HashMap;
import java.util.Map;
import lombok.*;

public class DatabricksSinkDto {

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Config implements CommandConfig {
    private String databricksCredentialId;
    private String volumePath;
    private String tableName;
    private String warehouseId;
    private Map<String, Object> avroSchema;

    @Builder.Default private int batchSize = 10000;

    @Builder.Default private long flushIntervalMillis = 30_000;

    @Builder.Default private boolean cleanupAfterCopy = true;

    @Builder.Default private Map<String, String> copyOptions = new HashMap<>();

    @Builder.Default private Map<String, String> formatOptions = new HashMap<>();
  }
}
