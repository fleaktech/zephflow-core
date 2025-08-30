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
package io.fleak.zephflow.lib.commands.deltalakesink;

import io.fleak.zephflow.api.CommandConfig;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.*;

public class DeltaLakeSinkDto {

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Config implements CommandConfig {
    private String tablePath;

    private List<String> partitionColumns;

    @Builder.Default private int batchSize = 1000;

    @Builder.Default private Map<String, String> hadoopConfiguration = new HashMap<>();
    private String credentialId;
  }
}
