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
package io.fleak.zephflow.lib.commands.azuremonitorsink;

import io.fleak.zephflow.api.CommandConfig;
import lombok.*;

public interface AzureMonitorSinkDto {

  int DEFAULT_BATCH_SIZE = 500;

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {
    @NonNull private String tenantId;
    @NonNull private String dceEndpoint;

    @NonNull private String dcrImmutableId;

    @NonNull private String streamName;

    @NonNull private String credentialId;

    @Builder.Default private String timeGeneratedField = "TimeGenerated";

    @Builder.Default private Integer batchSize = DEFAULT_BATCH_SIZE;
  }
}
