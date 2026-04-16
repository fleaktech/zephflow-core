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
package io.fleak.zephflow.lib.commands.sentinelsink;

import io.fleak.zephflow.api.CommandConfig;
import lombok.*;

public interface SentinelSinkDto {

  int DEFAULT_BATCH_SIZE = 500;

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {
    /** Azure Log Analytics Workspace ID (GUID). */
    @NonNull private String workspaceId;
    /**
     * ID of a stored UsernamePasswordCredential where password is the workspace primary or
     * secondary shared key (base64-encoded).
     */
    @NonNull private String credentialId;
    /**
     * Custom log table name. Must be alphanumeric/underscore only and max 100 characters. Azure
     * will append "_CL" suffix to this name in the workspace.
     */
    @NonNull private String logType;
    /** Field name in the event to use as the timestamp. Defaults to "TimeGenerated". */
    @Builder.Default private String timeGeneratedField = "TimeGenerated";

    @Builder.Default private Integer batchSize = DEFAULT_BATCH_SIZE;
  }
}
