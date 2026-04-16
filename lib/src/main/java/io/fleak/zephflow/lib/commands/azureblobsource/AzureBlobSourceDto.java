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
package io.fleak.zephflow.lib.commands.azureblobsource;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.lib.serdes.EncodingType;
import lombok.*;

public interface AzureBlobSourceDto {

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {
    @NonNull private String containerName;
    /** Azure Storage connection string. If provided, takes priority over credentialId. */
    private String connectionString;
    /**
     * ID of a stored UsernamePasswordCredential where username=storageAccountName and
     * password=storageAccountKey.
     */
    private String credentialId;
    /** Optional prefix to filter blobs (e.g. "logs/2024/"). */
    private String blobPrefix;

    @NonNull private EncodingType encodingType;
  }
}
