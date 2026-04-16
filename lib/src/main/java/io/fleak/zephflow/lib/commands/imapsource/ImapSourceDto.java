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
package io.fleak.zephflow.lib.commands.imapsource;

import io.fleak.zephflow.api.CommandConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public interface ImapSourceDto {

  int DEFAULT_PORT = 993;
  String DEFAULT_FOLDER = "INBOX";
  long DEFAULT_POLL_INTERVAL_MS = 60000L;
  boolean DEFAULT_MARK_AS_READ = true;
  boolean DEFAULT_INCLUDE_ATTACHMENTS = false;
  boolean DEFAULT_USE_SSL = true;
  int DEFAULT_MAX_MESSAGES = 100;

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {
    private String host;
    @Builder.Default private Integer port = DEFAULT_PORT;
    private String credentialId;
    @Builder.Default private AuthType authType = AuthType.PASSWORD;
    @Builder.Default private String folder = DEFAULT_FOLDER;
    private String searchCriteria;
    @Builder.Default private Long pollIntervalMs = DEFAULT_POLL_INTERVAL_MS;
    @Builder.Default private Boolean markAsRead = DEFAULT_MARK_AS_READ;
    @Builder.Default private Boolean includeAttachments = DEFAULT_INCLUDE_ATTACHMENTS;
    @Builder.Default private Boolean useSsl = DEFAULT_USE_SSL;
    @Builder.Default private Integer maxMessages = DEFAULT_MAX_MESSAGES;
  }

  enum AuthType {
    PASSWORD,
    OAUTH2
  }
}
