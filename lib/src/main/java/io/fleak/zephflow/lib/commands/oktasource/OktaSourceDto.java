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
package io.fleak.zephflow.lib.commands.oktasource;

import io.fleak.zephflow.api.CommandConfig;
import lombok.*;

public interface OktaSourceDto {

  int DEFAULT_LIMIT = 100;
  int MAX_LIMIT = 1000;

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {
    /** Okta organization domain, e.g. "mycompany.okta.com". */
    @NonNull private String oktaDomain;
    /**
     * ID of a stored UsernamePasswordCredential. The password field contains the Okta API token
     * (SSWS token). The username field is not used.
     */
    @NonNull private String credentialId;
    /**
     * ISO 8601 timestamp to start fetching events from. If not provided, events are fetched from
     * the current time.
     */
    private String sinceTimestamp;
    /** Optional SCIM filter expression, e.g. {@code eventType eq "user.session.start"}. */
    private String filter;
    /** Number of events to fetch per page. Defaults to 100, max 1000. */
    @Builder.Default private Integer limit = DEFAULT_LIMIT;
  }
}
