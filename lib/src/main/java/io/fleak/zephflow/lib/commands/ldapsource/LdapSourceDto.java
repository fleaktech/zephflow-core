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
package io.fleak.zephflow.lib.commands.ldapsource;

import io.fleak.zephflow.api.CommandConfig;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public interface LdapSourceDto {

  int DEFAULT_PAGE_SIZE = 1000;
  int DEFAULT_TIME_LIMIT_MS = 30000;

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {
    private String ldapUrl;
    private String credentialId;
    private String baseDn;
    private String searchFilter;
    private List<String> attributes;
    @Builder.Default private SearchScope searchScope = SearchScope.SUBTREE;
    @Builder.Default private Integer pageSize = DEFAULT_PAGE_SIZE;
    @Builder.Default private Integer timeLimitMs = DEFAULT_TIME_LIMIT_MS;
  }

  enum SearchScope {
    SUBTREE,
    ONELEVEL,
    OBJECT
  }
}
