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
package io.fleak.zephflow.lib.commands.elasticsearchsource;

import io.fleak.zephflow.api.CommandConfig;
import lombok.*;

public interface ElasticsearchSourceDto {

  int DEFAULT_BATCH_SIZE = 500;
  String DEFAULT_SCROLL_TIMEOUT = "5m";

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {
    /** Elasticsearch host URL (e.g. https://my-cluster.es.io:9200). */
    @NonNull private String host;

    /**
     * ID of a stored UsernamePasswordCredential where username is the ES username and password is
     * the ES password or API key.
     */
    private String credentialId;

    /** Index name or pattern to read from (e.g. "logs-*"). */
    @NonNull private String index;

    /**
     * Optional Elasticsearch query DSL JSON (the "query" part only). When null, matches all
     * documents.
     */
    private String query;

    /** Scroll context keep-alive duration. Defaults to "5m". */
    @Builder.Default private String scrollTimeout = DEFAULT_SCROLL_TIMEOUT;

    /** Number of documents to fetch per scroll page. Defaults to 500. */
    @Builder.Default private int batchSize = DEFAULT_BATCH_SIZE;

    /** Encoding type for deserializing document _source fields. */
    @NonNull private String encodingType;
  }
}
