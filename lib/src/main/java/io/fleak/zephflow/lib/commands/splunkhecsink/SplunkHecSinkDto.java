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
package io.fleak.zephflow.lib.commands.splunkhecsink;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.lib.serdes.encode.EncodingConfig;
import io.fleak.zephflow.lib.serdes.ser.kv.KvSinkConfig;
import java.util.List;
import lombok.*;

public interface SplunkHecSinkDto {

  int DEFAULT_BATCH_SIZE = 500;

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig, KvSinkConfig {
    @NonNull private String hecUrl;

    @NonNull private String credentialId;

    private String index;

    private String sourcetype;

    private String source;

    @Builder.Default private Boolean verifySsl = true;

    @Builder.Default private Integer batchSize = DEFAULT_BATCH_SIZE;

    private String encodingType;

    private List<String> csvColumns;

    private List<String> kvFields;

    private String kvPairDelimiter;

    private Boolean kvCleanFields;

    private EncodingConfig encoding;

    /**
     * Store-and-forward: when enabled, a batch that fails because of HEC is kept in a local durable
     * queue and delivered once HEC accepts it again, instead of being dropped.
     */
    @Builder.Default private boolean storeAndForwardEnabled = false;

    /** Directory for the local store-and-forward queue. Defaults to a temp dir keyed by node id. */
    private String localStorePath;

    /** Hard cap on local store size; once reached, incoming records are dropped. */
    @Builder.Default private long localStoreMaxBytes = 1_073_741_824L;

    /** How often the background forwarder retries draining the local queue during an outage. */
    @Builder.Default private long forwardRetryIntervalMillis = 5_000L;
  }
}
