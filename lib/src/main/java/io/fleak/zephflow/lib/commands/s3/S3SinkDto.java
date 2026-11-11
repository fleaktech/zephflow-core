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
package io.fleak.zephflow.lib.commands.s3;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.lib.serdes.encode.EncodingConfig;
import lombok.*;

/** Created by bolei on 9/3/24 */
public interface S3SinkDto {
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {
    @NonNull private String regionStr;
    @NonNull private String bucketName;
    @NonNull private String keyName;
    private String credentialId;
    private String s3EndpointOverride;
    @Builder.Default private boolean batching = true;
    @Builder.Default private int batchSize = 10_000;
    @Builder.Default private long flushIntervalMillis = 10_000;
    private EncodingConfig encoding;

    /**
     * Store-and-forward: when enabled, a batch that fails because of the destination is kept in a
     * local durable queue and uploaded once the destination accepts it again, instead of dropped.
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
