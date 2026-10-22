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
package io.fleak.zephflow.lib.commands.kafkasource;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import java.util.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Created by bolei on 9/24/24 */
public interface KafkaSourceDto {

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {
    private String broker;
    private String topic;
    private String groupId;

    /**
     * Encoding of the messages, translated to a decoding configuration; a message that starts with
     * the gzip magic bytes is gunzipped. Exactly one of this and {@code decoding} is set.
     */
    private EncodingType encodingType;

    /** How the messages are decoded; exactly one of this and {@code encodingType}. */
    private DecodingConfig decoding;

    private Map<String, String> properties;

    // Commit strategy configuration
    @Builder.Default private CommitStrategyType commitStrategy = CommitStrategyType.BATCH;
    @Builder.Default private Integer commitBatchSize = 1000;
    @Builder.Default private Long commitIntervalMs = 5000L;
  }

  enum CommitStrategyType {
    PER_RECORD,
    BATCH,
    NONE
  }
}
