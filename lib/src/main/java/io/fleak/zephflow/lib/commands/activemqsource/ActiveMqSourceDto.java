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
package io.fleak.zephflow.lib.commands.activemqsource;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public interface ActiveMqSourceDto {

  int DEFAULT_COMMIT_BATCH_SIZE = 1000;
  long DEFAULT_COMMIT_INTERVAL_MS = 5000L;
  long DEFAULT_RECEIVE_TIMEOUT_MS = 1000L;
  int DEFAULT_MAX_BATCH_SIZE = 500;

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {
    private String brokerUrl;
    private BrokerType brokerType;
    private String destination;
    @Builder.Default private DestinationType destinationType = DestinationType.QUEUE;
    private EncodingType encodingType;
    private String credentialId;
    private String clientId;
    private String subscriptionName;
    private Map<String, String> properties;

    @Builder.Default private CommitStrategyType commitStrategy = CommitStrategyType.BATCH;
    @Builder.Default private Integer commitBatchSize = DEFAULT_COMMIT_BATCH_SIZE;
    @Builder.Default private Long commitIntervalMs = DEFAULT_COMMIT_INTERVAL_MS;
    @Builder.Default private Long receiveTimeoutMs = DEFAULT_RECEIVE_TIMEOUT_MS;
    @Builder.Default private Integer maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
  }

  enum BrokerType {
    CLASSIC,
    ARTEMIS
  }

  enum DestinationType {
    QUEUE,
    TOPIC
  }

  enum CommitStrategyType {
    PER_RECORD,
    BATCH,
    NONE
  }
}
