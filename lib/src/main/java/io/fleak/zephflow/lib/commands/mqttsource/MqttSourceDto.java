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
package io.fleak.zephflow.lib.commands.mqttsource;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.lib.serdes.EncodingType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public interface MqttSourceDto {

  int DEFAULT_QOS = 1;
  long DEFAULT_RECEIVE_TIMEOUT_MS = 1000L;
  int DEFAULT_MAX_BATCH_SIZE = 500;
  int DEFAULT_RECEIVE_QUEUE_CAPACITY = 10000;

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {
    private String brokerUrl;
    private String topicFilter;
    private EncodingType encodingType;
    private String clientId;
    private String credentialId;

    @Builder.Default private int qos = DEFAULT_QOS;
    @Builder.Default private boolean cleanStart = true;
    private Long sessionExpiryIntervalSeconds;
    @Builder.Default private boolean useTls = false;

    @Builder.Default private Long receiveTimeoutMs = DEFAULT_RECEIVE_TIMEOUT_MS;
    @Builder.Default private Integer maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
    @Builder.Default private Integer receiveQueueCapacity = DEFAULT_RECEIVE_QUEUE_CAPACITY;
  }
}
