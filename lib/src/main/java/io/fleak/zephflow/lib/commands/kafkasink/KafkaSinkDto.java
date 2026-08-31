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
package io.fleak.zephflow.lib.commands.kafkasink;

import io.fleak.zephflow.api.CommandConfig;
import java.util.Map;
import lombok.*;

public interface KafkaSinkDto {

  /**
   * Controls when the sink reports a flushed batch as delivered. Defaults to {@link #WAIT_FOR_ACK}.
   */
  enum DeliveryMode {
    /**
     * The default: wait for a broker acknowledgement of every record before returning, so a crash
     * cannot lose records the source has already checkpointed past.
     */
    WAIT_FOR_ACK,
    /**
     * Explicit opt-in for throughput over durability: return once records are handed to the
     * producer's client-side buffer without waiting for broker acknowledgements. Records still
     * buffered when the process crashes are lost.
     */
    FIRE_AND_FORGET
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {
    @NonNull private String broker;
    @NonNull private String topic;
    private String partitionKeyFieldExpressionStr;
    @NonNull private String encodingType;
    private Map<String, String> properties;

    private String credentialId;
    private String securityProtocol;
    private String saslMechanism;

    /**
     * Defaults to {@link DeliveryMode#WAIT_FOR_ACK}. Cannot be {@code FIRE_AND_FORGET} when {@link
     * #storeAndForwardEnabled} is set.
     */
    @Builder.Default private DeliveryMode deliveryMode = DeliveryMode.WAIT_FOR_ACK;

    /**
     * Jackson deserializes through the no-args constructor, which skips {@code @Builder.Default}
     * initializers, so a config without the field must still resolve to the default here.
     */
    public DeliveryMode getDeliveryMode() {
      return deliveryMode == null ? DeliveryMode.WAIT_FOR_ACK : deliveryMode;
    }

    /**
     * Store-and-forward: when enabled, a connectivity failure persists records to a local durable
     * queue and replays them once the broker is reachable again, instead of dropping them. This
     * also switches the producer to synchronous delivery so failures are detected at flush time.
     */
    @Builder.Default private boolean storeAndForwardEnabled = false;

    /** Directory for the local store-and-forward queue. Defaults to a temp dir keyed by node id. */
    private String localStorePath;

    /** Hard cap on local store size; once reached, incoming records are dropped. */
    @Builder.Default private long localStoreMaxBytes = 1_073_741_824L; // 1 GiB

    /** How often the background forwarder retries draining the local queue during an outage. */
    @Builder.Default private long forwardRetryIntervalMillis = 5_000L;
  }
}
