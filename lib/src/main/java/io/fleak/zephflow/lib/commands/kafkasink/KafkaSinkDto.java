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
   * When the sink's flush is allowed to return. The source checkpoint advances as soon as the sink
   * returns, so this is the pipeline's delivery guarantee.
   */
  enum DeliveryMode {
    /**
     * Default. Send the batch, then wait for a broker ack of every record before returning — no
     * silent loss on crash. Costs one broker round-trip per flushed batch; since the source
     * typically hands the sink one record per accept(), that usually means one round-trip per
     * source record. Latency-sensitive users who have measured should consider FIRE_AND_FORGET.
     */
    WAIT_FOR_ACK,
    /**
     * Return as soon as records are handed to the Kafka producer's client-side buffer. Highest
     * throughput, but records still buffered at crash time are silently lost even though the source
     * checkpoint has already advanced past them.
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
     * Delivery guarantee for the flush path; see {@link DeliveryMode}. {@code FIRE_AND_FORGET} is
     * an explicit throughput-over-durability opt-in and cannot be combined with {@link
     * #storeAndForwardEnabled}.
     */
    @Builder.Default private DeliveryMode deliveryMode = DeliveryMode.WAIT_FOR_ACK;

    /**
     * Never returns null: a config deserialized without the field (Jackson uses the no-args
     * constructor, which skips {@code @Builder.Default} initializers) must still get the durable
     * default.
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
