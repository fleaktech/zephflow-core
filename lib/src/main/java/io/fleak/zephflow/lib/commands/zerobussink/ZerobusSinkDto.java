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
package io.fleak.zephflow.lib.commands.zerobussink;

import io.fleak.zephflow.api.CommandConfig;
import java.util.Map;
import lombok.*;

/**
 * Configuration for the Databricks Zerobus sink. Unlike {@code databrickssink}, Zerobus writes
 * records directly into a Unity Catalog managed Delta table over a gRPC stream. It does not use a
 * SQL warehouse or a staging volume, so neither {@code warehouseId} nor {@code volumePath} are part
 * of this config.
 */
public class ZerobusSinkDto {

  public static final String ENCODING_PROTOBUF = "protobuf";
  public static final String ENCODING_JSON = "json";

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Config implements CommandConfig {
    /** Key used to look up the {@code DatabricksCredential} in the JobContext. */
    private String databricksCredentialId;

    /** gRPC ingest endpoint, e.g. {@code https://<id>.zerobus.<region>.cloud.databricks.com}. */
    private String zerobusEndpoint;

    /** Fully-qualified Unity Catalog table name: {@code catalog.schema.table}. */
    private String tableName;

    /** Avro schema of the target table. Used to build the protobuf descriptor at runtime. */
    private Map<String, Object> avroSchema;

    /** Wire encoding: {@code protobuf} (default) or {@code json}. */
    @Builder.Default private String encodingType = ENCODING_PROTOBUF;

    /**
     * Store-and-forward: when enabled, a connectivity failure persists records to a local durable
     * queue and replays them once the connection recovers, instead of dropping them.
     */
    @Builder.Default private boolean storeAndForwardEnabled = false;

    /** Directory for the local store-and-forward queue. Defaults to a temp dir keyed by node id. */
    private String localStorePath;

    /** Hard cap on local store size; once reached, incoming records are dropped. */
    @Builder.Default private long localStoreMaxBytes = 1_073_741_824L; // 1 GiB

    /** How often the background forwarder retries draining the local queue during an outage. */
    @Builder.Default private long forwardRetryIntervalMillis = 5_000L;

    /**
     * Max time to wait for a server ack before treating the connection as down. Lower than the SDK
     * default (60s) so an outage is detected in seconds rather than minutes.
     */
    @Builder.Default private int ackTimeoutMillis = 20_000;

    /**
     * Max time a flush blocks waiting for queued records to be acknowledged. Lower than the SDK
     * default (5min) to bound how long a flush parks the native thread when offline.
     */
    @Builder.Default private int flushTimeoutMillis = 60_000;
  }
}
