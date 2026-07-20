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
package io.fleak.zephflow.lib.commands.azureiothubsource;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.lib.serdes.EncodingType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Configuration for the Azure IoT Hub source connector. Consumes device-to-cloud telemetry from an
 * IoT Hub built-in <b>Event Hub-compatible endpoint</b> (IoT Hub portal → Hub settings → Built-in
 * endpoints). The connection string and name below are the Event Hub-compatible values found there.
 */
public interface AzureIotHubSourceDto {

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {

    // --- IoT Hub built-in endpoint connection / authentication (exactly one auth mode) ---

    /** Event Hub-compatible connection string for the IoT Hub built-in endpoint. */
    private String connectionString;

    /** Event Hub-compatible fully qualified namespace (Entra ID auth path). */
    private String fullyQualifiedNamespace;

    /** Event Hub-compatible name of the built-in endpoint. */
    private String eventHubName;

    /** Consumer group to read under. Defaults to the built-in {@code $Default} group. */
    @Builder.Default private String consumerGroup = "$Default";

    // --- Optional Entra ID service principal; when omitted, DefaultAzureCredential is used ---
    private String tenantId;
    private String clientId;
    private String clientSecret;

    // --- Blob checkpoint store (durable partition offsets; exactly one storage auth mode) ---

    /** Connection string for the Azure Storage account that backs the checkpoint store. */
    private String checkpointStorageConnectionString;

    /** Blob endpoint for the checkpoint storage account (Entra ID auth path). */
    private String checkpointStorageEndpoint;

    /** Blob container that holds checkpoint and ownership data. */
    private String checkpointContainerName;

    // --- Decoding of the telemetry message body ---
    private EncodingType encodingType;

    /** Where to start when a partition has no persisted checkpoint. */
    @Builder.Default private InitialPosition initialPosition = InitialPosition.EARLIEST;

    // --- Push-to-pull buffering ---
    @Builder.Default private int maxBufferedEvents = 1024;
    @Builder.Default private int maxEventsPerFetch = 500;

    // --- Commit (checkpoint) strategy ---
    @Builder.Default private CommitStrategyType commitStrategy = CommitStrategyType.BATCH;
    @Builder.Default private Integer commitBatchSize = 1000;
    @Builder.Default private Long commitIntervalMs = 5000L;
  }

  enum CommitStrategyType {
    PER_RECORD,
    BATCH,
    NONE
  }

  enum InitialPosition {
    EARLIEST,
    LATEST
  }
}
