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
package io.fleak.zephflow.lib.commands.azureeventhubsource;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.lib.serdes.EncodingType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Configuration for the Azure Event Hub source connector. */
public interface AzureEventHubSourceDto {

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {

    // --- Event Hub connection / authentication (exactly one auth mode) ---

    /** SAS connection string for the Event Hub namespace or entity. */
    private String connectionString;

    /** Fully qualified namespace, e.g. {@code my-namespace.servicebus.windows.net} (Entra ID). */
    private String fullyQualifiedNamespace;

    /** Event Hub (topic) name to consume from. */
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

    // --- Decoding ---
    private EncodingType encodingType;

    /** Where to start when a partition has no persisted checkpoint. */
    @Builder.Default private InitialPosition initialPosition = InitialPosition.EARLIEST;

    // --- Push-to-pull buffering ---

    /**
     * Bound on events buffered between the processor's push callback and the pull loop. A full
     * buffer blocks the callback, applying backpressure to Event Hub.
     */
    @Builder.Default private int maxBufferedEvents = 1024;

    /** Maximum number of events a single {@code fetch()} drains from the buffer. */
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
