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
package io.fleak.zephflow.lib.commands.azureeventhub;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.messaging.eventhubs.CheckpointStore;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.EventProcessorClientBuilder;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.messaging.eventhubs.models.ErrorContext;
import com.azure.messaging.eventhubs.models.EventContext;
import com.azure.messaging.eventhubs.models.EventPosition;
import com.azure.storage.blob.BlobContainerAsyncClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import java.io.Serializable;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;

/**
 * Central place that wires the Azure Event Hub client builders. Both the source and the sink
 * resolve authentication identically here — a SAS connection string, or Entra ID (Azure AD) via a
 * {@link TokenCredential} — so the two connectors cannot drift apart.
 *
 * <p>Serializable and injectable so commands can substitute a stub in unit tests, mirroring {@code
 * KafkaConsumerClientFactory} / {@code KafkaProducerClientFactory}.
 */
public class AzureEventHubClientFactory implements Serializable {

  /** Builds a producer client (sink) for the given Event Hub connection. */
  public EventHubProducerClient createProducerClient(AzureEventHubConnectionConfig connection) {
    EventHubClientBuilder builder = new EventHubClientBuilder();
    if (connection.usesConnectionString()) {
      builder.connectionString(connection.connectionString(), connection.eventHubName());
    } else {
      builder.credential(
          connection.fullyQualifiedNamespace(),
          connection.eventHubName(),
          resolveTokenCredential(connection));
    }
    return builder.buildProducerClient();
  }

  /**
   * Builds a partition-load-balancing processor client (source). The {@code processEvent} callback
   * is invoked per event (one thread per owned partition) and {@code processError} on partition
   * errors. {@code initialPosition} is only used for partitions that have no persisted checkpoint.
   */
  public EventProcessorClient createProcessorClient(
      AzureEventHubConnectionConfig connection,
      String consumerGroup,
      CheckpointStore checkpointStore,
      EventPosition initialPosition,
      Consumer<EventContext> processEvent,
      Consumer<ErrorContext> processError) {
    EventProcessorClientBuilder builder =
        new EventProcessorClientBuilder()
            .consumerGroup(consumerGroup)
            .checkpointStore(checkpointStore)
            .initialPartitionEventPosition(partitionId -> initialPosition)
            .processEvent(processEvent)
            .processError(processError);
    if (connection.usesConnectionString()) {
      builder.connectionString(connection.connectionString(), connection.eventHubName());
    } else {
      builder.credential(
          connection.fullyQualifiedNamespace(),
          connection.eventHubName(),
          resolveTokenCredential(connection));
    }
    return builder.buildEventProcessorClient();
  }

  /**
   * Builds the blob-backed {@link CheckpointStore} used by the source to persist partition offsets.
   * The checkpoint store lives in an Azure Storage account (distinct from the Event Hub namespace):
   * it is addressed either by a storage connection string, or by an endpoint authenticated with the
   * Event Hub's Entra ID credential.
   */
  public CheckpointStore createBlobCheckpointStore(
      AzureEventHubConnectionConfig eventHubConnection,
      String storageConnectionString,
      String storageEndpoint,
      String containerName) {
    BlobContainerClientBuilder builder =
        new BlobContainerClientBuilder().containerName(containerName);
    if (StringUtils.isNotBlank(storageConnectionString)) {
      builder.connectionString(storageConnectionString);
    } else {
      builder.endpoint(storageEndpoint).credential(resolveTokenCredential(eventHubConnection));
    }
    BlobContainerAsyncClient containerClient = builder.buildAsyncClient();
    return new BlobCheckpointStore(containerClient);
  }

  private TokenCredential resolveTokenCredential(AzureEventHubConnectionConfig connection) {
    if (connection.usesServicePrincipal()) {
      return new ClientSecretCredentialBuilder()
          .tenantId(connection.tenantId())
          .clientId(connection.clientId())
          .clientSecret(connection.clientSecret())
          .build();
    }
    return new DefaultAzureCredentialBuilder().build();
  }
}
