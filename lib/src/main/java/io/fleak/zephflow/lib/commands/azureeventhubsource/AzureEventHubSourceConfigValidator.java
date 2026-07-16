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

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.azureeventhub.AzureEventHubConfigValidation;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import org.apache.commons.lang3.StringUtils;

/** Validates {@link AzureEventHubSourceDto.Config}. */
public class AzureEventHubSourceConfigValidator implements ConfigValidator {

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    AzureEventHubSourceDto.Config config = (AzureEventHubSourceDto.Config) commandConfig;

    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getEventHubName()), "no eventHubName is provided");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getConsumerGroup()), "no consumerGroup is provided");

    // Exactly one Event Hub authentication mode.
    AzureEventHubConfigValidation.validateEventHubAuth(
        config.getConnectionString(),
        config.getFullyQualifiedNamespace(),
        config.getTenantId(),
        config.getClientId(),
        config.getClientSecret());

    // Checkpoint store: container required, plus exactly one storage auth mode.
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getCheckpointContainerName()),
        "no checkpointContainerName is provided");
    boolean hasStorageConnectionString =
        StringUtils.isNotBlank(config.getCheckpointStorageConnectionString());
    boolean hasStorageEndpoint = StringUtils.isNotBlank(config.getCheckpointStorageEndpoint());
    Preconditions.checkArgument(
        hasStorageConnectionString || hasStorageEndpoint,
        "checkpoint store requires either checkpointStorageConnectionString or checkpointStorageEndpoint");
    Preconditions.checkArgument(
        !(hasStorageConnectionString && hasStorageEndpoint),
        "provide only one of checkpointStorageConnectionString or checkpointStorageEndpoint, not both");

    Preconditions.checkNotNull(config.getEncodingType(), "no encoding type is provided");
    DeserializerFactory.validateEncodingType(config.getEncodingType());

    Preconditions.checkArgument(
        config.getMaxBufferedEvents() > 0,
        "maxBufferedEvents must be positive, got: %s",
        config.getMaxBufferedEvents());
    Preconditions.checkArgument(
        config.getMaxEventsPerFetch() > 0,
        "maxEventsPerFetch must be positive, got: %s",
        config.getMaxEventsPerFetch());

    if (config.getCommitStrategy() == AzureEventHubSourceDto.CommitStrategyType.BATCH) {
      if (config.getCommitBatchSize() != null) {
        Preconditions.checkArgument(
            config.getCommitBatchSize() > 0,
            "commitBatchSize must be positive, got: %s",
            config.getCommitBatchSize());
      }
      if (config.getCommitIntervalMs() != null) {
        Preconditions.checkArgument(
            config.getCommitIntervalMs() > 0,
            "commitIntervalMs must be positive, got: %s",
            config.getCommitIntervalMs());
      }
    }
  }
}
