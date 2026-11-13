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

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.require;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.ConfigurationIssue;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.azureeventhub.AzureEventHubConfigValidation;
import io.fleak.zephflow.lib.commands.filedecode.SourceDecoding;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

/** Validates {@link AzureIotHubSourceDto.Config}. */
public class AzureIotHubSourceConfigValidator implements ConfigValidator {

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    AzureIotHubSourceDto.Config config = (AzureIotHubSourceDto.Config) commandConfig;

    require(
        StringUtils.isNotBlank(config.getEventHubName()),
        of("eventHubName"),
        "eventHubName is required and must not be blank.",
        "no eventHubName is provided");
    require(
        StringUtils.isNotBlank(config.getConsumerGroup()),
        of("consumerGroup"),
        "consumerGroup is required and must not be blank.",
        "no consumerGroup is provided");

    // Exactly one authentication mode (shared with the Event Hub connector).
    AzureEventHubConfigValidation.validateEventHubAuth(
        config.getConnectionString(),
        config.getFullyQualifiedNamespace(),
        config.getTenantId(),
        config.getClientId(),
        config.getClientSecret());

    // Checkpoint store: container required, plus exactly one storage auth mode.
    require(
        StringUtils.isNotBlank(config.getCheckpointContainerName()),
        of("checkpointContainerName"),
        "checkpointContainerName is required and must not be blank.",
        "no checkpointContainerName is provided");
    boolean hasStorageConnectionString =
        StringUtils.isNotBlank(config.getCheckpointStorageConnectionString());
    boolean hasStorageEndpoint = StringUtils.isNotBlank(config.getCheckpointStorageEndpoint());
    if (!(hasStorageConnectionString || hasStorageEndpoint)) {
      throw new ConfigurationValidationException(
          new IllegalArgumentException(
              "checkpoint store requires either checkpointStorageConnectionString or checkpointStorageEndpoint"),
          List.of("checkpointStorageConnectionString", "checkpointStorageEndpoint").stream()
              .map(
                  field ->
                      new ConfigurationIssue(
                          of(field),
                          "checkpoint store requires either checkpointStorageConnectionString or checkpointStorageEndpoint"))
              .toList());
    }
    if (hasStorageConnectionString && hasStorageEndpoint) {
      throw new ConfigurationValidationException(
          new IllegalArgumentException(
              "provide only one of checkpointStorageConnectionString or checkpointStorageEndpoint, not both"),
          List.of("checkpointStorageConnectionString", "checkpointStorageEndpoint").stream()
              .map(
                  field ->
                      new ConfigurationIssue(
                          of(field),
                          "provide only one of checkpointStorageConnectionString or checkpointStorageEndpoint, not both"))
              .toList());
    }

    SourceDecoding.validate(AzureIotHubSourceCommand.decodingConfig(config));

    require(
        config.getMaxBufferedEvents() > 0,
        of("maxBufferedEvents"),
        "maxBufferedEvents must be greater than 0.",
        "maxBufferedEvents must be positive, got: %s",
        config.getMaxBufferedEvents());
    require(
        config.getMaxEventsPerFetch() > 0,
        of("maxEventsPerFetch"),
        "maxEventsPerFetch must be greater than 0.",
        "maxEventsPerFetch must be positive, got: %s",
        config.getMaxEventsPerFetch());

    if (config.getCommitStrategy() == AzureIotHubSourceDto.CommitStrategyType.BATCH) {
      if (config.getCommitBatchSize() != null) {
        require(
            config.getCommitBatchSize() > 0,
            of("commitBatchSize"),
            "commitBatchSize must be greater than 0 when commitStrategy is BATCH.",
            "commitBatchSize must be positive, got: %s",
            config.getCommitBatchSize());
      }
      if (config.getCommitIntervalMs() != null) {
        require(
            config.getCommitIntervalMs() > 0,
            of("commitIntervalMs"),
            "commitIntervalMs must be greater than 0 when commitStrategy is BATCH.",
            "commitIntervalMs must be positive, got: %s",
            config.getCommitIntervalMs());
      }
    }
  }
}
