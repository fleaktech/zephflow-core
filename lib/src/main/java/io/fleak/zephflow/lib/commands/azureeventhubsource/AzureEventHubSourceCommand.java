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

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import com.azure.messaging.eventhubs.CheckpointStore;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.models.ErrorContext;
import com.azure.messaging.eventhubs.models.EventContext;
import com.azure.messaging.eventhubs.models.EventPosition;
import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.azureeventhub.AzureEventHubClientFactory;
import io.fleak.zephflow.lib.commands.azureeventhub.AzureEventHubConnectionConfig;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

/** Streaming source that consumes from an Azure Event Hub via the native processor client. */
@Slf4j
public class AzureEventHubSourceCommand extends SimpleSourceCommand<SerializedEvent> {

  // Matches the Kafka source's 1s poll: bound how long an idle fetch() blocks before returning.
  private static final Duration FETCH_POLL_TIMEOUT = Duration.ofMillis(1000);

  private final AzureEventHubClientFactory clientFactory;

  public AzureEventHubSourceCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      AzureEventHubClientFactory clientFactory) {
    super(nodeId, jobContext, configParser, configValidator);
    this.clientFactory = clientFactory;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    AzureEventHubSourceDto.Config config = (AzureEventHubSourceDto.Config) commandConfig;

    Fetcher<SerializedEvent> fetcher = createFetcher(config);
    RawDataEncoder<SerializedEvent> encoder = new BytesRawDataEncoder();
    RawDataConverter<SerializedEvent> converter = createRawDataConverter(config);

    var metricTags = basicCommandMetricTags(jobContext.getMetricTags(), commandName(), nodeId);
    FleakCounter dataSizeCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_EVENT_SIZE_COUNT, metricTags);
    FleakCounter inputEventCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_EVENT_COUNT, metricTags);
    FleakCounter deserializeFailureCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_DESER_ERR_COUNT, metricTags);

    String keyPrefix = (String) jobContext.getOtherProperties().get(JobContext.DATA_KEY_PREFIX);
    DlqWriter dlqWriter =
        Optional.of(jobContext)
            .map(JobContext::getDlqConfig)
            .map(c -> DlqWriterFactory.createDlqWriter(c, keyPrefix))
            .orElse(null);
    if (dlqWriter != null) {
      dlqWriter.open();
    }

    return new SourceExecutionContext<>(
        fetcher,
        converter,
        encoder,
        dataSizeCounter,
        inputEventCounter,
        deserializeFailureCounter,
        dlqWriter);
  }

  private Fetcher<SerializedEvent> createFetcher(AzureEventHubSourceDto.Config config) {
    AzureEventHubConnectionConfig connection = connectionConfig(config);

    CheckpointStore checkpointStore =
        clientFactory.createBlobCheckpointStore(
            connection,
            config.getCheckpointStorageConnectionString(),
            config.getCheckpointStorageEndpoint(),
            config.getCheckpointContainerName());

    BlockingQueue<EventContext> queue = new LinkedBlockingQueue<>(config.getMaxBufferedEvents());
    Consumer<EventContext> onEvent =
        eventContext -> {
          try {
            queue.put(eventContext); // blocks when full -> backpressure to Event Hub
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        };
    Consumer<ErrorContext> onError =
        errorContext ->
            log.error(
                "Event Hub processor error on partition {}",
                errorContext.getPartitionContext().getPartitionId(),
                errorContext.getThrowable());

    EventPosition initialPosition =
        config.getInitialPosition() == AzureEventHubSourceDto.InitialPosition.LATEST
            ? EventPosition.latest()
            : EventPosition.earliest();

    EventProcessorClient processorClient =
        clientFactory.createProcessorClient(
            connection,
            config.getConsumerGroup(),
            checkpointStore,
            initialPosition,
            onEvent,
            onError);

    AzureEventHubSourceFetcher fetcher =
        new AzureEventHubSourceFetcher(
            processorClient,
            queue,
            config.getMaxEventsPerFetch(),
            FETCH_POLL_TIMEOUT,
            createCommitStrategy(config));

    processorClient.start();
    return fetcher;
  }

  private static AzureEventHubConnectionConfig connectionConfig(
      AzureEventHubSourceDto.Config config) {
    return new AzureEventHubConnectionConfig(
        config.getConnectionString(),
        config.getFullyQualifiedNamespace(),
        config.getEventHubName(),
        config.getTenantId(),
        config.getClientId(),
        config.getClientSecret());
  }

  private RawDataConverter<SerializedEvent> createRawDataConverter(
      AzureEventHubSourceDto.Config config) {
    FleakDeserializer<?> deserializer =
        DeserializerFactory.createDeserializerFactory(config.getEncodingType())
            .createDeserializer();
    return new BytesRawDataConverter(deserializer);
  }

  private CommitStrategy createCommitStrategy(AzureEventHubSourceDto.Config config) {
    return switch (config.getCommitStrategy()) {
      case PER_RECORD -> PerRecordCommitStrategy.INSTANCE;
      case BATCH ->
          new BatchCommitStrategy(
              config.getCommitBatchSize() != null ? config.getCommitBatchSize() : 1000,
              config.getCommitIntervalMs() != null ? config.getCommitIntervalMs() : 5000L);
      case NONE -> NoCommitStrategy.INSTANCE;
    };
  }

  @Override
  public SourceType sourceType() {
    return SourceType.STREAMING;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_AZURE_EVENTHUB_SOURCE;
  }
}
