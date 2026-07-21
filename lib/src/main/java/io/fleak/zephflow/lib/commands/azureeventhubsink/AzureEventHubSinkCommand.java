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
package io.fleak.zephflow.lib.commands.azureeventhubsink;

import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_AZURE_EVENTHUB_SINK;

import com.azure.messaging.eventhubs.EventHubProducerClient;
import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.azureeventhub.AzureEventHubClientFactory;
import io.fleak.zephflow.lib.commands.azureeventhub.AzureEventHubConnectionConfig;
import io.fleak.zephflow.lib.commands.sink.PassThroughMessagePreProcessor;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

/** Sink that publishes records to an Azure Event Hub via the native producer client. */
public class AzureEventHubSinkCommand extends SimpleSinkCommand<RecordFleakData> {

  private final AzureEventHubClientFactory clientFactory;

  protected AzureEventHubSinkCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      AzureEventHubClientFactory clientFactory) {
    super(nodeId, jobContext, configParser, configValidator);
    this.clientFactory = clientFactory;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_AZURE_EVENTHUB_SINK;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    SinkCounters counters =
        createSinkCounters(metricClientProvider, jobContext, commandName(), nodeId);

    AzureEventHubSinkDto.Config config = (AzureEventHubSinkDto.Config) commandConfig;

    SimpleSinkCommand.Flusher<RecordFleakData> flusher = createFlusher(config);
    SimpleSinkCommand.SinkMessagePreProcessor<RecordFleakData> messagePreProcessor =
        new PassThroughMessagePreProcessor();

    return new SinkExecutionContext<>(
        flusher,
        messagePreProcessor,
        counters.inputMessageCounter(),
        counters.errorCounter(),
        counters.sinkOutputCounter(),
        counters.outputSizeCounter(),
        counters.sinkErrorCounter());
  }

  private SimpleSinkCommand.Flusher<RecordFleakData> createFlusher(
      AzureEventHubSinkDto.Config config) {
    AzureEventHubConnectionConfig connection =
        new AzureEventHubConnectionConfig(
            config.getConnectionString(),
            config.getFullyQualifiedNamespace(),
            config.getEventHubName(),
            config.getTenantId(),
            config.getClientId(),
            config.getClientSecret());

    EventHubProducerClient producerClient = clientFactory.createProducerClient(connection);

    EncodingType encodingType = EncodingType.valueOf(config.getEncodingType().toUpperCase());
    FleakSerializer<?> serializer =
        SerializerFactory.createSerializerFactory(encodingType).createSerializer();

    PathExpression partitionKeyExpression =
        Optional.ofNullable(config.getPartitionKeyFieldExpressionStr())
            .filter(StringUtils::isNotBlank)
            .map(PathExpression::fromString)
            .orElse(null);

    return new AzureEventHubSinkFlusher(producerClient, serializer, partitionKeyExpression);
  }

  @Override
  protected int batchSize() {
    // Defer batching to EventDataBatch (which enforces the service's 1 MB limit), the way the Kafka
    // sink defers to the Kafka client's native batching.
    return Integer.MAX_VALUE;
  }
}
