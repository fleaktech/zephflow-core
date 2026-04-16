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
package io.fleak.zephflow.lib.commands.azureblobsink;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.azure.AzureClientFactory;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import org.apache.commons.lang3.StringUtils;

public class AzureBlobSinkCommand extends SimpleSinkCommand<AzureBlobOutboundMessage> {

  private final AzureClientFactory azureClientFactory;

  protected AzureBlobSinkCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      AzureClientFactory azureClientFactory) {
    super(nodeId, jobContext, configParser, configValidator);
    this.azureClientFactory = azureClientFactory;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_AZURE_BLOB_SINK;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    SinkCounters counters =
        createSinkCounters(metricClientProvider, jobContext, commandName(), nodeId);

    AzureBlobSinkDto.Config config = (AzureBlobSinkDto.Config) commandConfig;
    SimpleSinkCommand.Flusher<AzureBlobOutboundMessage> flusher = createFlusher(config, jobContext);
    SimpleSinkCommand.SinkMessagePreProcessor<AzureBlobOutboundMessage> messagePreProcessor =
        new AzureBlobSinkMessageProcessor();

    return new SinkExecutionContext<>(
        flusher,
        messagePreProcessor,
        counters.inputMessageCounter(),
        counters.errorCounter(),
        counters.sinkOutputCounter(),
        counters.outputSizeCounter(),
        counters.sinkErrorCounter());
  }

  private SimpleSinkCommand.Flusher<AzureBlobOutboundMessage> createFlusher(
      AzureBlobSinkDto.Config config, JobContext jobContext) {
    BlobServiceClient serviceClient;

    if (StringUtils.isNotBlank(config.getConnectionString())) {
      serviceClient =
          azureClientFactory.createBlobServiceClientFromConnectionString(
              config.getConnectionString());
    } else {
      UsernamePasswordCredential credential =
          lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
      serviceClient =
          azureClientFactory.createBlobServiceClientFromAccountKey(
              credential.getUsername(), credential.getPassword());
    }

    BlobContainerClient containerClient =
        serviceClient.getBlobContainerClient(config.getContainerName());
    String prefix =
        StringUtils.isNotBlank(config.getBlobNamePrefix()) ? config.getBlobNamePrefix() : "events/";
    return new AzureBlobSinkFlusher(containerClient, prefix);
  }

  @Override
  protected int batchSize() {
    return AzureBlobSinkDto.DEFAULT_BATCH_SIZE;
  }
}
