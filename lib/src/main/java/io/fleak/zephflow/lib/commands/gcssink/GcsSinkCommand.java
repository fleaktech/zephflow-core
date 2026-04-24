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
package io.fleak.zephflow.lib.commands.gcssink;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import com.google.cloud.storage.Storage;
import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.credentials.GcpCredential;
import io.fleak.zephflow.lib.gcp.GcsClientFactory;
import org.apache.commons.lang3.StringUtils;

public class GcsSinkCommand extends SimpleSinkCommand<GcsOutboundMessage> {

  private final GcsClientFactory gcsClientFactory;

  protected GcsSinkCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      GcsClientFactory gcsClientFactory) {
    super(nodeId, jobContext, configParser, configValidator);
    this.gcsClientFactory = gcsClientFactory;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_GCS_SINK;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    SinkCounters counters =
        createSinkCounters(metricClientProvider, jobContext, commandName(), nodeId);

    GcsSinkDto.Config config = (GcsSinkDto.Config) commandConfig;
    SimpleSinkCommand.Flusher<GcsOutboundMessage> flusher = createFlusher(config, jobContext);
    SimpleSinkCommand.SinkMessagePreProcessor<GcsOutboundMessage> messagePreProcessor =
        new GcsSinkMessageProcessor();

    return new SinkExecutionContext<>(
        flusher,
        messagePreProcessor,
        counters.inputMessageCounter(),
        counters.errorCounter(),
        counters.sinkOutputCounter(),
        counters.outputSizeCounter(),
        counters.sinkErrorCounter());
  }

  private SimpleSinkCommand.Flusher<GcsOutboundMessage> createFlusher(
      GcsSinkDto.Config config, JobContext jobContext) {
    Storage storage;
    if (StringUtils.isNotBlank(config.getCredentialId())) {
      GcpCredential credential = lookupGcpCredential(jobContext, config.getCredentialId());
      storage = gcsClientFactory.createStorageClient(credential);
    } else {
      storage = gcsClientFactory.createStorageClient();
    }

    String prefix =
        StringUtils.isNotBlank(config.getObjectPrefix())
            ? config.getObjectPrefix()
            : GcsSinkDto.DEFAULT_OBJECT_PREFIX;
    return new GcsSinkFlusher(storage, config.getBucketName(), prefix);
  }

  @Override
  protected int batchSize() {
    GcsSinkDto.Config config = (GcsSinkDto.Config) commandConfig;
    return config.getBatchSize() != null ? config.getBatchSize() : GcsSinkDto.DEFAULT_BATCH_SIZE;
  }
}
