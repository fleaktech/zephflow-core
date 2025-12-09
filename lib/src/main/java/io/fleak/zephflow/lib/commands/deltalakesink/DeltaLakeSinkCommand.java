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
package io.fleak.zephflow.lib.commands.deltalakesink;

import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_DELTA_LAKE_SINK;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.S3DlqWriter;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DeltaLakeSinkCommand extends SimpleSinkCommand<Map<String, Object>> {

  protected DeltaLakeSinkCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator) {
    super(nodeId, jobContext, configParser, configValidator);
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    SinkCounters counters =
        createSinkCounters(metricClientProvider, jobContext, commandName(), nodeId);

    DeltaLakeSinkDto.Config config = (DeltaLakeSinkDto.Config) commandConfig;
    SimpleSinkCommand.Flusher<Map<String, Object>> flusher =
        createDeltaLakeFlusher(config, jobContext);
    SimpleSinkCommand.SinkMessagePreProcessor<Map<String, Object>> messagePreProcessor =
        new DeltaLakeMessageProcessor();

    return new SinkExecutionContext<>(
        flusher,
        messagePreProcessor,
        counters.inputMessageCounter(),
        counters.errorCounter(),
        counters.sinkOutputCounter(),
        counters.outputSizeCounter(),
        counters.sinkErrorCounter());
  }

  private SimpleSinkCommand.Flusher<Map<String, Object>> createDeltaLakeFlusher(
      DeltaLakeSinkDto.Config config, JobContext jobContext) {
    DlqWriter dlqWriter = createDlqWriter();
    DeltaLakeWriter writer = new DeltaLakeWriter(config, jobContext, dlqWriter);
    writer.initialize();
    return writer;
  }

  private DlqWriter createDlqWriter() {
    JobContext.DlqConfig dlqConfig = jobContext.getDlqConfig();
    if (dlqConfig == null) {
      return null;
    }
    if (dlqConfig instanceof JobContext.S3DlqConfig s3DlqConfig) {
      DlqWriter writer = S3DlqWriter.createS3DlqWriter(s3DlqConfig);
      writer.open();
      return writer;
    }
    log.warn("Unsupported DLQ config type: {}", dlqConfig.getClass());
    return null;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_DELTA_LAKE_SINK;
  }

  @Override
  protected int batchSize() {
    if (commandConfig != null) {
      DeltaLakeSinkDto.Config config = (DeltaLakeSinkDto.Config) commandConfig;
      return config.getBatchSize();
    }
    return 1000;
  }
}
