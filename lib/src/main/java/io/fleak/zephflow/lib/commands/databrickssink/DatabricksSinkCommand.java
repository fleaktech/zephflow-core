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
package io.fleak.zephflow.lib.commands.databrickssink;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import com.databricks.sdk.WorkspaceClient;
import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.deltalakesink.DeltaLakeMessageProcessor;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.credentials.DatabricksCredential;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.S3DlqWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class DatabricksSinkCommand extends SimpleSinkCommand<Map<String, Object>> {

  protected DatabricksSinkCommand(
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

    DatabricksSinkDto.Config config = (DatabricksSinkDto.Config) commandConfig;

    SinkCounters counters =
        createSinkCounters(metricClientProvider, jobContext, commandName(), nodeId);

    Flusher<Map<String, Object>> flusher = createBatchFlusher(config, counters);

    SinkMessagePreProcessor<Map<String, Object>> preprocessor = new DeltaLakeMessageProcessor();

    return new SinkExecutionContext<>(
        flusher,
        preprocessor,
        counters.inputMessageCounter(),
        counters.errorCounter(),
        counters.sinkOutputCounter(),
        counters.outputSizeCounter(),
        counters.sinkErrorCounter());
  }

  private Flusher<Map<String, Object>> createBatchFlusher(
      DatabricksSinkDto.Config config, SinkCounters counters) {
    try {
      Path tempDir = Files.createTempDirectory("databricks-sink-");
      DlqWriter dlqWriter = createDlqWriter();
      DatabricksCredential credential =
          lookupDatabricksCredential(jobContext, config.getDatabricksCredentialId());
      WorkspaceClient workspaceClient = DatabricksClientFactory.createClient(credential);
      BatchDatabricksFlusher flusher =
          new BatchDatabricksFlusher(
              config,
              workspaceClient,
              tempDir,
              dlqWriter,
              jobContext,
              counters.sinkOutputCounter(),
              counters.outputSizeCounter(),
              counters.sinkErrorCounter(),
              nodeId);
      flusher.initialize();
      return flusher;
    } catch (IOException e) {
      throw new RuntimeException("Failed to create temp directory", e);
    }
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
    throw new UnsupportedOperationException("Unsupported DLQ type: " + dlqConfig);
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_DATABRICKS_SINK;
  }

  @Override
  protected int batchSize() {
    return Integer.MAX_VALUE;
  }
}
