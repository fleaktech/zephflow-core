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
package io.fleak.zephflow.lib.commands.clickhousesink;

import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_CLICK_HOUSE_SINK;
import static io.fleak.zephflow.lib.utils.MiscUtils.lookupUsernamePasswordCredentialOpt;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClickHouseSinkCommand extends SimpleSinkCommand<Map<String, Object>> {

  private static final int CLICKHOUSE_SINK_BATCH_SIZE = 100;

  protected ClickHouseSinkCommand(
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

    ClickHouseSinkDto.Config config = (ClickHouseSinkDto.Config) commandConfig;
    SimpleSinkCommand.Flusher<Map<String, Object>> flusher =
        createClickHouseFlusher(config, jobContext);
    SimpleSinkCommand.SinkMessagePreProcessor<Map<String, Object>> messagePreProcessor =
        new ClickHouseMessageProcessor();

    return new SinkExecutionContext<>(
        flusher,
        messagePreProcessor,
        counters.inputMessageCounter(),
        counters.errorCounter(),
        counters.sinkOutputCounter(),
        counters.outputSizeCounter(),
        counters.sinkErrorCounter());
  }

  private SimpleSinkCommand.Flusher<Map<String, Object>> createClickHouseFlusher(
      ClickHouseSinkDto.Config config, JobContext jobContext) {
    var writer =
        new ClickHouseWriter(
            config,
            lookupUsernamePasswordCredentialOpt(jobContext, config.getCredentialId()).orElse(null));
    try {
      writer.downloadAndSetSchema(config.getDatabase(), config.getTable());
    } catch (Exception e) {
      log.error("Error downloading schema", e);
      throw new IllegalArgumentException(e);
    }
    return writer;
  }

  @Override
  protected int batchSize() {
    return CLICKHOUSE_SINK_BATCH_SIZE;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_CLICK_HOUSE_SINK;
  }
}
