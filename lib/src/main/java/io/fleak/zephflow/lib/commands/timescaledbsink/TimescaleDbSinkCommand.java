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
package io.fleak.zephflow.lib.commands.timescaledbsink;

import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_TIMESCALE_DB_SINK;
import static io.fleak.zephflow.lib.utils.MiscUtils.lookupUsernamePasswordCredential;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.jdbcsink.JdbcSinkFlusher;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class TimescaleDbSinkCommand extends SimpleSinkCommand<Map<String, Object>> {

  private final TimescaleHypertableInitializer hypertableInitializer;

  protected TimescaleDbSinkCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      TimescaleHypertableInitializer hypertableInitializer) {
    super(nodeId, jobContext, configParser, configValidator);
    this.hypertableInitializer = hypertableInitializer;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_TIMESCALE_DB_SINK;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    SinkCounters counters =
        createSinkCounters(metricClientProvider, jobContext, commandName(), nodeId);

    TimescaleDbSinkDto.Config config = (TimescaleDbSinkDto.Config) commandConfig;

    String username = null;
    String password = null;
    if (StringUtils.isNotBlank(config.getCredentialId())) {
      UsernamePasswordCredential credential =
          lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
      username = credential.getUsername();
      password = credential.getPassword();
    }

    if (config.isCreateHypertable()) {
      hypertableInitializer.ensureHypertable(
          config.getJdbcUrl(),
          username,
          password,
          qualifiedTableName(config),
          config.getTimeColumn());
    }

    SimpleSinkCommand.Flusher<Map<String, Object>> flusher =
        new JdbcSinkFlusher(
            config.getJdbcUrl(),
            username,
            password,
            config.getTableName(),
            config.getSchemaName(),
            config.getWriteMode(),
            upsertKeyColumns(config));
    SimpleSinkCommand.SinkMessagePreProcessor<Map<String, Object>> messagePreProcessor =
        new TimescaleDbSinkMessageProcessor(config.getTimeColumn(), config.getTimeUnit());

    return new SinkExecutionContext<>(
        flusher,
        messagePreProcessor,
        counters.inputMessageCounter(),
        counters.errorCounter(),
        counters.sinkOutputCounter(),
        counters.outputSizeCounter(),
        counters.sinkErrorCounter());
  }

  @Override
  protected int batchSize() {
    return ((TimescaleDbSinkDto.Config) commandConfig).getBatchSize();
  }

  private static String qualifiedTableName(TimescaleDbSinkDto.Config config) {
    String table = quoteIdentifier(config.getTableName());
    if (StringUtils.isBlank(config.getSchemaName())) {
      return table;
    }
    return quoteIdentifier(config.getSchemaName()) + "." + table;
  }

  private static String quoteIdentifier(String identifier) {
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }

  private static List<String> upsertKeyColumns(TimescaleDbSinkDto.Config config) {
    if (StringUtils.isBlank(config.getUpsertKeyColumns())) {
      return List.of();
    }
    return Arrays.stream(config.getUpsertKeyColumns().split(","))
        .map(String::trim)
        .filter(StringUtils::isNotBlank)
        .toList();
  }
}
