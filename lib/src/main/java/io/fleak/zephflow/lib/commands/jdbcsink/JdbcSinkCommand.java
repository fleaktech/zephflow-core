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
package io.fleak.zephflow.lib.commands.jdbcsink;

import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_JDBC_SINK;
import static io.fleak.zephflow.lib.utils.MiscUtils.lookupUsernamePasswordCredentialOpt;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class JdbcSinkCommand extends SimpleSinkCommand<Map<String, Object>> {

  private JdbcSinkDto.Config sinkConfig;

  protected JdbcSinkCommand(
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

    sinkConfig = (JdbcSinkDto.Config) commandConfig;

    if (StringUtils.isNotBlank(sinkConfig.getDriverClassName())) {
      try {
        Class.forName(sinkConfig.getDriverClassName());
      } catch (ClassNotFoundException e) {
        throw new RuntimeException(
            "Failed to load JDBC driver: " + sinkConfig.getDriverClassName(), e);
      }
    }

    SimpleSinkCommand.Flusher<Map<String, Object>> flusher =
        createJdbcFlusher(sinkConfig, jobContext);
    SimpleSinkCommand.SinkMessagePreProcessor<Map<String, Object>> messagePreProcessor =
        new JdbcSinkMessageProcessor();

    return new SinkExecutionContext<>(
        flusher,
        messagePreProcessor,
        counters.inputMessageCounter(),
        counters.errorCounter(),
        counters.sinkOutputCounter(),
        counters.outputSizeCounter(),
        counters.sinkErrorCounter());
  }

  private SimpleSinkCommand.Flusher<Map<String, Object>> createJdbcFlusher(
      JdbcSinkDto.Config config, JobContext jobContext) {
    String username = null;
    String password = null;
    var credentialOpt = lookupUsernamePasswordCredentialOpt(jobContext, config.getCredentialId());
    if (credentialOpt.isPresent()) {
      username = credentialOpt.get().getUsername();
      password = credentialOpt.get().getPassword();
    }

    List<String> upsertKeyColumns = List.of();
    if (StringUtils.isNotBlank(config.getUpsertKeyColumns())) {
      upsertKeyColumns =
          Arrays.stream(config.getUpsertKeyColumns().split(","))
              .map(String::trim)
              .filter(s -> !s.isEmpty())
              .toList();
    }

    return new JdbcSinkFlusher(
        config.getJdbcUrl(),
        username,
        password,
        config.getTableName(),
        config.getSchemaName(),
        config.getWriteMode(),
        upsertKeyColumns);
  }

  @Override
  protected int batchSize() {
    if (sinkConfig != null) {
      return sinkConfig.getBatchSize();
    }
    return JdbcSinkDto.DEFAULT_BATCH_SIZE;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_JDBC_SINK;
  }
}
