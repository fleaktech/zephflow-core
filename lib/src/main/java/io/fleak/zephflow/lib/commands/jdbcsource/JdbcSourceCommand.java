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
package io.fleak.zephflow.lib.commands.jdbcsource;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class JdbcSourceCommand extends SimpleSourceCommand<Map<String, Object>> {

  public JdbcSourceCommand(
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
    JdbcSourceDto.Config config = (JdbcSourceDto.Config) commandConfig;

    Fetcher<Map<String, Object>> fetcher = createJdbcFetcher(config, jobContext);
    RawDataEncoder<Map<String, Object>> encoder = new JdbcSourceRawDataEncoder();
    RawDataConverter<Map<String, Object>> converter = new JdbcSourceRawDataConverter();

    Map<String, String> metricTags =
        basicCommandMetricTags(jobContext.getMetricTags(), commandName(), nodeId);
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

  private Fetcher<Map<String, Object>> createJdbcFetcher(
      JdbcSourceDto.Config config, JobContext jobContext) {
    if (StringUtils.isNotBlank(config.getDriverClassName())) {
      try {
        Class.forName(config.getDriverClassName());
      } catch (ClassNotFoundException e) {
        throw new RuntimeException("Failed to load JDBC driver: " + config.getDriverClassName(), e);
      }
    }

    String username = null;
    String password = null;
    if (StringUtils.isNotBlank(config.getCredentialId())) {
      UsernamePasswordCredential credential =
          lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
      username = credential.getUsername();
      password = credential.getPassword();
    }

    return new JdbcSourceFetcher(
        config.getJdbcUrl(),
        username,
        password,
        config.getQuery(),
        config.getWatermarkColumn(),
        config.getFetchSize());
  }

  @Override
  public SourceType sourceType() {
    if (commandConfig instanceof JdbcSourceDto.Config config
        && StringUtils.isNotBlank(config.getWatermarkColumn())) {
      return SourceType.STREAMING;
    }
    return SourceType.BATCH;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_JDBC_SOURCE;
  }
}
