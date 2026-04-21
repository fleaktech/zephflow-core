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
package io.fleak.zephflow.lib.commands.azuremonitorsource;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.azure.EntraIdTokenProvider;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.commands.splunksource.MapRawDataConverter;
import io.fleak.zephflow.lib.commands.splunksource.MapRawDataEncoder;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

public class AzureMonitorSourceCommand extends SimpleSourceCommand<Map<String, String>> {

  public AzureMonitorSourceCommand(
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
    AzureMonitorSourceDto.Config config = (AzureMonitorSourceDto.Config) commandConfig;

    UsernamePasswordCredential credential =
        lookupUsernamePasswordCredential(jobContext, config.getCredentialId());

    HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();

    EntraIdTokenProvider tokenProvider =
        new EntraIdTokenProvider(
            config.getTenantId(),
            credential.getUsername(),
            credential.getPassword(),
            "https://api.loganalytics.io/.default",
            httpClient);

    AzureMonitorQueryClient queryClient =
        new AzureMonitorQueryClient(config.getWorkspaceId(), tokenProvider, httpClient);

    Fetcher<Map<String, String>> fetcher =
        new AzureMonitorSourceFetcher(config.getKqlQuery(), config.getBatchSize(), queryClient);

    RawDataEncoder<Map<String, String>> encoder = new MapRawDataEncoder();
    RawDataConverter<Map<String, String>> converter = new MapRawDataConverter();

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

  @Override
  public SourceType sourceType() {
    return SourceType.BATCH;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_AZURE_MONITOR_SOURCE;
  }
}
