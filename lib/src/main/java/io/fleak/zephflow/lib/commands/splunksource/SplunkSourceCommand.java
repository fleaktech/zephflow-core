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
package io.fleak.zephflow.lib.commands.splunksource;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import com.splunk.HttpService;
import com.splunk.Service;
import com.splunk.ServiceArgs;
import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.S3DlqWriter;
import java.net.URL;
import java.util.Map;
import java.util.Optional;

public class SplunkSourceCommand extends SimpleSourceCommand<Map<String, String>> {

  public SplunkSourceCommand(
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
    SplunkSourceDto.Config config = (SplunkSourceDto.Config) commandConfig;

    Service service = createSplunkService(config, jobContext);
    Fetcher<Map<String, String>> fetcher = new SplunkSourceFetcher(config, service);
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

    String keyPrefix = (String) jobContext.getOtherProperties().get("DATA_KEY_PREFIX");
    DlqWriter dlqWriter =
        Optional.of(jobContext)
            .map(JobContext::getDlqConfig)
            .map(c -> createDlqWriter(c, keyPrefix))
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

  private Service createSplunkService(SplunkSourceDto.Config config, JobContext jobContext) {
    try {
      UsernamePasswordCredential credential =
          lookupUsernamePasswordCredential(jobContext, config.getCredentialId());

      if (!config.isValidateCertificates()) {
        HttpService.setValidateCertificates(false);
      }

      URL url = new URL(config.getSplunkUrl());
      ServiceArgs loginArgs = new ServiceArgs();
      loginArgs.setHost(url.getHost());
      loginArgs.setPort(url.getPort());
      loginArgs.setScheme(url.getProtocol());
      loginArgs.setUsername(credential.getUsername());
      loginArgs.setPassword(credential.getPassword());

      return Service.connect(loginArgs);
    } catch (Exception e) {
      throw new RuntimeException("Failed to connect to Splunk", e);
    }
  }

  private DlqWriter createDlqWriter(JobContext.DlqConfig dlqConfig, String keyPrefix) {
    if (dlqConfig instanceof JobContext.S3DlqConfig s3DlqConfig) {
      return S3DlqWriter.createS3DlqWriter(s3DlqConfig, keyPrefix);
    }
    throw new UnsupportedOperationException("unsupported dlq type: " + dlqConfig);
  }

  @Override
  public SourceType sourceType() {
    return SourceType.BATCH;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_SPLUNK_SOURCE;
  }
}
