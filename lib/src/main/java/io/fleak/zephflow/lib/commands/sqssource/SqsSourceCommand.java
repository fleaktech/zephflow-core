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
package io.fleak.zephflow.lib.commands.sqssource;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.sqs.SqsClient;

public class SqsSourceCommand extends SimpleSourceCommand<SqsReceivedMessage> {

  private final AwsClientFactory awsClientFactory;

  public SqsSourceCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      AwsClientFactory awsClientFactory) {
    super(nodeId, jobContext, configParser, configValidator);
    this.awsClientFactory = awsClientFactory;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    SqsSourceDto.Config config = (SqsSourceDto.Config) commandConfig;

    Fetcher<SqsReceivedMessage> fetcher = createSqsFetcher(config, jobContext);
    RawDataEncoder<SqsReceivedMessage> encoder = new SqsRawDataEncoder();
    RawDataConverter<SqsReceivedMessage> converter = createRawDataConverter(config);

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

  private Fetcher<SqsReceivedMessage> createSqsFetcher(
      SqsSourceDto.Config config, JobContext jobContext) {
    UsernamePasswordCredential credential = null;
    if (StringUtils.trimToNull(config.getCredentialId()) != null) {
      Optional<UsernamePasswordCredential> credentialOpt =
          lookupUsernamePasswordCredentialOpt(jobContext, config.getCredentialId());
      credential = credentialOpt.orElse(null);
    }

    SqsClient sqsClient = awsClientFactory.createSqsClient(config.getRegionStr(), credential);

    int maxMessages =
        config.getMaxNumberOfMessages() != null
            ? config.getMaxNumberOfMessages()
            : SqsSourceDto.DEFAULT_MAX_NUMBER_OF_MESSAGES;
    int waitTime =
        config.getWaitTimeSeconds() != null
            ? config.getWaitTimeSeconds()
            : SqsSourceDto.DEFAULT_WAIT_TIME_SECONDS;
    int visibilityTimeout =
        config.getVisibilityTimeoutSeconds() != null
            ? config.getVisibilityTimeoutSeconds()
            : SqsSourceDto.DEFAULT_VISIBILITY_TIMEOUT_SECONDS;

    return new SqsSourceFetcher(
        sqsClient, config.getQueueUrl(), maxMessages, waitTime, visibilityTimeout);
  }

  private RawDataConverter<SqsReceivedMessage> createRawDataConverter(SqsSourceDto.Config config) {
    FleakDeserializer<?> deserializer =
        DeserializerFactory.createDeserializerFactory(config.getEncodingType())
            .createDeserializer();
    return new SqsRawDataConverter(deserializer);
  }

  @Override
  public SourceType sourceType() {
    return SourceType.STREAMING;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_SQS_SOURCE;
  }
}
