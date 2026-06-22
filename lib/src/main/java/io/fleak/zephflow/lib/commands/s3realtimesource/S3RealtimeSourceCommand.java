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
package io.fleak.zephflow.lib.commands.s3realtimesource;

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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

public class S3RealtimeSourceCommand extends SimpleSourceCommand<S3EventMessage> {

  private final AwsClientFactory awsClientFactory;

  public S3RealtimeSourceCommand(
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
    S3RealtimeSourceDto.Config config = (S3RealtimeSourceDto.Config) commandConfig;

    UsernamePasswordCredential sqsCredential =
        resolveCredential(jobContext, config.getCredentialId());
    String s3CredentialId =
        StringUtils.trimToNull(config.getS3CredentialId()) != null
            ? config.getS3CredentialId()
            : config.getCredentialId();
    UsernamePasswordCredential s3Credential = resolveCredential(jobContext, s3CredentialId);

    SqsClient sqsClient = awsClientFactory.createSqsClient(config.getRegionStr(), sqsCredential);
    String s3Region =
        StringUtils.trimToNull(config.getS3RegionStr()) != null
            ? config.getS3RegionStr()
            : config.getRegionStr();
    S3Client s3Client =
        awsClientFactory.createS3Client(s3Region, s3Credential, config.getS3EndpointOverride());

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

    Queue<String> confirmedReceiptHandles = new ConcurrentLinkedQueue<>();

    FleakDeserializer<?> deserializer =
        DeserializerFactory.createDeserializerFactory(config.getEncodingType())
            .createDeserializer();
    RawDataConverter<S3EventMessage> converter =
        new S3RealtimeRawDataConverter(
            s3Client,
            deserializer,
            config.getCompressionType(),
            orDefault(
                config.getMaxObjectSizeBytes(), S3RealtimeSourceDto.DEFAULT_MAX_OBJECT_SIZE_BYTES),
            config.isAddS3Metadata(),
            confirmedReceiptHandles);
    RawDataEncoder<S3EventMessage> encoder = new S3RealtimeRawDataEncoder();

    Fetcher<S3EventMessage> fetcher =
        new S3RealtimeSourceFetcher(
            sqsClient,
            s3Client,
            config.getQueueUrl(),
            orDefault(
                config.getMaxNumberOfMessages(),
                S3RealtimeSourceDto.DEFAULT_MAX_NUMBER_OF_MESSAGES),
            orDefault(config.getWaitTimeSeconds(), S3RealtimeSourceDto.DEFAULT_WAIT_TIME_SECONDS),
            orDefault(
                config.getVisibilityTimeoutSeconds(),
                S3RealtimeSourceDto.DEFAULT_VISIBILITY_TIMEOUT_SECONDS),
            orDefault(config.getMaxRetries(), S3RealtimeSourceDto.DEFAULT_MAX_RETRIES),
            dlqWriter,
            nodeId,
            confirmedReceiptHandles);

    return new SourceExecutionContext<>(
        fetcher,
        converter,
        encoder,
        dataSizeCounter,
        inputEventCounter,
        deserializeFailureCounter,
        dlqWriter);
  }

  private UsernamePasswordCredential resolveCredential(JobContext jobContext, String credentialId) {
    if (StringUtils.trimToNull(credentialId) == null) {
      return null;
    }
    return lookupUsernamePasswordCredentialOpt(jobContext, credentialId).orElse(null);
  }

  private static int orDefault(Integer value, int defaultValue) {
    return value != null ? value : defaultValue;
  }

  private static long orDefault(Long value, long defaultValue) {
    return value != null ? value : defaultValue;
  }

  @Override
  public SourceType sourceType() {
    return SourceType.STREAMING;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_S3_REALTIME_SOURCE;
  }
}
