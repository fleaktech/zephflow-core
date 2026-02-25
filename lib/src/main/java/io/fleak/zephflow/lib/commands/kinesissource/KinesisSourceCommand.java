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
package io.fleak.zephflow.lib.commands.kinesissource;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.S3DlqWriter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.compression.DecompressorFactory;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

public class KinesisSourceCommand extends SimpleSourceCommand<SerializedEvent> {
  public KinesisSourceCommand(
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
    KinesisSourceDto.Config config = (KinesisSourceDto.Config) commandConfig;

    Fetcher<SerializedEvent> fetcher = createKinesisFetcher(config, jobContext);
    RawDataEncoder<SerializedEvent> encoder = new BytesRawDataEncoder();
    RawDataConverter<SerializedEvent> converter = createRawDataConverter(config);

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

  private Fetcher<SerializedEvent> createKinesisFetcher(
      KinesisSourceDto.Config config, JobContext jobContext) {
    AwsCredentialsProvider credentialsProvider = getAwsCredentialsProvider(config, jobContext);
    KinesisSourceFetcher fetcher = new KinesisSourceFetcher(config, credentialsProvider);
    fetcher.start();
    return fetcher;
  }

  private @Nullable AwsCredentialsProvider getAwsCredentialsProvider(
      KinesisSourceDto.Config config, JobContext jobContext) {
    AwsCredentialsProvider credentialsProvider = null;
    var credentialId = StringUtils.trimToNull(config.getCredentialId());
    if (credentialId != null) {
      var credentialsOpt =
          lookupUsernamePasswordCredentialOpt(jobContext, config.getCredentialId());
      if (credentialsOpt.isPresent()) {
        var credentials = credentialsOpt.get();
        credentialsProvider =
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(credentials.getUsername(), credentials.getPassword()));
      }
    }
    return credentialsProvider;
  }

  private RawDataConverter<SerializedEvent> createRawDataConverter(KinesisSourceDto.Config config) {
    return new BytesRawDataConverter(
        DeserializerFactory.createDeserializerFactory(config.getEncodingType())
            .createDeserializer(),
        DecompressorFactory.getDecompressor(config.getCompressionTypes()));
  }

  private DlqWriter createDlqWriter(JobContext.DlqConfig dlqConfig, String keyPrefix) {
    if (dlqConfig instanceof JobContext.S3DlqConfig s3DlqConfig) {
      return S3DlqWriter.createS3DlqWriter(s3DlqConfig, keyPrefix);
    }
    throw new UnsupportedOperationException("unsupported dlq type: " + dlqConfig);
  }

  @Override
  public SourceType sourceType() {
    return SourceType.STREAMING;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_KINESIS_SOURCE;
  }
}
