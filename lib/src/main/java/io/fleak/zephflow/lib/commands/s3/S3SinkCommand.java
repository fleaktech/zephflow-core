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
package io.fleak.zephflow.lib.commands.s3;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.commands.sink.BlobFileWriter;
import io.fleak.zephflow.lib.commands.sink.ParquetBlobFileWriter;
import io.fleak.zephflow.lib.commands.sink.PassThroughMessagePreProcessor;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.commands.sink.TextBlobFileWriter;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.S3DlqWriter;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.s3.S3Client;

public class S3SinkCommand extends SimpleSinkCommand<RecordFleakData> {
  private static final int S3_SINK_BATCH_SIZE = 1_000_000_000;

  private final AwsClientFactory awsClientFactory;

  protected S3SinkCommand(
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
    SinkCounters counters =
        createSinkCounters(metricClientProvider, jobContext, commandName(), nodeId);

    S3SinkDto.Config config = (S3SinkDto.Config) commandConfig;
    SimpleSinkCommand.Flusher<RecordFleakData> flusher = createS3Flusher(config, jobContext);
    SimpleSinkCommand.SinkMessagePreProcessor<RecordFleakData> messagePreProcessor =
        new PassThroughMessagePreProcessor();

    return new SinkExecutionContext<>(
        flusher,
        messagePreProcessor,
        counters.inputMessageCounter(),
        counters.errorCounter(),
        counters.sinkOutputCounter(),
        counters.outputSizeCounter(),
        counters.sinkErrorCounter());
  }

  private SimpleSinkCommand.Flusher<RecordFleakData> createS3Flusher(
      S3SinkDto.Config config, JobContext jobContext) {
    Optional<UsernamePasswordCredential> usernamePasswordCredentialOpt =
        lookupUsernamePasswordCredentialOpt(jobContext, config.getCredentialId());

    EncodingType encodingType = parseEnum(EncodingType.class, config.getEncodingType());

    if (config.isBatching()) {
      AwsClientFactory.S3TransferResources s3TransferResources =
          awsClientFactory.createS3TransferResources(
              config.getRegionStr(),
              usernamePasswordCredentialOpt.orElse(null),
              config.getS3EndpointOverride());

      BlobFileWriter<RecordFleakData> fileWriter = createFileWriter(encodingType, config);
      String keyPrefix = (String) jobContext.getOtherProperties().get(JobContext.DATA_KEY_PREFIX);
      DlqWriter dlqWriter = null;
      if (jobContext.getDlqConfig() instanceof JobContext.S3DlqConfig s3DlqConfig) {
        dlqWriter = S3DlqWriter.createS3DlqWriter(s3DlqConfig, keyPrefix);
      }
      BatchS3Flusher flusher =
          new BatchS3Flusher(
              s3TransferResources,
              config.getBucketName(),
              StringUtils.stripEnd(config.getKeyName(), "/"),
              fileWriter,
              config.getBatchSize(),
              config.getFlushIntervalMillis(),
              dlqWriter,
              jobContext,
              nodeId);
      flusher.initialize();
      return flusher;
    } else {
      S3Client s3Client =
          awsClientFactory.createS3Client(
              config.getRegionStr(),
              usernamePasswordCredentialOpt.orElse(null),
              config.getS3EndpointOverride());
      SerializerFactory<?> serializerFactory =
          SerializerFactory.createSerializerFactory(encodingType);
      FleakSerializer<?> serializer = serializerFactory.createSerializer();
      S3Commiter<RecordFleakData> commiter =
          new OnDemandS3Commiter(
              s3Client,
              config.getBucketName(),
              StringUtils.stripEnd(config.getKeyName(), "/"),
              serializer);
      return new S3Flusher(commiter);
    }
  }

  private BlobFileWriter<RecordFleakData> createFileWriter(
      EncodingType encodingType, S3SinkDto.Config config) {
    if (encodingType == EncodingType.PARQUET) {
      if (config.getAvroSchema() == null || config.getAvroSchema().isEmpty()) {
        throw new IllegalArgumentException("avroSchema is required for PARQUET encoding type");
      }
      return new ParquetBlobFileWriter(config.getAvroSchema());
    } else {
      SerializerFactory<?> serializerFactory =
          SerializerFactory.createSerializerFactory(encodingType);
      FleakSerializer<?> serializer = serializerFactory.createSerializer();
      return new TextBlobFileWriter(serializer, encodingType);
    }
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_S3_SINK;
  }

  @Override
  protected int batchSize() {
    return S3_SINK_BATCH_SIZE;
  }
}
