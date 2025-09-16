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

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.commands.sink.PassThroughMessagePreProcessor;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkCommandPartsFactory;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import java.util.Optional;
import java.util.concurrent.Executors;
import software.amazon.awssdk.services.s3.S3Client;

/** Created by bolei on 9/3/24 */
public class S3SinkPartsFactory extends SinkCommandPartsFactory<RecordFleakData> {
  private final S3SinkDto.Config config;
  private final AwsClientFactory awsClientFactory;

  protected S3SinkPartsFactory(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      S3SinkDto.Config config,
      AwsClientFactory awsClientFactory) {
    super(metricClientProvider, jobContext);
    this.config = config;
    this.awsClientFactory = awsClientFactory;
  }

  public SimpleSinkCommand.Flusher<RecordFleakData> createFlusher() {

    Optional<UsernamePasswordCredential> usernamePasswordCredentialOpt =
        lookupUsernamePasswordCredentialOpt(jobContext, config.getCredentialId());

    S3Client s3Client =
        awsClientFactory.createS3Client(
            config.getRegionStr(),
            usernamePasswordCredentialOpt.orElse(null),
            config.getS3EndpointOverride());

    EncodingType encodingType = parseEnum(EncodingType.class, config.getEncodingType());
    SerializerFactory<?> serializerFactory =
        SerializerFactory.createSerializerFactory(encodingType);
    FleakSerializer<?> serializer = serializerFactory.createSerializer();
    S3Commiter<RecordFleakData> commiter;
    if (config.isBatching()) {
      commiter =
          new BatchS3Commiter<>(
              s3Client,
              config.getBucketName(),
              config.getBatchSize(),
              config.getFlushIntervalMillis(),
              new S3CommiterSerializer.RecordFleakDataS3CommiterSerializer(serializer),
              Executors.newSingleThreadScheduledExecutor(
                  r -> {
                    Thread t = new Thread(r, "s3-sink-batch-flusher");
                    t.setDaemon(true);
                    return t;
                  }));
      ((BatchS3Commiter<RecordFleakData>) commiter).open();
    } else {
      commiter =
          new OnDemandS3Commiter(s3Client, config.getBucketName(), config.getKeyName(), serializer);
    }

    return new S3Flusher(commiter);
  }

  public SimpleSinkCommand.SinkMessagePreProcessor<RecordFleakData> createMessagePreProcessor() {
    return new PassThroughMessagePreProcessor();
  }
}
