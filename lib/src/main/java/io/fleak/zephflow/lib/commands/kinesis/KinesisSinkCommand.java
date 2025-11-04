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
package io.fleak.zephflow.lib.commands.kinesis;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.commands.JsonConfigParser;
import io.fleak.zephflow.lib.commands.sink.PassThroughMessagePreProcessor;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.kinesis.KinesisClient;

public class KinesisSinkCommand extends SimpleSinkCommand<RecordFleakData> {
  private static final int KINESIS_SINK_BATCH_SIZE = 100;

  private final AwsClientFactory awsClientFactory;

  protected KinesisSinkCommand(
      String nodeId,
      JobContext jobContext,
      JsonConfigParser<KinesisSinkDto.Config> configParser,
      KinesisSinkConfigValidator configValidator,
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

    KinesisSinkDto.Config config = (KinesisSinkDto.Config) commandConfig;
    SimpleSinkCommand.Flusher<RecordFleakData> flusher = createKinesisFlusher(config, jobContext);
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

  private SimpleSinkCommand.Flusher<RecordFleakData> createKinesisFlusher(
      KinesisSinkDto.Config config, JobContext jobContext) {
    Optional<UsernamePasswordCredential> usernamePasswordCredentialOpt =
        lookupUsernamePasswordCredentialOpt(jobContext, config.getCredentialId());

    KinesisClient kinesisClient =
        awsClientFactory.createKinesisClient(
            config.getRegionStr(), usernamePasswordCredentialOpt.orElse(null));

    PathExpression partitionKeyPathExpression = null;
    if (StringUtils.trimToNull(config.getPartitionKeyFieldExpressionStr()) != null) {
      partitionKeyPathExpression =
          PathExpression.fromString(config.getPartitionKeyFieldExpressionStr());
    }

    EncodingType encodingType = parseEnum(EncodingType.class, config.getEncodingType());
    SerializerFactory<?> serializerFactory =
        SerializerFactory.createSerializerFactory(encodingType);
    FleakSerializer<?> serializer = serializerFactory.createSerializer();

    return new KinesisFlusher(
        kinesisClient, config.getStreamName(), partitionKeyPathExpression, serializer);
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_KINESIS_SINK;
  }

  @Override
  protected int batchSize() {
    return KINESIS_SINK_BATCH_SIZE;
  }
}
