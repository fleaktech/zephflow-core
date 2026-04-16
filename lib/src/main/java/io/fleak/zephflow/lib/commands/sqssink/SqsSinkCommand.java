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
package io.fleak.zephflow.lib.commands.sqssink;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.aws.AwsClientFactory;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.services.sqs.SqsClient;

public class SqsSinkCommand extends SimpleSinkCommand<SqsOutboundMessage> {

  private final AwsClientFactory awsClientFactory;

  protected SqsSinkCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      AwsClientFactory awsClientFactory) {
    super(nodeId, jobContext, configParser, configValidator);
    this.awsClientFactory = awsClientFactory;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_SQS_SINK;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    SinkCounters counters =
        createSinkCounters(metricClientProvider, jobContext, commandName(), nodeId);

    SqsSinkDto.Config config = (SqsSinkDto.Config) commandConfig;
    SimpleSinkCommand.Flusher<SqsOutboundMessage> flusher = createSqsFlusher(config, jobContext);
    SimpleSinkCommand.SinkMessagePreProcessor<SqsOutboundMessage> messagePreProcessor =
        createMessagePreProcessor(config);

    return new SinkExecutionContext<>(
        flusher,
        messagePreProcessor,
        counters.inputMessageCounter(),
        counters.errorCounter(),
        counters.sinkOutputCounter(),
        counters.outputSizeCounter(),
        counters.sinkErrorCounter());
  }

  private SimpleSinkCommand.Flusher<SqsOutboundMessage> createSqsFlusher(
      SqsSinkDto.Config config, JobContext jobContext) {
    Optional<UsernamePasswordCredential> credentialOpt =
        lookupUsernamePasswordCredentialOpt(jobContext, config.getCredentialId());

    SqsClient sqsClient =
        awsClientFactory.createSqsClient(config.getRegionStr(), credentialOpt.orElse(null));

    return new SqsSinkFlusher(sqsClient, config.getQueueUrl());
  }

  private SimpleSinkCommand.SinkMessagePreProcessor<SqsOutboundMessage> createMessagePreProcessor(
      SqsSinkDto.Config config) {
    PathExpression messageGroupIdExpression =
        StringUtils.isNotBlank(config.getMessageGroupIdExpression())
            ? PathExpression.fromString(config.getMessageGroupIdExpression())
            : null;
    PathExpression deduplicationIdExpression =
        StringUtils.isNotBlank(config.getDeduplicationIdExpression())
            ? PathExpression.fromString(config.getDeduplicationIdExpression())
            : null;

    return new SqsSinkMessageProcessor(messageGroupIdExpression, deduplicationIdExpression);
  }

  @Override
  protected int batchSize() {
    return SqsSinkDto.MAX_BATCH_SIZE;
  }
}
