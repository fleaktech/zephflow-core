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
package io.fleak.zephflow.lib.commands.pubsubsink;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import com.google.cloud.pubsub.v1.stub.PublisherStub;
import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.credentials.GcpCredential;
import io.fleak.zephflow.lib.gcp.PubSubClientFactory;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

public class PubSubSinkCommand extends SimpleSinkCommand<PubSubOutboundMessage> {

  private final PubSubClientFactory pubSubClientFactory;

  protected PubSubSinkCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      PubSubClientFactory pubSubClientFactory) {
    super(nodeId, jobContext, configParser, configValidator);
    this.pubSubClientFactory = pubSubClientFactory;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_PUBSUB_SINK;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    SinkCounters counters =
        createSinkCounters(metricClientProvider, jobContext, commandName(), nodeId);

    PubSubSinkDto.Config config = (PubSubSinkDto.Config) commandConfig;
    SimpleSinkCommand.Flusher<PubSubOutboundMessage> flusher = createFlusher(config, jobContext);
    SimpleSinkCommand.SinkMessagePreProcessor<PubSubOutboundMessage> preProcessor =
        createPreProcessor(config);

    return new SinkExecutionContext<>(
        flusher,
        preProcessor,
        counters.inputMessageCounter(),
        counters.errorCounter(),
        counters.sinkOutputCounter(),
        counters.outputSizeCounter(),
        counters.sinkErrorCounter());
  }

  private SimpleSinkCommand.Flusher<PubSubOutboundMessage> createFlusher(
      PubSubSinkDto.Config config, JobContext jobContext) {
    Optional<GcpCredential> credentialOpt = Optional.empty();
    if (StringUtils.isNotBlank(config.getCredentialId())) {
      credentialOpt = lookupGcpCredentialOpt(jobContext, config.getCredentialId());
    }

    String projectId =
        StringUtils.firstNonBlank(
            config.getProjectId(), credentialOpt.map(GcpCredential::getProjectId).orElse(null));
    if (StringUtils.isBlank(projectId)) {
      throw new IllegalArgumentException(
          "projectId required: set Config.projectId or GcpCredential.projectId");
    }
    String topicPath = "projects/" + projectId + "/topics/" + config.getTopic();

    PublisherStub stub =
        credentialOpt
            .map(pubSubClientFactory::createPublisherStub)
            .orElseGet(pubSubClientFactory::createPublisherStub);

    return new PubSubSinkFlusher(stub, topicPath);
  }

  private SimpleSinkCommand.SinkMessagePreProcessor<PubSubOutboundMessage> createPreProcessor(
      PubSubSinkDto.Config config) {
    PathExpression orderingKeyExpression =
        StringUtils.isNotBlank(config.getOrderingKeyExpression())
            ? PathExpression.fromString(config.getOrderingKeyExpression())
            : null;
    return new PubSubSinkMessageProcessor(orderingKeyExpression);
  }

  @Override
  protected int batchSize() {
    PubSubSinkDto.Config config = (PubSubSinkDto.Config) commandConfig;
    return config.getBatchSize() != null ? config.getBatchSize() : PubSubSinkDto.DEFAULT_BATCH_SIZE;
  }
}
