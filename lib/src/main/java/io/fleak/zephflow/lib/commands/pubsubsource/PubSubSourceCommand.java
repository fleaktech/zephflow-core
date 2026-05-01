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
package io.fleak.zephflow.lib.commands.pubsubsource;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.credentials.GcpCredential;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
import io.fleak.zephflow.lib.gcp.PubSubClientFactory;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

public class PubSubSourceCommand extends SimpleSourceCommand<PubSubReceivedMessage> {

  private final PubSubClientFactory pubSubClientFactory;

  public PubSubSourceCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      PubSubClientFactory pubSubClientFactory) {
    super(nodeId, jobContext, configParser, configValidator);
    this.pubSubClientFactory = pubSubClientFactory;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    PubSubSourceDto.Config config = (PubSubSourceDto.Config) commandConfig;

    Fetcher<PubSubReceivedMessage> fetcher = createFetcher(config, jobContext);
    RawDataEncoder<PubSubReceivedMessage> encoder = new PubSubRawDataEncoder();
    RawDataConverter<PubSubReceivedMessage> converter = createConverter(config);

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

  private Fetcher<PubSubReceivedMessage> createFetcher(
      PubSubSourceDto.Config config, JobContext jobContext) {
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
    String subscriptionPath = "projects/" + projectId + "/subscriptions/" + config.getSubscription();

    SubscriberStub stub =
        credentialOpt
            .map(pubSubClientFactory::createSubscriberStub)
            .orElseGet(pubSubClientFactory::createSubscriberStub);

    int maxMessages =
        config.getMaxMessages() != null
            ? config.getMaxMessages()
            : PubSubSourceDto.DEFAULT_MAX_MESSAGES;
    boolean returnImmediately =
        config.getReturnImmediately() != null && config.getReturnImmediately();
    int ackExtension =
        config.getAckDeadlineExtensionSeconds() != null
            ? config.getAckDeadlineExtensionSeconds()
            : 0;

    return new PubSubSourceFetcher(
        stub, subscriptionPath, maxMessages, returnImmediately, ackExtension);
  }

  private RawDataConverter<PubSubReceivedMessage> createConverter(PubSubSourceDto.Config config) {
    FleakDeserializer<?> deserializer =
        DeserializerFactory.createDeserializerFactory(config.getEncodingType())
            .createDeserializer();
    return new PubSubRawDataConverter(deserializer);
  }

  @Override
  public SourceType sourceType() {
    return SourceType.STREAMING;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_PUBSUB_SOURCE;
  }
}
