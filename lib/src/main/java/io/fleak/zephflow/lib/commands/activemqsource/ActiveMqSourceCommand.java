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
package io.fleak.zephflow.lib.commands.activemqsource;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import jakarta.jms.*;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class ActiveMqSourceCommand extends SimpleSourceCommand<SerializedEvent> {

  private final JmsConnectionFactoryProvider jmsConnectionFactoryProvider;

  public ActiveMqSourceCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      JmsConnectionFactoryProvider jmsConnectionFactoryProvider) {
    super(nodeId, jobContext, configParser, configValidator);
    this.jmsConnectionFactoryProvider = jmsConnectionFactoryProvider;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    ActiveMqSourceDto.Config config = (ActiveMqSourceDto.Config) commandConfig;

    Fetcher<SerializedEvent> fetcher = createJmsFetcher(config, jobContext);
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

  private Fetcher<SerializedEvent> createJmsFetcher(
      ActiveMqSourceDto.Config config, JobContext jobContext) {
    Connection connection = null;
    Session session = null;
    MessageConsumer consumer = null;
    try {
      ConnectionFactory cf = jmsConnectionFactoryProvider.createConnectionFactory(config);
      if (StringUtils.isNotBlank(config.getCredentialId())) {
        UsernamePasswordCredential credential =
            lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
        connection = cf.createConnection(credential.getUsername(), credential.getPassword());
      } else {
        connection = cf.createConnection();
      }

      if (StringUtils.isNotBlank(config.getClientId())) {
        connection.setClientID(config.getClientId());
      }
      connection.start();

      session = connection.createSession(true, Session.SESSION_TRANSACTED);

      if (config.getDestinationType() == ActiveMqSourceDto.DestinationType.TOPIC) {
        Topic topic = session.createTopic(config.getDestination());
        consumer = session.createDurableSubscriber(topic, config.getSubscriptionName());
      } else {
        Queue queue = session.createQueue(config.getDestination());
        consumer = session.createConsumer(queue);
      }

      CommitStrategy commitStrategy = createCommitStrategy(config);

      return new ActiveMqSourceFetcher(
          connection,
          session,
          consumer,
          config.getReceiveTimeoutMs(),
          config.getMaxBatchSize(),
          commitStrategy);
    } catch (Exception e) {
      ActiveMqSourceFetcher.closeQuietly(consumer);
      ActiveMqSourceFetcher.closeQuietly(session);
      ActiveMqSourceFetcher.closeQuietly(connection);
      throw new RuntimeException("Failed to create JMS fetcher", e);
    }
  }

  private CommitStrategy createCommitStrategy(ActiveMqSourceDto.Config config) {
    return switch (config.getCommitStrategy()) {
      case PER_RECORD -> PerRecordCommitStrategy.INSTANCE;
      case BATCH ->
          new BatchCommitStrategy(
              config.getCommitBatchSize() != null
                  ? config.getCommitBatchSize()
                  : ActiveMqSourceDto.DEFAULT_COMMIT_BATCH_SIZE,
              config.getCommitIntervalMs() != null
                  ? config.getCommitIntervalMs()
                  : ActiveMqSourceDto.DEFAULT_COMMIT_INTERVAL_MS);
      case NONE -> NoCommitStrategy.INSTANCE;
    };
  }

  private RawDataConverter<SerializedEvent> createRawDataConverter(
      ActiveMqSourceDto.Config config) {
    FleakDeserializer<?> deserializer =
        DeserializerFactory.createDeserializerFactory(config.getEncodingType())
            .createDeserializer();
    return new BytesRawDataConverter(deserializer);
  }

  @Override
  public SourceType sourceType() {
    return SourceType.STREAMING;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_ACTIVEMQ_SOURCE;
  }
}
