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
package io.fleak.zephflow.lib.commands.kafkasource;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
import io.fleak.zephflow.lib.kafka.KafkaClientProperties;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

/** Created by bolei on 9/23/24 */
@Slf4j
public class KafkaSourceCommand extends SimpleSourceCommand<SerializedEvent> {

  private final KafkaConsumerClientFactory kafkaConsumerClientFactory;

  public KafkaSourceCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      KafkaConsumerClientFactory kafkaConsumerClientFactory) {
    super(nodeId, jobContext, configParser, configValidator);
    this.kafkaConsumerClientFactory = kafkaConsumerClientFactory;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    KafkaSourceDto.Config config = (KafkaSourceDto.Config) commandConfig;

    Fetcher<SerializedEvent> fetcher = createKafkaFetcher(config);
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

  private Fetcher<SerializedEvent> createKafkaFetcher(KafkaSourceDto.Config config) {
    Properties consumerProps = KafkaClientProperties.source(config);
    log.debug("Using consumer: {}", consumerProps.get(ConsumerConfig.GROUP_ID_CONFIG));
    KafkaConsumer<byte[], byte[]> consumer =
        kafkaConsumerClientFactory.createKafkaConsumer(consumerProps);
    initializeKafkaConsumer(consumer, config.getTopic());
    var monitoring = kafkaConsumerClientFactory.createAndStartHealthMonitor(consumerProps);

    CommitStrategy commitStrategy = createCommitStrategy(config);
    return new KafkaSourceFetcher(consumer, monitoring, commitStrategy);
  }

  private RawDataConverter<SerializedEvent> createRawDataConverter(KafkaSourceDto.Config config) {
    FleakDeserializer<?> deserializer =
        DeserializerFactory.createDeserializerFactory(config.getEncodingType())
            .createDeserializer();
    return new BytesRawDataConverter(deserializer);
  }

  private static void initializeKafkaConsumer(
      KafkaConsumer<byte[], byte[]> consumer, String topic) {
    consumer.subscribe(
        Collections.singletonList(topic),
        new ConsumerRebalanceListener() {
          @Override
          public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            log.info(
                "Partitions revoked: {}",
                partitions.stream().map(TopicPartition::toString).collect(Collectors.joining(",")));
          }

          @Override
          public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            log.info(
                "Partitions assigned: {}",
                partitions.stream().map(TopicPartition::toString).collect(Collectors.joining(",")));
          }
        });
  }

  private CommitStrategy createCommitStrategy(KafkaSourceDto.Config config) {
    return switch (config.getCommitStrategy()) {
      case PER_RECORD -> PerRecordCommitStrategy.INSTANCE;
      case BATCH ->
          new BatchCommitStrategy(
              config.getCommitBatchSize() != null ? config.getCommitBatchSize() : 1000,
              config.getCommitIntervalMs() != null ? config.getCommitIntervalMs() : 5000L);
      case NONE -> NoCommitStrategy.INSTANCE;
    };
  }

  @Override
  public SourceType sourceType() {
    return SourceType.STREAMING;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_KAFKA_SOURCE;
  }
}
