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

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/** Created by bolei on 9/24/24 */
@Slf4j
public class KafkaSourceCommandPartsFactory extends SourceCommandPartsFactory<SerializedEvent> {

  private final KafkaConsumerClientFactory kafkaConsumerClientFactory;

  public KafkaSourceCommandPartsFactory(
      MetricClientProvider metricClientProvider,
      KafkaConsumerClientFactory kafkaConsumerClientFactory) {
    super(metricClientProvider);
    this.kafkaConsumerClientFactory = kafkaConsumerClientFactory;
  }

  @Override
  public Fetcher<SerializedEvent> createFetcher(CommandConfig commandConfig) {
    KafkaSourceDto.Config config = (KafkaSourceDto.Config) commandConfig;

    Properties consumerProps = calculateConsumerProperties(config);
    KafkaConsumer<byte[], byte[]> consumer =
        kafkaConsumerClientFactory.createKafkaConsumer(consumerProps);
    initializeKafkaConsumer(consumer, config.getTopic());
    var monitoring = kafkaConsumerClientFactory.createAndStartHealthMonitor(consumerProps);

    CommitStrategy commitStrategy = createCommitStrategy(config);
    return new KafkaSourceFetcher(consumer, monitoring, commitStrategy);
  }

  @Override
  public RawDataConverter<SerializedEvent> createRawDataConverter(CommandConfig commandConfig) {
    KafkaSourceDto.Config config = (KafkaSourceDto.Config) commandConfig;
    FleakDeserializer<?> deserializer =
        DeserializerFactory.createDeserializerFactory(config.getEncodingType())
            .createDeserializer();

    return new BytesRawDataConverter(deserializer);
  }

  @Override
  public RawDataEncoder<SerializedEvent> createRawDataEncoder(CommandConfig commandConfig) {
    return new BytesRawDataEncoder();
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

  private Properties calculateConsumerProperties(KafkaSourceDto.Config config) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBroker());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupId());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000");
    props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1048576");
    props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "100");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
    props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "10485760");

    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // Add any additional properties from config
    if (config.getProperties() != null) {
      props.putAll(config.getProperties());
    }
    log.debug("Using consumer: {}", props.get(ConsumerConfig.GROUP_ID_CONFIG));
    return props;
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
}
