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
package io.fleak.zephflow.lib.commands.kafkasink;

import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_KAFKA_SINK;
import static io.fleak.zephflow.lib.utils.MiscUtils.basicCommandMetricTags;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.metric.MetricClientProvider.NoopMetricClientProvider.NoopFleakCounter;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.ChronicleStoreForward;
import io.fleak.zephflow.lib.commands.sink.PassThroughMessagePreProcessor;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.commands.sink.SinkStoreForward;
import io.fleak.zephflow.lib.commands.sink.StoreForwardCleaner;
import io.fleak.zephflow.lib.commands.sink.StoreForwardPaths;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.jetbrains.annotations.NotNull;

public class KafkaSinkCommand extends SimpleSinkCommand<RecordFleakData> {

  private final KafkaProducerClientFactory kafkaProducerClientFactory;

  protected KafkaSinkCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      KafkaProducerClientFactory kafkaProducerClientFactory) {
    super(nodeId, jobContext, configParser, configValidator);
    this.kafkaProducerClientFactory = kafkaProducerClientFactory;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_KAFKA_SINK;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    SinkCounters counters =
        createSinkCounters(metricClientProvider, jobContext, commandName(), nodeId);

    KafkaSinkDto.Config config = (KafkaSinkDto.Config) commandConfig;
    KafkaConnectionFailureClassifier classifier =
        config.isStoreAndForwardEnabled() ? new KafkaConnectionFailureClassifier() : null;

    SimpleSinkCommand.Flusher<RecordFleakData> flusher =
        createKafkaFlusher(
            config,
            counters.sinkOutputCounter(),
            counters.outputSizeCounter(),
            counters.sinkErrorCounter(),
            classifier);

    SimpleSinkCommand.SinkMessagePreProcessor<RecordFleakData> messagePreProcessor =
        new PassThroughMessagePreProcessor();

    SinkStoreForward storeForward =
        createStoreForward(
            metricClientProvider,
            jobContext,
            config,
            nodeId,
            flusher,
            messagePreProcessor,
            classifier);

    return new SinkExecutionContext<>(
        flusher,
        messagePreProcessor,
        storeForward,
        counters.inputMessageCounter(),
        counters.errorCounter(),
        new NoopFleakCounter(),
        new NoopFleakCounter(),
        counters.sinkErrorCounter());
  }

  private SimpleSinkCommand.Flusher<RecordFleakData> createKafkaFlusher(
      KafkaSinkDto.Config config,
      FleakCounter asyncDeliveredCountCounter,
      FleakCounter asyncDeliveredSizeCounter,
      FleakCounter asyncErrorCounter,
      KafkaConnectionFailureClassifier classifier) {
    Properties props = getProperties(config);

    boolean isTestMode =
        jobContext != null
            && Boolean.TRUE.equals(jobContext.getOtherProperties().get(JobContext.FLAG_TEST_MODE));
    if (isTestMode) {
      props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
    }

    if (config.isStoreAndForwardEnabled()) {
      // Bounded timeouts so an outage surfaces quickly as a thrown failure (-> buffer) instead of
      // blocking. delivery.timeout.ms must be >= request.timeout.ms + linger.ms.
      props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
      props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "2000");
      props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "5000");
    }

    if (config.getProperties() != null) {
      props.putAll(config.getProperties());
    }

    EncodingType encodingType = EncodingType.valueOf(config.getEncodingType().toUpperCase());
    SerializerFactory<?> serializerFactory =
        SerializerFactory.createSerializerFactory(encodingType);
    FleakSerializer<?> serializer = serializerFactory.createSerializer();

    PathExpression partitionKeyExpression =
        Optional.ofNullable(config.getPartitionKeyFieldExpressionStr())
            .filter(StringUtils::isNotBlank)
            .map(PathExpression::fromString)
            .orElse(null);

    KafkaProducer<byte[], byte[]> producer = kafkaProducerClientFactory.createKafkaProducer(props);

    if (isTestMode) {
      verifyConnectivity(producer, config);
    }

    return new KafkaSinkFlusher(
        producer,
        config.getTopic(),
        serializer,
        partitionKeyExpression,
        asyncDeliveredCountCounter,
        asyncDeliveredSizeCounter,
        asyncErrorCounter,
        config.isStoreAndForwardEnabled(),
        classifier);
  }

  private static final int STORE_FORWARD_DRAIN_CHUNK = 1000;
  private static final String METRIC_NAME_STORE_FORWARD_BUFFERED = "store_forward_buffered_count";
  private static final String METRIC_NAME_STORE_FORWARD_REPLAYED = "store_forward_replayed_count";
  private static final String METRIC_NAME_STORE_FORWARD_DROPPED = "store_forward_dropped_count";

  private SinkStoreForward createStoreForward(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      KafkaSinkDto.Config config,
      String nodeId,
      SimpleSinkCommand.Flusher<RecordFleakData> flusher,
      SimpleSinkCommand.SinkMessagePreProcessor<RecordFleakData> preprocessor,
      KafkaConnectionFailureClassifier classifier) {
    if (!config.isStoreAndForwardEnabled()) {
      return SinkStoreForward.noop();
    }

    Path storePath = StoreForwardPaths.resolve(config.getLocalStorePath(), jobContext, nodeId);
    StoreForwardCleaner.sweepOnce(config.getLocalStorePath(), jobContext, storePath);

    Map<String, String> metricTags =
        basicCommandMetricTags(jobContext.getMetricTags(), commandName(), nodeId);

    ChronicleStoreForward storeForward =
        new ChronicleStoreForward(
            new ChronicleStoreForward.Config(
                storePath,
                config.getLocalStoreMaxBytes(),
                config.getForwardRetryIntervalMillis(),
                STORE_FORWARD_DRAIN_CHUNK,
                nodeId),
            classifier,
            metricClientProvider.counter(METRIC_NAME_STORE_FORWARD_BUFFERED, metricTags),
            metricClientProvider.counter(METRIC_NAME_STORE_FORWARD_REPLAYED, metricTags),
            metricClientProvider.counter(METRIC_NAME_STORE_FORWARD_DROPPED, metricTags));

    storeForward.start(
        records -> {
          SimpleSinkCommand.PreparedInputEvents<RecordFleakData> prepared =
              new SimpleSinkCommand.PreparedInputEvents<>();
          long ts = System.currentTimeMillis();
          for (RecordFleakData record : records) {
            prepared.add(record, preprocessor.preprocess(record, ts));
          }
          flusher.flush(prepared, Map.of());
        });
    return storeForward;
  }

  private static @NotNull Properties getProperties(KafkaSinkDto.Config config) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBroker());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

    // Performance optimizations - Kafka's native batching handles throughput
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");
    props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864");
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
    props.put(ProducerConfig.ACKS_CONFIG, "1");
    props.put(ProducerConfig.RETRIES_CONFIG, "3");
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
    return props;
  }

  /**
   * Fails the test run immediately if the broker is unreachable or misconfigured, instead of
   * blocking {@code max.block.ms} for every event and then silently reporting success (FLE-840).
   * One bounded metadata lookup (capped by {@code max.block.ms}, set to 10s in test mode) surfaces
   * the real connection/auth failure.
   */
  private static void verifyConnectivity(
      KafkaProducer<byte[], byte[]> producer, KafkaSinkDto.Config config) {
    try {
      List<PartitionInfo> partitions = producer.partitionsFor(config.getTopic());
      if (partitions == null || partitions.isEmpty()) {
        throw new IllegalStateException(
            "topic '%s' not found or has no partitions".formatted(config.getTopic()));
      }
    } catch (Exception e) {
      producer.close(Duration.ZERO);
      throw new IllegalArgumentException(
          "Failed to reach Kafka broker '%s' for topic '%s'. Check the broker address, credentials, and security/SASL settings."
              .formatted(config.getBroker(), config.getTopic()),
          e);
    }
  }

  @Override
  protected int batchSize() {
    // Use very large batch size to effectively disable SimpleSinkCommand-level batching
    // Let Kafka's native batching handle throughput optimization
    return Integer.MAX_VALUE;
  }
}
