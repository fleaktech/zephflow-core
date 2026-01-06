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

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.PassThroughMessagePreProcessor;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import java.util.Optional;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
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
    SimpleSinkCommand.Flusher<RecordFleakData> flusher =
        createKafkaFlusher(
            config,
            counters.sinkOutputCounter(),
            counters.outputSizeCounter(),
            counters.sinkErrorCounter());

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

  private SimpleSinkCommand.Flusher<RecordFleakData> createKafkaFlusher(
      KafkaSinkDto.Config config,
      FleakCounter asyncSuccessCounter,
      FleakCounter asyncOutputSizeCounter,
      FleakCounter asyncErrorCounter) {
    Properties props = getProperties(config);

    boolean isTestMode =
        jobContext != null
            && Boolean.TRUE.equals(jobContext.getOtherProperties().get(JobContext.FLAG_TEST_MODE));
    if (isTestMode) {
      props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
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

    return new KafkaSinkFlusher(
        producer,
        config.getTopic(),
        serializer,
        partitionKeyExpression,
        asyncSuccessCounter,
        asyncOutputSizeCounter,
        asyncErrorCounter);
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

  @Override
  protected int batchSize() {
    // Use very large batch size to effectively disable SimpleSinkCommand-level batching
    // Let Kafka's native batching handle throughput optimization
    return Integer.MAX_VALUE;
  }
}
