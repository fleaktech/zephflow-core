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

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.PassThroughMessagePreProcessor;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkCommandPartsFactory;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import java.util.Optional;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class KafkaSinkPartsFactory extends SinkCommandPartsFactory<RecordFleakData> {
  private final KafkaSinkDto.Config config;
  private final KafkaProducerClientFactory kafkaProducerClientFactory;

  protected KafkaSinkPartsFactory(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      KafkaSinkDto.Config config,
      KafkaProducerClientFactory kafkaProducerClientFactory) {
    super(metricClientProvider, jobContext);
    this.config = config;
    this.kafkaProducerClientFactory = kafkaProducerClientFactory;
  }

  @Override
  public SimpleSinkCommand.Flusher<RecordFleakData> createFlusher() {
    Properties props = calculateProducerProperties();

    EncodingType encodingType = EncodingType.valueOf(config.getEncodingType().toUpperCase());
    SerializerFactory<?> serializerFactory =
        SerializerFactory.createSerializerFactory(encodingType);
    FleakSerializer<?> serializer = serializerFactory.createSerializer();

    PathExpression partitionKeyExpression =
        Optional.ofNullable(config.getPartitionKeyFieldExpressionStr())
            .filter(StringUtils::isNotBlank)
            .map(PathExpression::fromString)
            .orElse(null);

    return new KafkaSinkFlusher(
        kafkaProducerClientFactory.createKafkaProducer(props),
        config.getTopic(),
        serializer,
        partitionKeyExpression);
  }

  private Properties calculateProducerProperties() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBroker());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

    // Performance optimizations for batching
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536"); // 64KB batches (up from 16KB default)
    props.put(ProducerConfig.LINGER_MS_CONFIG, "10"); // Wait 10ms for batching
    props.put(
        ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864"); // 64MB buffer (up from 32MB default)
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4"); // Fast compression
    props.put(ProducerConfig.ACKS_CONFIG, "1"); // Leader acknowledgment only
    props.put(ProducerConfig.RETRIES_CONFIG, "3"); // Retry on failure
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // Pipeline requests

    if (config.getProperties() != null) {
      props.putAll(config.getProperties());
    }

    return props;
  }

  @Override
  public SimpleSinkCommand.SinkMessagePreProcessor<RecordFleakData> createMessagePreProcessor() {
    return new PassThroughMessagePreProcessor();
  }
}
