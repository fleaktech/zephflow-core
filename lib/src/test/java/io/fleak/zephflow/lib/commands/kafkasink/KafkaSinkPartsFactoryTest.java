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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class KafkaSinkPartsFactoryTest {

  @Test
  public void testCreateFlusher_ProducerConfiguration() throws Exception {
    // Arrange
    KafkaProducerClientFactory kafkaProducerClientFactory = mock();
    MetricClientProvider metricClientProvider = mock();
    KafkaProducer<byte[], byte[]> kafkaProducer = mock();

    Map<String, String> additionalProps = new HashMap<>();
    additionalProps.put("compression.type", "snappy");
    additionalProps.put("batch.size", "16384");

    KafkaSinkDto.Config config =
        KafkaSinkDto.Config.builder()
            .broker("localhost:9092")
            .topic("test-topic")
            .encodingType(EncodingType.JSON_OBJECT.name())
            .partitionKeyFieldExpressionStr("$.user_id")
            .properties(additionalProps)
            .build();

    KafkaSinkPartsFactory factory =
        new KafkaSinkPartsFactory(
            metricClientProvider,
            mock(), // JobContext
            config,
            kafkaProducerClientFactory);

    when(kafkaProducerClientFactory.createKafkaProducer(any(Properties.class)))
        .thenReturn(kafkaProducer);

    ArgumentCaptor<Properties> propertiesCaptor = ArgumentCaptor.forClass(Properties.class);

    // Act
    SimpleSinkCommand.Flusher<?> flusher = factory.createFlusher();

    // Assert
    verify(kafkaProducerClientFactory).createKafkaProducer(propertiesCaptor.capture());

    Properties producerProps = propertiesCaptor.getValue();
    
    // Verify core configuration is propagated
    assertEquals("localhost:9092", producerProps.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals("snappy", producerProps.get("compression.type"));
    assertEquals("16384", producerProps.get("batch.size"));
    
    // Verify the correct flusher type is created
    assertInstanceOf(BatchKafkaSinkFlusher.class, flusher);
    
    // Cleanup
    flusher.close();
  }

  @Test
  public void testCreateFlusher_DefaultConfiguration() throws Exception {
    // Arrange - minimal config without optional properties
    KafkaProducerClientFactory kafkaProducerClientFactory = mock();
    MetricClientProvider metricClientProvider = mock();
    KafkaProducer<byte[], byte[]> kafkaProducer = mock();

    KafkaSinkDto.Config config =
        KafkaSinkDto.Config.builder()
            .broker("localhost:9092")
            .topic("default-topic")
            .encodingType(EncodingType.JSON_OBJECT.name())
            .build(); // No custom properties

    KafkaSinkPartsFactory factory =
        new KafkaSinkPartsFactory(metricClientProvider, mock(), config, kafkaProducerClientFactory);

    when(kafkaProducerClientFactory.createKafkaProducer(any(Properties.class)))
        .thenReturn(kafkaProducer);

    // Act
    SimpleSinkCommand.Flusher<?> flusher = factory.createFlusher();

    // Assert
    assertNotNull(flusher);
    assertInstanceOf(BatchKafkaSinkFlusher.class, flusher);
    verify(kafkaProducerClientFactory).createKafkaProducer(any(Properties.class));
    
    // Cleanup
    flusher.close();
  }
}
