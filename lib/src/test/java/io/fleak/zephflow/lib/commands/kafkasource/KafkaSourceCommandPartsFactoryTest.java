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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Created by bolei on 9/24/24 */
public class KafkaSourceCommandPartsFactoryTest {

  @Test
  public void testCreateFetcher() {
    KafkaConsumerClientFactory kafkaConsumerClientFactory = mock();
    MetricClientProvider metricClientProvider = mock();
    KafkaConsumer<byte[], byte[]> kafkaConsumer = mock();
    KafkaSourceCommandPartsFactory factory =
        new KafkaSourceCommandPartsFactory(metricClientProvider, kafkaConsumerClientFactory);

    Map<String, String> additionalProps = new HashMap<>();
    additionalProps.put("some.property", "some.value");
    additionalProps.put("another.property", "another.value");

    KafkaSourceDto.Config config =
        KafkaSourceDto.Config.builder()
            .broker("localhost:9092")
            .topic("test-topic")
            .groupId("test-group")
            .encodingType(EncodingType.JSON_OBJECT)
            .properties(additionalProps)
            .build();

    // Mock the kafkaConsumerClientFactory to return the mocked kafkaConsumer
    when(kafkaConsumerClientFactory.createKafkaConsumer(any(Properties.class)))
        .thenReturn(kafkaConsumer);

    // Use ArgumentCaptor to capture the consumerProps
    ArgumentCaptor<Properties> propertiesCaptor = ArgumentCaptor.forClass(Properties.class);

    // Call createFetcher
    Fetcher fetcher = factory.createFetcher(config);

    // Verify that createKafkaConsumer was called with correct parameters and capture the Properties
    verify(kafkaConsumerClientFactory).createKafkaConsumer(propertiesCaptor.capture());

    // Get the captured Properties
    Properties consumerProps = propertiesCaptor.getValue();

    assertEquals(
        Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            "localhost:9092",
            ConsumerConfig.GROUP_ID_CONFIG,
            "test-group",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            ByteArrayDeserializer.class.getName(),
            "some.property",
            "some.value",
            "another.property",
            "another.value",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
            "true",
            ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
            "500",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
            "earliest"),
        consumerProps);

    verify(kafkaConsumer)
        .subscribe(eq(List.of("test-topic")), any(ConsumerRebalanceListener.class));

    assertInstanceOf(KafkaSourceFetcher.class, fetcher);
  }
}
