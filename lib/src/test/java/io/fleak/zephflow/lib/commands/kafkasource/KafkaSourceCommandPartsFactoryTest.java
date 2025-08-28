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
import io.fleak.zephflow.lib.commands.source.*;
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

    // Verify the essential properties are set correctly
    assertEquals("localhost:9092", consumerProps.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
    assertEquals("test-group", consumerProps.get(ConsumerConfig.GROUP_ID_CONFIG));
    assertEquals(
        ByteArrayDeserializer.class.getName(),
        consumerProps.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG));
    assertEquals(
        ByteArrayDeserializer.class.getName(),
        consumerProps.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG));
    assertEquals("some.value", consumerProps.get("some.property"));
    assertEquals("another.value", consumerProps.get("another.property"));
    assertEquals("false", consumerProps.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    assertEquals("earliest", consumerProps.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
    
    // Verify additional performance tuning properties are set
    assertEquals("5000", consumerProps.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
    assertEquals("1048576", consumerProps.get(ConsumerConfig.FETCH_MIN_BYTES_CONFIG));
    assertEquals("100", consumerProps.get(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG));
    assertEquals("10000", consumerProps.get(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG));
    assertEquals("10485760", consumerProps.get(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG));

    verify(kafkaConsumer)
        .subscribe(eq(List.of("test-topic")), any(ConsumerRebalanceListener.class));

    assertInstanceOf(KafkaSourceFetcher.class, fetcher);
  }

  @Test
  public void testCreateFetcherWithDefaultCommitStrategy() {
    KafkaConsumerClientFactory kafkaConsumerClientFactory = mock();
    MetricClientProvider metricClientProvider = mock();
    KafkaConsumer<byte[], byte[]> kafkaConsumer = mock();
    KafkaSourceCommandPartsFactory factory =
        new KafkaSourceCommandPartsFactory(metricClientProvider, kafkaConsumerClientFactory);

    // Use default commit strategy (not specified)
    KafkaSourceDto.Config config =
        KafkaSourceDto.Config.builder()
            .broker("localhost:9092")
            .topic("test-topic")
            .groupId("test-group")
            .encodingType(EncodingType.JSON_OBJECT)
            .build();

    when(kafkaConsumerClientFactory.createKafkaConsumer(any(Properties.class)))
        .thenReturn(kafkaConsumer);

    KafkaSourceFetcher fetcher = (KafkaSourceFetcher) factory.createFetcher(config);
    CommitStrategy commitStrategy = fetcher.commitStrategy();

    // Should use default batch strategy
    assertInstanceOf(BatchCommitStrategy.class, commitStrategy);
    assertEquals(1000, commitStrategy.getCommitBatchSize());
    assertEquals(5000L, commitStrategy.getCommitIntervalMs());
    assertEquals(CommitStrategy.CommitMode.BATCH, commitStrategy.getCommitMode());
  }

  @Test
  public void testCreateFetcherWithCustomBatchCommitStrategy() {
    KafkaConsumerClientFactory kafkaConsumerClientFactory = mock();
    MetricClientProvider metricClientProvider = mock();
    KafkaConsumer<byte[], byte[]> kafkaConsumer = mock();
    KafkaSourceCommandPartsFactory factory =
        new KafkaSourceCommandPartsFactory(metricClientProvider, kafkaConsumerClientFactory);

    KafkaSourceDto.Config config =
        KafkaSourceDto.Config.builder()
            .broker("localhost:9092")
            .topic("test-topic")
            .groupId("test-group")
            .encodingType(EncodingType.JSON_OBJECT)
            .commitStrategy(KafkaSourceDto.CommitStrategyType.BATCH)
            .commitBatchSize(2500)
            .commitIntervalMs(10000L)
            .build();

    when(kafkaConsumerClientFactory.createKafkaConsumer(any(Properties.class)))
        .thenReturn(kafkaConsumer);

    KafkaSourceFetcher fetcher = (KafkaSourceFetcher) factory.createFetcher(config);
    CommitStrategy commitStrategy = fetcher.commitStrategy();

    assertInstanceOf(BatchCommitStrategy.class, commitStrategy);
    assertEquals(2500, commitStrategy.getCommitBatchSize());
    assertEquals(10000L, commitStrategy.getCommitIntervalMs());
    assertEquals(CommitStrategy.CommitMode.BATCH, commitStrategy.getCommitMode());
  }

  @Test
  public void testCreateFetcherWithPerRecordCommitStrategy() {
    KafkaConsumerClientFactory kafkaConsumerClientFactory = mock();
    MetricClientProvider metricClientProvider = mock();
    KafkaConsumer<byte[], byte[]> kafkaConsumer = mock();
    KafkaSourceCommandPartsFactory factory =
        new KafkaSourceCommandPartsFactory(metricClientProvider, kafkaConsumerClientFactory);

    KafkaSourceDto.Config config =
        KafkaSourceDto.Config.builder()
            .broker("localhost:9092")
            .topic("test-topic")
            .groupId("test-group")
            .encodingType(EncodingType.JSON_OBJECT)
            .commitStrategy(KafkaSourceDto.CommitStrategyType.PER_RECORD)
            .build();

    when(kafkaConsumerClientFactory.createKafkaConsumer(any(Properties.class)))
        .thenReturn(kafkaConsumer);

    KafkaSourceFetcher fetcher = (KafkaSourceFetcher) factory.createFetcher(config);
    CommitStrategy commitStrategy = fetcher.commitStrategy();

    assertInstanceOf(PerRecordCommitStrategy.class, commitStrategy);
    assertEquals(CommitStrategy.CommitMode.PER_RECORD, commitStrategy.getCommitMode());
    assertEquals(1, commitStrategy.getCommitBatchSize());
    assertEquals(0L, commitStrategy.getCommitIntervalMs());
  }

  @Test
  public void testCreateFetcherWithNoCommitStrategy() {
    KafkaConsumerClientFactory kafkaConsumerClientFactory = mock();
    MetricClientProvider metricClientProvider = mock();
    KafkaConsumer<byte[], byte[]> kafkaConsumer = mock();
    KafkaSourceCommandPartsFactory factory =
        new KafkaSourceCommandPartsFactory(metricClientProvider, kafkaConsumerClientFactory);

    KafkaSourceDto.Config config =
        KafkaSourceDto.Config.builder()
            .broker("localhost:9092")
            .topic("test-topic")
            .groupId("test-group")
            .encodingType(EncodingType.JSON_OBJECT)
            .commitStrategy(KafkaSourceDto.CommitStrategyType.NONE)
            .build();

    when(kafkaConsumerClientFactory.createKafkaConsumer(any(Properties.class)))
        .thenReturn(kafkaConsumer);

    KafkaSourceFetcher fetcher = (KafkaSourceFetcher) factory.createFetcher(config);
    CommitStrategy commitStrategy = fetcher.commitStrategy();

    assertInstanceOf(NoCommitStrategy.class, commitStrategy);
    assertEquals(CommitStrategy.CommitMode.NONE, commitStrategy.getCommitMode());
    assertEquals(0, commitStrategy.getCommitBatchSize());
    assertEquals(0L, commitStrategy.getCommitIntervalMs());
  }
}
