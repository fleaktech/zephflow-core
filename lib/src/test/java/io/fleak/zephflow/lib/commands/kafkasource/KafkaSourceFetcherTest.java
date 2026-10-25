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
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.lib.commands.source.BatchCommitStrategy;
import io.fleak.zephflow.lib.commands.source.BytesRawDataConverter;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.time.Duration;
import java.util.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

/** Created by bolei on 9/24/24 */
class KafkaSourceFetcherTest {

  @Test
  void testFetch_withNoRecords() {
    KafkaConsumer<byte[], byte[]> mockConsumer = mock();
    // Set up mockConsumer to return empty ConsumerRecords
    ConsumerRecords<byte[], byte[]> emptyRecords = new ConsumerRecords<>(Collections.emptyMap());
    when(mockConsumer.poll(Duration.ofMillis(1000))).thenReturn(emptyRecords);

    KafkaSourceFetcher fetcher = new KafkaSourceFetcher(mockConsumer, null);
    // Call fetch
    var result = fetcher.fetch();

    // Verify that the result is an empty list
    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testFetch_withRecords() {
    KafkaConsumer<byte[], byte[]> mockConsumer = mock();
    // Create sample ConsumerRecord
    byte[] key = "key".getBytes();
    byte[] value =
        """
    {"f1": 100}"""
            .getBytes();
    String topic = "test-topic";
    int partition = 0;
    long offset = 100L;
    long timestamp = System.currentTimeMillis();
    TimestampType timestampType = TimestampType.CREATE_TIME;
    int serializedKeySize = key.length;
    int serializedValueSize = value.length;
    Optional<Integer> leaderEpoch = Optional.of(1);

    Header header1 = new RecordHeader("headerKey1", "headerValue1".getBytes());
    Header header2 = new RecordHeader("headerKey2", "headerValue2".getBytes());

    Headers headers = new RecordHeaders(List.of(header1, header2));

    // Create ConsumerRecord
    ConsumerRecord<byte[], byte[]> consumerRecord =
        new ConsumerRecord<>(
            topic,
            partition,
            offset,
            timestamp,
            timestampType,
            serializedKeySize,
            serializedValueSize,
            key,
            value,
            headers,
            leaderEpoch);

    // Create ConsumerRecords
    Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> recordsMap = new HashMap<>();
    recordsMap.put(new TopicPartition(topic, partition), Collections.singletonList(consumerRecord));
    ConsumerRecords<byte[], byte[]> consumerRecords = new ConsumerRecords<>(recordsMap);

    // Set up mockConsumer to return consumerRecords
    when(mockConsumer.poll(Duration.ofMillis(1000))).thenReturn(consumerRecords);

    FleakDeserializer<?> deserializer =
        DeserializerFactory.createDeserializerFactory(EncodingType.JSON_OBJECT)
            .createDeserializer();
    KafkaSourceFetcher fetcher = new KafkaSourceFetcher(mockConsumer, null);

    BytesRawDataConverter converter = new BytesRawDataConverter(deserializer);

    SourceExecutionContext<SerializedEvent> sourceInitializedConfig = mock();
    when(sourceInitializedConfig.dataSizeCounter()).thenReturn(mock());
    when(sourceInitializedConfig.inputEventCounter()).thenReturn(mock());
    when(sourceInitializedConfig.deserializeFailureCounter()).thenReturn(mock());
    var fetchedData = fetcher.fetch();
    var result =
        fetchedData.stream()
            .flatMap(x -> converter.convert(x, sourceInitializedConfig).transformedData().stream())
            .toList();
    // Verify that the result contains the deserialized data
    assertNotNull(result);
    assertEquals(1, result.size());

    // With metadata removed, we only expect the actual deserialized data
    Map<String, Object> payload = Map.of("f1", 100L);

    assertEquals(FleakData.wrap(payload), result.get(0));
  }

  private record FetchedEvent(String key, String value, Map<String, String> metadata) {
    static FetchedEvent from(SerializedEvent event) {
      return new FetchedEvent(new String(event.key()), new String(event.value()), event.metadata());
    }
  }

  private static KafkaConsumer<byte[], byte[]> consumerReturningRecordWithHeader() {
    KafkaConsumer<byte[], byte[]> mockConsumer = mock();
    RecordHeaders headers = new RecordHeaders();
    headers.add("asset_id", "a1".getBytes());
    ConsumerRecord<byte[], byte[]> consumerRecord =
        new ConsumerRecord<>(
            "topic",
            0,
            0L,
            0L,
            TimestampType.CREATE_TIME,
            1,
            1,
            "k".getBytes(),
            "v".getBytes(),
            headers,
            Optional.empty());
    when(mockConsumer.poll(Duration.ofMillis(1000)))
        .thenReturn(
            new ConsumerRecords<>(Map.of(new TopicPartition("topic", 0), List.of(consumerRecord))));
    return mockConsumer;
  }

  @Test
  void extractorMetadataReachesSerializedEvent() {
    KafkaSourceFetcher fetcher =
        new KafkaSourceFetcher(
            consumerReturningRecordWithHeader(),
            null,
            BatchCommitStrategy.forKafka(),
            KafkaHeaderMetadata::fromHeaders);

    assertEquals(
        List.of(new FetchedEvent("k", "v", Map.of("__md_kafka_header_asset_id", "a1"))),
        fetcher.fetch().stream().map(FetchedEvent::from).toList());
  }

  @Test
  void defaultConstructorsKeepMetadataNull() {
    List<FetchedEvent> expected = List.of(new FetchedEvent("k", "v", null));

    assertEquals(
        expected,
        new KafkaSourceFetcher(consumerReturningRecordWithHeader(), null)
            .fetch().stream().map(FetchedEvent::from).toList());
    assertEquals(
        expected,
        new KafkaSourceFetcher(
                consumerReturningRecordWithHeader(), null, BatchCommitStrategy.forKafka())
            .fetch().stream().map(FetchedEvent::from).toList());
  }
}
