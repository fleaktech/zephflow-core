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
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableMap;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.lib.commands.source.BytesRawDataConverter;
import io.fleak.zephflow.lib.commands.source.SourceInitializedConfig;
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
    when(mockConsumer.poll(Duration.ofMillis(100))).thenReturn(emptyRecords);

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
    when(mockConsumer.poll(Duration.ofMillis(100))).thenReturn(consumerRecords);

    FleakDeserializer<?> deserializer =
        DeserializerFactory.createDeserializerFactory(EncodingType.JSON_OBJECT)
            .createDeserializer();
    KafkaSourceFetcher fetcher = new KafkaSourceFetcher(mockConsumer, null);

    BytesRawDataConverter converter = new BytesRawDataConverter(deserializer);

    SourceInitializedConfig<SerializedEvent> sourceInitializedConfig = mock();
    when(sourceInitializedConfig.dataSizeCounter()).thenReturn(mock());
    when(sourceInitializedConfig.inputEventCounter()).thenReturn(mock());
    when(sourceInitializedConfig.deserializeFailureCounter()).thenReturn(mock());
    var fetchedData = fetcher.fetch();
    var result =
        fetchedData.stream()
            .flatMap(
                x -> converter.convert(x, sourceInitializedConfig).getTransformedData().stream())
            .toList();
    // Verify that the result contains the deserialized data
    assertNotNull(result);
    assertEquals(1, result.size());

    Map<String, Object> payload =
        ImmutableMap.<String, Object>builder()
            .put("f1", 100L)
            .put(METADATA_KAFKA_TOPIC, topic)
            .put(METADATA_KAFKA_PARTITION, Integer.toString(partition))
            .put(METADATA_KAFKA_OFFSET, Long.toString(offset))
            .put(METADATA_KAFKA_TIMESTAMP, Long.toString(timestamp))
            .put(METADATA_KAFKA_TIMESTAMP_TYPE, timestampType.toString())
            .put(METADATA_KAFKA_SERIALIZED_KEY_SIZE, Integer.toString(serializedKeySize))
            .put(METADATA_KAFKA_SERIALIZED_VALUE_SIZE, Integer.toString(serializedValueSize))
            .put(METADATA_KAFKA_LEADER_EPOCH, leaderEpoch.map(Object::toString).orElse(null))
            .put(
                METADATA_KAFKA_HEADER_PREFIX + "headerKey1",
                Base64.getEncoder().encodeToString("headerValue1".getBytes()))
            .put(
                METADATA_KAFKA_HEADER_PREFIX + "headerKey2",
                Base64.getEncoder().encodeToString("headerValue2".getBytes()))
            .put(METADATA_KEY, toBase64String(key))
            .build();

    assertEquals(FleakData.wrap(payload), result.get(0));
  }
}
