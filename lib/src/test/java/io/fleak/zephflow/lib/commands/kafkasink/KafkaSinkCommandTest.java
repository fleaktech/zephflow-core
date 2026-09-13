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

import static io.fleak.zephflow.lib.utils.JsonUtils.*;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;
import lombok.NonNull;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/** Created by bolei on 3/17/25 */
@Testcontainers
class KafkaSinkCommandTest {
  private static final String TOPIC_NAME = "test_topic";

  @Container
  private static final KafkaContainer KAFKA_CONTAINER =
      new KafkaContainer("apache/kafka-native:3.8.0");

  private static AdminClient adminClient;
  private static KafkaConsumer<byte[], byte[]> consumer;

  static final List<RecordFleakData> SOURCE_EVENTS = new ArrayList<>();

  static {
    for (int i = 0; i < 10; ++i) {
      SOURCE_EVENTS.add((RecordFleakData) FleakData.wrap(Map.of("num", i)));
    }
  }

  @BeforeAll
  static void setupKafka() throws Exception {
    // Create AdminClient to manage topics
    Properties adminProps = new Properties();
    adminProps.put(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
    adminClient = AdminClient.create(adminProps);

    // Create topic
    NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, (short) 1);
    adminClient.createTopics(Collections.singleton(newTopic)).all().get(30, TimeUnit.SECONDS);

    Properties consumerProps = getProperties();

    consumer = new KafkaConsumer<>(consumerProps);
    consumer.subscribe(Collections.singletonList(TOPIC_NAME));
  }

  private static @NonNull Properties getProperties() {
    Properties consumerProps = new Properties();
    consumerProps.put(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    return consumerProps;
  }

  @AfterAll
  static void tearDown() {
    if (consumer != null) {
      consumer.close();
    }
    if (adminClient != null) {
      adminClient.close();
    }
    if (KAFKA_CONTAINER.isRunning()) {
      KAFKA_CONTAINER.stop();
    }
  }

  @Test
  public void testWriteToSink() throws Exception {
    KafkaSinkCommandFactory commandFactory = new KafkaSinkCommandFactory();
    KafkaSinkCommand kafkaSinkCommand =
        (KafkaSinkCommand) commandFactory.createCommand("my_node", TestUtils.JOB_CONTEXT);
    KafkaSinkDto.Config config =
        KafkaSinkDto.Config.builder()
            .topic(TOPIC_NAME)
            .broker(KAFKA_CONTAINER.getBootstrapServers())
            .encodingType(EncodingType.JSON_OBJECT.toString())
            .build();
    kafkaSinkCommand.parseAndValidateArg(
        OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));

    // Initialize and process
    kafkaSinkCommand.initialize(new MetricClientProvider.NoopMetricClientProvider());
    var context = kafkaSinkCommand.getExecutionContext();
    kafkaSinkCommand.writeToSink(SOURCE_EVENTS, "test_user", context);

    // Wait for records to be processed (simulating batch processing delay)
    Thread.sleep(2000);

    // Poll for records
    ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(5));

    // Count how many of our test records were found
    List<RecordFleakData> foundEvents =
        StreamSupport.stream(records.spliterator(), false)
            .map(
                r -> fromJsonString(new String(r.value()), new TypeReference<RecordFleakData>() {}))
            .toList();
    assertEquals(SOURCE_EVENTS, foundEvents);
  }

  /**
   * Regression test for FLE-2242: a numeric partition key used to resolve to null, so records were
   * sent unkeyed and one id's records were spread over every partition. Mirrors the manual
   * reproduction: ten records with ids 1x1, 2x2, 3x3, 4x4 over a four-partition topic.
   */
  @Test
  public void testNumericPartitionKeyKeepsEachKeyInOnePartition() throws Exception {
    String topic = "numeric_key_topic";
    adminClient
        .createTopics(Collections.singleton(new NewTopic(topic, 4, (short) 1)))
        .all()
        .get(30, TimeUnit.SECONDS);

    List<RecordFleakData> events = new ArrayList<>();
    for (int id = 1; id <= 4; id++) {
      for (int copy = 0; copy < id; copy++) {
        events.add((RecordFleakData) FleakData.wrap(Map.of("id", id, "copy", copy)));
      }
    }

    KafkaSinkCommand kafkaSinkCommand =
        (KafkaSinkCommand)
            new KafkaSinkCommandFactory().createCommand("my_node", TestUtils.JOB_CONTEXT);
    KafkaSinkDto.Config config =
        KafkaSinkDto.Config.builder()
            .topic(topic)
            .broker(KAFKA_CONTAINER.getBootstrapServers())
            .encodingType(EncodingType.JSON_OBJECT.toString())
            .partitionKeyFieldExpressionStr("$.id")
            .build();
    kafkaSinkCommand.parseAndValidateArg(
        OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));
    kafkaSinkCommand.initialize(new MetricClientProvider.NoopMetricClientProvider());
    kafkaSinkCommand.writeToSink(events, "test_user", kafkaSinkCommand.getExecutionContext());

    Properties props = getProperties();
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "numeric-key-consumer-group");
    Map<String, Set<Integer>> partitionsByKey = new HashMap<>();
    int consumed = 0;
    try (KafkaConsumer<byte[], byte[]> keyedConsumer = new KafkaConsumer<>(props)) {
      keyedConsumer.subscribe(Collections.singletonList(topic));
      long deadline = System.currentTimeMillis() + 30_000;
      while (consumed < events.size() && System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<byte[], byte[]> record :
            keyedConsumer.poll(Duration.ofSeconds(2)).records(topic)) {
          assertNotNull(record.key(), "record was sent without a key");
          partitionsByKey
              .computeIfAbsent(
                  new String(record.key(), StandardCharsets.UTF_8), k -> new HashSet<>())
              .add(record.partition());
          consumed++;
        }
      }
    }

    assertEquals(events.size(), consumed, "not all records were delivered");
    // Keys are the plain numeric ids ("4", not "4.0"), and each key sits in exactly one partition.
    assertEquals(Set.of("1", "2", "3", "4"), partitionsByKey.keySet());
    partitionsByKey.forEach(
        (key, partitions) ->
            assertEquals(1, partitions.size(), "key " + key + " was split across " + partitions));
  }

  @Test
  public void testHighVolumePerformance() {
    // Create a large number of test records to simulate the original performance problem
    List<RecordFleakData> largeEventSet = new ArrayList<>();
    for (int i = 0; i < 1000; i++) { // 1000 records to simulate high volume
      largeEventSet.add(
          (RecordFleakData) FleakData.wrap(Map.of("id", i, "data", "large-test-" + i)));
    }

    KafkaSinkCommandFactory commandFactory = new KafkaSinkCommandFactory();
    KafkaSinkCommand kafkaSinkCommand =
        (KafkaSinkCommand) commandFactory.createCommand("perf_test_node", TestUtils.JOB_CONTEXT);

    KafkaSinkDto.Config config =
        KafkaSinkDto.Config.builder()
            .topic(TOPIC_NAME)
            .broker(KAFKA_CONTAINER.getBootstrapServers())
            .encodingType(EncodingType.JSON_OBJECT.toString())
            .build();
    kafkaSinkCommand.parseAndValidateArg(
        OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));

    // Measure performance - this is the main validation
    long startTime = System.currentTimeMillis();

    // Initialize command
    kafkaSinkCommand.initialize(new MetricClientProvider.NoopMetricClientProvider());
    var context = kafkaSinkCommand.getExecutionContext();

    // Process large batch - this should NOT cause 1000 individual flushes
    assertDoesNotThrow(
        () -> {
          kafkaSinkCommand.writeToSink(largeEventSet, "perf_test_user", context);
        },
        "High volume write should not throw exceptions");

    long processingTime = System.currentTimeMillis() - startTime;

    // Should complete quickly due to batching (not 1000 individual flush operations)
    assertTrue(
        processingTime < 10000, // Should complete within 10 seconds
        "High volume processing took too long: "
            + processingTime
            + "ms. "
            + "This suggests batching is not working effectively.");

    System.out.println(
        "✅ High volume test: Processed "
            + largeEventSet.size()
            + " records in "
            + processingTime
            + "ms - Performance test PASSED");
  }
}
