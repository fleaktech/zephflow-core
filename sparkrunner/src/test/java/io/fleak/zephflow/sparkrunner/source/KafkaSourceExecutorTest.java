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
package io.fleak.zephflow.sparkrunner.source;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ExecutionContext;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceCommand;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.kafkasource.KafkaSourceCommand;
import io.fleak.zephflow.lib.commands.kafkasource.KafkaSourceCommandFactory;
import io.fleak.zephflow.lib.commands.kafkasource.KafkaSourceDto;
import io.fleak.zephflow.lib.commands.source.SimpleSourceCommand;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.sparkrunner.SparkSchemas;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers
class KafkaSourceExecutorTest {

  private static final String TOPIC_NAME = "test_topic";

  @Container
  private static final KafkaContainer KAFKA_CONTAINER =
      new KafkaContainer("apache/kafka-native:3.8.0");

  private static AdminClient adminClient;
  private static KafkaProducer<byte[], byte[]> producer;
  private static SparkSession spark;

  @BeforeAll
  static void setup() throws Exception {
    // Setup Kafka
    Properties adminProps = new Properties();
    adminProps.put(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
    adminClient = AdminClient.create(adminProps);

    NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, (short) 1);
    adminClient.createTopics(Collections.singleton(newTopic)).all().get(30, TimeUnit.SECONDS);

    Properties producerProps = new Properties();
    producerProps.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
    producerProps.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producer = new KafkaProducer<>(producerProps);

    // Setup Spark
    spark =
        SparkSession.builder()
            .appName("KafkaSourceExecutorTest")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate();
  }

  @AfterAll
  static void tearDown() {
    if (producer != null) {
      producer.close();
    }
    if (adminClient != null) {
      adminClient.close();
    }
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  void testExecute_withJsonMessages() {
    sendTestMessages();

    KafkaSourceCommandFactory commandFactory = new KafkaSourceCommandFactory();
    JobContext jobContext = JobContext.builder().metricTags(Map.of()).build();
    KafkaSourceCommand kafkaSourceCommand = commandFactory.createCommand("test_node", jobContext);

    KafkaSourceDto.Config config =
        KafkaSourceDto.Config.builder()
            .broker(KAFKA_CONTAINER.getBootstrapServers())
            .topic(TOPIC_NAME)
            .encodingType(EncodingType.JSON_OBJECT)
            .groupId("test-group-" + System.currentTimeMillis())
            .properties(Map.of("auto.offset.reset", "earliest"))
            .build();

    kafkaSourceCommand.parseAndValidateArg(
        OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));

    KafkaSourceExecutor executor = new KafkaSourceExecutor();
    Dataset<Row> result = executor.execute(kafkaSourceCommand, spark);

    assertNotNull(result);
    assertEquals(SparkSchemas.INPUT_EVENT_SCHEMA, result.schema());
    assertTrue(result.isStreaming());
    assertEquals(1, result.schema().fields().length);
    assertEquals("data", result.schema().fields()[0].name());
  }

  @Test
  void testExecute_verifySchema() {
    KafkaSourceCommandFactory commandFactory = new KafkaSourceCommandFactory();
    JobContext jobContext = JobContext.builder().metricTags(Map.of()).build();
    KafkaSourceCommand kafkaSourceCommand = commandFactory.createCommand("test_node", jobContext);

    KafkaSourceDto.Config config =
        KafkaSourceDto.Config.builder()
            .broker(KAFKA_CONTAINER.getBootstrapServers())
            .topic(TOPIC_NAME)
            .encodingType(EncodingType.JSON_OBJECT)
            .groupId("test-group-verify-" + System.currentTimeMillis())
            .properties(Map.of("auto.offset.reset", "latest"))
            .build();

    kafkaSourceCommand.parseAndValidateArg(
        OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));

    KafkaSourceExecutor executor = new KafkaSourceExecutor();
    Dataset<Row> result = executor.execute(kafkaSourceCommand, spark);

    assertEquals(SparkSchemas.INPUT_EVENT_SCHEMA, result.schema());
    assertEquals(1, result.schema().fields().length);
    assertEquals("data", result.schema().fields()[0].name());
  }

  @Test
  void testExecute_canStartStreamingQuery() throws Exception {
    sendTestMessages();

    KafkaSourceCommandFactory commandFactory = new KafkaSourceCommandFactory();
    JobContext jobContext = JobContext.builder().metricTags(Map.of()).build();
    KafkaSourceCommand kafkaSourceCommand = commandFactory.createCommand("test_node", jobContext);

    KafkaSourceDto.Config config =
        KafkaSourceDto.Config.builder()
            .broker(KAFKA_CONTAINER.getBootstrapServers())
            .topic(TOPIC_NAME)
            .encodingType(EncodingType.JSON_OBJECT)
            .groupId("test-group-streaming-" + System.currentTimeMillis())
            .properties(Map.of("auto.offset.reset", "earliest"))
            .build();

    kafkaSourceCommand.parseAndValidateArg(
        OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));

    KafkaSourceExecutor executor = new KafkaSourceExecutor();
    Dataset<Row> result = executor.execute(kafkaSourceCommand, spark);

    // Verify we can start a streaming query
    org.apache.spark.sql.streaming.StreamingQuery query =
        result
            .writeStream()
            .format("console")
            .option("checkpointLocation", "/tmp/kafka-test-checkpoint-" + System.currentTimeMillis())
            .start();

    // Verify the query is active
    assertTrue(query.isActive());
    assertNotNull(query.id());

    // Stop the query
    query.stop();
    query.awaitTermination(5000);
    assertFalse(query.isActive());
  }

  @Test
  void testExecute_invalidCommandType() {
    KafkaSourceExecutor executor = new KafkaSourceExecutor();

    // Create a non-Kafka source command
    JobContext jobContext = JobContext.builder().metricTags(Map.of()).build();

    // Use an anonymous class to create an invalid command type
    SimpleSourceCommand<SerializedEvent> invalidCommand =
        new SimpleSourceCommand<>("test", jobContext, null, null) {
          @Override
          public SourceCommand.SourceType sourceType() {
            return SourceCommand.SourceType.BATCH;
          }

          @Override
          public String commandName() {
            return "notKafka";
          }

          @Override
          protected ExecutionContext createExecutionContext(
              MetricClientProvider metricClientProvider,
              JobContext jobContext,
              CommandConfig commandConfig,
              String nodeId) {
            return null;
          }
        };

    assertThrows(IllegalArgumentException.class, () -> executor.execute(invalidCommand, spark));
  }

  private void sendTestMessages() {
    for (int i = 1; i <= 3; i++) {
      String message =
          String.format(
              "{\"id\":%d,\"value\":\"test%d\",\"timestamp\":\"2024-09-24T10:00:00Z\"}", i, i);
      producer.send(
          new ProducerRecord<>(TOPIC_NAME, null, message.getBytes(StandardCharsets.UTF_8)));
    }
    producer.flush();
  }
}
