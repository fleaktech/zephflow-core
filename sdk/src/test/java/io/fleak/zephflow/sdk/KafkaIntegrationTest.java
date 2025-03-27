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
package io.fleak.zephflow.sdk;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.fleak.zephflow.lib.serdes.EncodingType;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.*;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;
import lombok.NonNull;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** Created by bolei on 3/17/25 */
@Testcontainers
public class KafkaIntegrationTest {
  private static final String INPUT_TOPIC_NAME = "in_topic";
  private static final String OUTPUT_TOPIC_NAME = "out_topic";

  @Container
  private static final KafkaContainer KAFKA_CONTAINER =
      new KafkaContainer(DockerImageName.parse("apache/kafka:latest"));

  private static AdminClient adminClient;
  private static KafkaProducer<byte[], byte[]> producer;
  private static KafkaConsumer<byte[], byte[]> consumer;

  @BeforeAll
  static void setupKafka() throws Exception {
    // Create AdminClient to manage topics
    Properties adminProps = new Properties();
    adminProps.put(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
    adminClient = AdminClient.create(adminProps);

    // Create topic
    NewTopic inputTopic = new NewTopic(INPUT_TOPIC_NAME, 1, (short) 1);
    adminClient.createTopics(Collections.singleton(inputTopic)).all().get(30, TimeUnit.SECONDS);
    NewTopic outputTopic = new NewTopic(OUTPUT_TOPIC_NAME, 1, (short) 1);
    adminClient.createTopics(Collections.singleton(outputTopic)).all().get(30, TimeUnit.SECONDS);

    Properties producerProps = getProducerProperties();
    producer = new KafkaProducer<>(producerProps);
    sendTestMessages();

    Properties consumerProps = getConsumerProperties();
    consumer = new KafkaConsumer<>(consumerProps);
    consumer.subscribe(Collections.singletonList(OUTPUT_TOPIC_NAME));
  }

  @AfterAll
  static void tearDown() {
    if (producer != null) {
      producer.close();
    }
    if (consumer != null) {
      consumer.close();
    }

    if (adminClient != null) {
      adminClient.close();
    }
  }

  private static @Nonnull Properties getProducerProperties() {
    // Setup producer to send test messages
    Properties producerProps = new Properties();
    producerProps.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
    producerProps.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    return producerProps;
  }

  private static @NonNull Properties getConsumerProperties() {
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

  // read json data from kafka input topic and write it into output topic,
  @Test
  public void testKafkaSourceAndSink() throws Exception {
    String broker = KAFKA_CONTAINER.getBootstrapServers();
    ZephFlow fromKafka =
        ZephFlow.startFlow()
            .kafkaSource(broker, INPUT_TOPIC_NAME, "test_group", EncodingType.JSON_OBJECT, null);
    var stream =
        fromKafka.kafkaSink(broker, OUTPUT_TOPIC_NAME, null, EncodingType.JSON_OBJECT, null);

    //noinspection resource
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> future =
          executor.submit(
              () -> {
                try {
                  stream.execute("test_job_id", "test_env", "test_service");
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              });

      // Wait for messages to be consumed (adjust timeout as needed)
      try {
        Thread.sleep(5000); // Give the consumer time to start and connect
        sendTestMessages();
        future.get(5, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        // Expected - the execute method runs until explicitly stopped
      } finally {
        future.cancel(true);
      }

      ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(5));

      // Verify that messages were consumed
      assertEquals(6, StreamSupport.stream(records.spliterator(), false).count());
    } finally {
      executor.shutdownNow();
      //noinspection ResultOfMethodCallIgnored
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private static void sendTestMessages() {
    // Sample JSON messages for testing
    String[] testMessages = {
      "{\"id\":1,\"value\":\"test1\",\"timestamp\":\"2024-09-24T10:00:00Z\"}",
      "{\"id\":2,\"value\":\"test2\",\"timestamp\":\"2024-09-24T10:01:00Z\"}",
      "{\"id\":3,\"value\":\"test3\",\"timestamp\":\"2024-09-24T10:02:00Z\"}"
    };

    // Send messages to Kafka
    for (String message : testMessages) {
      producer.send(
          new ProducerRecord<>(INPUT_TOPIC_NAME, null, message.getBytes(StandardCharsets.UTF_8)));
    }
    producer.flush();
  }
}
