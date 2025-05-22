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

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.Getter;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers
public class KafkaSourceCommandTest {

  private static final String TOPIC_NAME = "test_topic";

  @Container
  private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer("apache/kafka-native:3.8.0");

  private static AdminClient adminClient;
  private static KafkaProducer<byte[], byte[]> producer;

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

    // Setup producer to send test messages
    Properties producerProps = new Properties();
    producerProps.put(
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
    producerProps.put(
        ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producerProps.put(
        ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    producer = new KafkaProducer<>(producerProps);
  }

  @AfterAll
  static void tearDown() {
    if (producer != null) {
      producer.close();
    }
    if (adminClient != null) {
      adminClient.close();
    }
    if (KAFKA_CONTAINER.isRunning()) {
      KAFKA_CONTAINER.stop();
    }
  }

  // - send a batch of events to kafka before consumer starts
  // - start the consumer with latest offset and wait for it to be connected
  // - send another batch of events
  // - confirm that the consumer only receives the second batch
  @Test
  public void testReadFromRealKafka() throws Exception {
    // Create a test source event acceptor that collects received events
    TestSourceEventAcceptor eventConsumer = new TestSourceEventAcceptor();

    // Produce some test messages
    sendTestMessages(1);

    // Create and configure the KafkaSourceCommand
    KafkaSourceCommandFactory commandFactory = new KafkaSourceCommandFactory();
    KafkaSourceCommand kafkaSourceCommand =
        commandFactory.createCommand("my_node", TestUtils.JOB_CONTEXT);

    KafkaSourceDto.Config config =
        KafkaSourceDto.Config.builder()
            .broker(KAFKA_CONTAINER.getBootstrapServers())
            .topic(TOPIC_NAME)
            .encodingType(EncodingType.JSON_OBJECT)
            .groupId("test-group-" + System.currentTimeMillis())
            .properties(Map.of("auto.offset.reset", "latest"))
            .build();

    kafkaSourceCommand.parseAndValidateArg(toJsonString(config));

    // Execute the command in a separate thread with timeout
    // since Kafka source will keep polling indefinitely
    //noinspection resource
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> future =
          executor.submit(
              () -> {
                kafkaSourceCommand.execute(
                    "test_user",
                    new MetricClientProvider.NoopMetricClientProvider(),
                    eventConsumer);
                return null;
              });

      // Wait for messages to be consumed (adjust timeout as needed)
      try {
        Thread.sleep(5000); // Give the consumer time to start and connect
        sendTestMessages(2);
        future.get(5, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        // Expected - the execute method runs until explicitly stopped
      } finally {
        future.cancel(true);
      }

      // Verify that messages were consumed
      assertEquals(3, eventConsumer.getReceivedEvents().size());
      assertEquals(2, eventConsumer.getReceivedEvents().getFirst().unwrap().get("batch"));
    } finally {
      executor.shutdownNow();
      //noinspection ResultOfMethodCallIgnored
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private void sendTestMessages(int batchCount) {
    // Sample JSON messages for testing
    String[] testMessages = {
      "{\"id\":1,\"value\":\"test1\",\"timestamp\":\"2024-09-24T10:00:00Z\", \"batch\": "
          + batchCount
          + "}",
      "{\"id\":2,\"value\":\"test2\",\"timestamp\":\"2024-09-24T10:01:00Z\", \"batch\": "
          + batchCount
          + "}",
      "{\"id\":3,\"value\":\"test3\",\"timestamp\":\"2024-09-24T10:02:00Z\", \"batch\": "
          + batchCount
          + "}"
    };

    // Send messages to Kafka
    for (String message : testMessages) {
      producer.send(
          new ProducerRecord<>(TOPIC_NAME, null, message.getBytes(StandardCharsets.UTF_8)));
    }
    producer.flush();
  }

  @Getter
  public static class TestSourceEventAcceptor implements SourceEventAcceptor {
    private final List<RecordFleakData> receivedEvents =
        Collections.synchronizedList(new ArrayList<>());

    @Override
    public void terminate() {}

    @Override
    public void accept(List<RecordFleakData> recordFleakData) {
      receivedEvents.addAll(recordFleakData);
    }
  }
}
