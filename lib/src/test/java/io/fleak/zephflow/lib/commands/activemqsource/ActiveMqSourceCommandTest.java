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
package io.fleak.zephflow.lib.commands.activemqsource;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.serdes.EncodingType;
import jakarta.jms.*;
import java.util.*;
import java.util.concurrent.*;
import lombok.Getter;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
public class ActiveMqSourceCommandTest {

  private static final int ACTIVEMQ_PORT = 61616;

  @Container
  private static final GenericContainer<?> ACTIVEMQ_CONTAINER =
      new GenericContainer<>("apache/activemq-classic:5.18.6").withExposedPorts(ACTIVEMQ_PORT);

  private static String brokerUrl;

  @BeforeAll
  static void setup() {
    brokerUrl =
        "tcp://"
            + ACTIVEMQ_CONTAINER.getHost()
            + ":"
            + ACTIVEMQ_CONTAINER.getMappedPort(ACTIVEMQ_PORT);
  }

  @AfterAll
  static void tearDown() {
    if (ACTIVEMQ_CONTAINER.isRunning()) {
      ACTIVEMQ_CONTAINER.stop();
    }
  }

  // Send 3 JSON messages to a queue before starting the consumer.
  // Verify the consumer reads all 3 messages with correct content.
  // Queue messages are persisted, so the consumer sees them even though it starts after send.
  @Test
  public void testReadFromQueue() throws Exception {
    String queueName = "test-queue-" + System.currentTimeMillis();

    sendMessages(queueName, false);

    TestSourceEventAcceptor eventAcceptor = new TestSourceEventAcceptor();

    ActiveMqSourceCommandFactory factory = new ActiveMqSourceCommandFactory();
    ActiveMqSourceCommand command = factory.createCommand("my_node", TestUtils.JOB_CONTEXT);

    ActiveMqSourceDto.Config config =
        ActiveMqSourceDto.Config.builder()
            .brokerUrl(brokerUrl)
            .brokerType(ActiveMqSourceDto.BrokerType.CLASSIC)
            .destination(queueName)
            .destinationType(ActiveMqSourceDto.DestinationType.QUEUE)
            .encodingType(EncodingType.JSON_OBJECT)
            .build();

    command.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> future =
          executor.submit(
              () -> {
                command.initialize(new MetricClientProvider.NoopMetricClientProvider());
                command.execute("test_user", eventAcceptor);
                return null;
              });

      try {
        future.get(10, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        // Expected
      } finally {
        future.cancel(true);
      }

      List<RecordFleakData> events = eventAcceptor.getReceivedEvents();
      assertEquals(3, events.size());
      assertEquals(1L, events.get(0).unwrap().get("id"));
      assertEquals("test1", events.get(0).unwrap().get("value"));
      assertEquals(2L, events.get(1).unwrap().get("id"));
      assertEquals(3L, events.get(2).unwrap().get("id"));
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  // Start a durable topic subscriber first, then send 3 messages to the topic.
  // Verify the subscriber receives all 3 messages.
  // Messages must be sent after subscription is established, since topics only deliver
  // to active subscribers (unlike queues which persist unconsumed messages).
  @Test
  public void testReadFromTopic() throws Exception {
    String topicName = "test-topic-" + System.currentTimeMillis();
    String clientId = "test-client-" + System.currentTimeMillis();
    String subscriptionName = "test-sub";

    // First create the durable subscription by starting the consumer
    TestSourceEventAcceptor eventAcceptor = new TestSourceEventAcceptor();

    ActiveMqSourceCommandFactory factory = new ActiveMqSourceCommandFactory();
    ActiveMqSourceCommand command = factory.createCommand("my_node", TestUtils.JOB_CONTEXT);

    ActiveMqSourceDto.Config config =
        ActiveMqSourceDto.Config.builder()
            .brokerUrl(brokerUrl)
            .brokerType(ActiveMqSourceDto.BrokerType.CLASSIC)
            .destination(topicName)
            .destinationType(ActiveMqSourceDto.DestinationType.TOPIC)
            .encodingType(EncodingType.JSON_OBJECT)
            .clientId(clientId)
            .subscriptionName(subscriptionName)
            .build();

    command.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> future =
          executor.submit(
              () -> {
                command.initialize(new MetricClientProvider.NoopMetricClientProvider());
                command.execute("test_user", eventAcceptor);
                return null;
              });

      // Give consumer time to start and subscribe
      Thread.sleep(3000);

      // Now send messages to the topic
      sendMessages(topicName, true);

      try {
        future.get(10, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        // Expected
      } finally {
        future.cancel(true);
      }

      assertEquals(3, eventAcceptor.getReceivedEvents().size());
    } finally {
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  private void sendMessages(String destinationName, boolean isTopic) throws Exception {
    ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(brokerUrl);
    try (Connection connection = cf.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
      connection.start();

      Destination destination =
          isTopic ? session.createTopic(destinationName) : session.createQueue(destinationName);
      MessageProducer producer = session.createProducer(destination);

      String[] messages = {
        "{\"id\":1,\"value\":\"test1\"}",
        "{\"id\":2,\"value\":\"test2\"}",
        "{\"id\":3,\"value\":\"test3\"}"
      };

      for (String msg : messages) {
        producer.send(session.createTextMessage(msg));
      }
      producer.close();
    }
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
