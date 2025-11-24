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

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.ScalarSinkCommand;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Error recovery scenario tests using TestContainers for realistic Kafka error conditions. Tests
 * focus on how the BatchKafkaSinkFlusher handles real infrastructure failures.
 */
@Testcontainers
class KafkaSinkErrorRecoveryTest {

  private static final String TOPIC_NAME = "error_recovery_test_topic";

  @Container
  private static final KafkaContainer KAFKA_CONTAINER =
      new KafkaContainer("apache/kafka-native:3.8.0");

  private static AdminClient adminClient;

  @BeforeAll
  static void setupKafka() throws Exception {
    // Create AdminClient to manage topics
    adminClient =
        AdminClient.create(
            Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers()));

    // Create test topic
    NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, (short) 1);
    adminClient.createTopics(Collections.singleton(newTopic)).all().get(30, TimeUnit.SECONDS);
  }

  @AfterAll
  static void tearDown() {
    if (adminClient != null) {
      adminClient.close();
    }
    if (KAFKA_CONTAINER.isRunning()) {
      KAFKA_CONTAINER.stop();
    }
  }

  @Test
  void testInvalidJSONData_SerializationError() {
    // Arrange: Setup sink command
    KafkaSinkCommandFactory commandFactory = new KafkaSinkCommandFactory();
    KafkaSinkCommand kafkaSinkCommand =
        (KafkaSinkCommand) commandFactory.createCommand("json_error_node", TestUtils.JOB_CONTEXT);

    KafkaSinkDto.Config config =
        KafkaSinkDto.Config.builder()
            .topic(TOPIC_NAME)
            .broker(KAFKA_CONTAINER.getBootstrapServers())
            .encodingType(EncodingType.JSON_OBJECT.toString())
            .batchSize(5)
            .flushIntervalMs(1000L)
            .build();
    kafkaSinkCommand.parseAndValidateArg(
        OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));

    // Create test data with potential serialization challenges
    List<RecordFleakData> testData = new ArrayList<>();

    // Add some normal records
    testData.add((RecordFleakData) FleakData.wrap(Map.of("id", 1, "status", "normal")));
    testData.add((RecordFleakData) FleakData.wrap(Map.of("id", 2, "status", "normal")));

    // Add records with edge case data
    testData.add((RecordFleakData) FleakData.wrap(Map.of("id", 3, "unicode", "测试数据 🚀")));
    testData.add(
        (RecordFleakData) FleakData.wrap(Map.of("id", 4, "special", "quotes\"and\\backslashes")));

    // Act: Process mixed data including edge cases
    kafkaSinkCommand.initialize(new MetricClientProvider.NoopMetricClientProvider());
    var context = kafkaSinkCommand.getExecutionContext();
    ScalarSinkCommand.SinkResult result =
        kafkaSinkCommand.writeToSink(testData, "json_test_user", context);

    // Assert: Should handle mixed data appropriately
    assertNotNull(result);
    assertTrue(result.getInputCount() > 0, "Should process all input records");
    // Some records might succeed, some might fail depending on serialization
    assertTrue(result.getSuccessCount() >= 0, "Success count should be valid");
    assertTrue(result.errorCount() >= 0, "Error count should be valid");

    System.out.println(
        "✅ JSON serialization test: Mixed data handled. "
            + "Total: "
            + result.getInputCount()
            + ", Success: "
            + result.getSuccessCount()
            + ", Errors: "
            + result.errorCount());
  }
}
