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

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.commands.JsonConfigParser;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/** Consumer-property defaults and the topic existence check, without a real broker. */
class KafkaSourceCommandTopicCheckTest {

  private static final String TOPIC = "my_topic";

  private final KafkaConsumerClientFactory factory = mock();

  @Test
  void autoCreateIsOffByDefaultAndTopicIsVerified() {
    when(factory.createKafkaConsumer(any())).thenReturn(mock());
    when(factory.createAndStartHealthMonitor(any())).thenReturn(mock());

    initialize(createCommand(null));

    ArgumentCaptor<Properties> props = ArgumentCaptor.forClass(Properties.class);
    verify(factory).verifyTopicExists(props.capture(), eq(TOPIC));
    assertEquals(
        "false", props.getValue().getProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG));
    verify(factory).createKafkaConsumer(props.getValue());
  }

  @Test
  void explicitOptInSkipsTheCheckAndKeepsAutoCreateOn() {
    when(factory.createKafkaConsumer(any())).thenReturn(mock());
    when(factory.createAndStartHealthMonitor(any())).thenReturn(mock());

    // Surrounding whitespace is what a hand-written config may contain; Kafka trims it, so the
    // gate must treat it as opted in too.
    initialize(createCommand(Map.of(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, " true ")));

    verify(factory, never()).verifyTopicExists(any(), any());
    ArgumentCaptor<Properties> props = ArgumentCaptor.forClass(Properties.class);
    verify(factory).createKafkaConsumer(props.capture());
    assertEquals(
        " true ", props.getValue().getProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG));
  }

  @Test
  void missingTopicAbortsBeforeAnyConsumerIsCreated() {
    doThrow(new IllegalArgumentException("topic not found"))
        .when(factory)
        .verifyTopicExists(any(), eq(TOPIC));

    KafkaSourceCommand command = createCommand(null);
    assertThrows(IllegalArgumentException.class, () -> initialize(command));

    verify(factory, never()).createKafkaConsumer(any());
    verify(factory, never()).createAndStartHealthMonitor(any());
  }

  private KafkaSourceCommand createCommand(Map<String, String> properties) {
    KafkaSourceCommand command =
        new KafkaSourceCommand(
            "my_node",
            TestUtils.JOB_CONTEXT,
            new JsonConfigParser<>(KafkaSourceDto.Config.class),
            new KafkaSourceConfigValidator(),
            factory);
    KafkaSourceDto.Config config =
        KafkaSourceDto.Config.builder()
            .broker("localhost:9092")
            .topic(TOPIC)
            .groupId("test-group")
            .encodingType(EncodingType.JSON_OBJECT)
            .properties(properties)
            .build();
    command.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));
    return command;
  }

  private static void initialize(KafkaSourceCommand command) {
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
  }
}
