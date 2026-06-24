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
package io.fleak.zephflow.lib.commands.mqttsource;

import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_MQTT_SOURCE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.SourceCommand;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.junit.jupiter.api.Test;

class MqttSourceCommandTest {

  @Test
  void testCommandMetadata() {
    MqttSourceCommandFactory factory = new MqttSourceCommandFactory();
    MqttSourceCommand command = factory.createCommand("my_node", TestUtils.JOB_CONTEXT);

    assertEquals(COMMAND_NAME_MQTT_SOURCE, command.commandName());
    assertEquals(SourceCommand.SourceType.STREAMING, command.sourceType());
  }

  @Test
  void testCreateExecutionContext_wiresFetcherAndConverter() throws Exception {
    MqttClientProvider mockProvider = mock(MqttClientProvider.class);
    MqttClient mockClient = mock(MqttClient.class);
    when(mockProvider.createClient(any())).thenReturn(mockClient);
    when(mockProvider.buildConnectionOptions(any(), any())).thenReturn(new MqttConnectionOptions());

    MqttSourceConfigValidator validator = new MqttSourceConfigValidator();
    MqttSourceCommand command =
        new MqttSourceCommand(
            "my_node",
            TestUtils.JOB_CONTEXT,
            new io.fleak.zephflow.lib.commands.JsonConfigParser<>(MqttSourceDto.Config.class),
            validator,
            mockProvider);

    MqttSourceDto.Config config =
        MqttSourceDto.Config.builder()
            .brokerUrl("tcp://localhost:1883")
            .topicFilter("sensors/#")
            .clientId("my-client")
            .encodingType(EncodingType.JSON_OBJECT)
            .build();

    SourceExecutionContext<SerializedEvent> context =
        (SourceExecutionContext<SerializedEvent>)
            command.createExecutionContext(
                new MetricClientProvider.NoopMetricClientProvider(),
                TestUtils.JOB_CONTEXT,
                config,
                "my_node");

    assertNotNull(context.fetcher());
    assertInstanceOf(MqttSourceFetcher.class, context.fetcher());
    assertNotNull(context.converter());
    assertNotNull(context.encoder());

    verify(mockClient).connect(any(MqttConnectionOptions.class));
    verify(mockClient).subscribe("sensors/#", 1);
    verify(mockClient).setCallback(any());
  }
}
