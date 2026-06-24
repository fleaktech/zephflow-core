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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.nio.charset.StandardCharsets;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.junit.jupiter.api.Test;

public class MqttClientProviderTest {

  private static MqttSourceDto.Config baseConfig() {
    return MqttSourceDto.Config.builder()
        .brokerUrl("tcp://localhost:1883")
        .topicFilter("sensors/#")
        .clientId("test-client")
        .encodingType(EncodingType.JSON_OBJECT)
        .build();
  }

  @Test
  void testBuildConnectionOptions_withoutCredential_leavesAuthUnset() {
    MqttClientProvider mqttClientProvider = new MqttClientProvider();
    MqttSourceDto.Config config = baseConfig();

    MqttConnectionOptions options = mqttClientProvider.buildConnectionOptions(config, null);

    assertNull(options.getUserName());
    assertNull(options.getPassword());
    assertTrue(options.isCleanStart());
  }

  @Test
  void testBuildConnectionOptions_withCredential_setsUsernameAndUtf8Password() {
    MqttClientProvider mqttClientProvider = new MqttClientProvider();
    MqttSourceDto.Config config = baseConfig();
    UsernamePasswordCredential credential =
        new UsernamePasswordCredential("mqtt-user", "secret-pass");

    MqttConnectionOptions options = mqttClientProvider.buildConnectionOptions(config, credential);

    assertEquals("mqtt-user", options.getUserName());
    assertArrayEquals("secret-pass".getBytes(StandardCharsets.UTF_8), options.getPassword());
  }

  @Test
  void testBuildConnectionOptions_withSessionExpiry_setsInterval() {
    MqttClientProvider mqttClientProvider = new MqttClientProvider();
    MqttSourceDto.Config config =
        MqttSourceDto.Config.builder()
            .brokerUrl("tcp://localhost:1883")
            .topicFilter("sensors/#")
            .clientId("test-client")
            .encodingType(EncodingType.JSON_OBJECT)
            .sessionExpiryIntervalSeconds(120L)
            .build();

    MqttConnectionOptions options = mqttClientProvider.buildConnectionOptions(config, null);

    assertEquals(120L, options.getSessionExpiryInterval());
  }

  @Test
  void testBuildConnectionOptions_withoutSessionExpiry_leavesIntervalUnset() {
    MqttClientProvider mqttClientProvider = new MqttClientProvider();
    MqttSourceDto.Config config = baseConfig();

    MqttConnectionOptions options = mqttClientProvider.buildConnectionOptions(config, null);

    assertNull(options.getSessionExpiryInterval());
  }

  @Test
  void testBuildConnectionOptions_automaticReconnect_reflectsConfigDefault() {
    MqttClientProvider mqttClientProvider = new MqttClientProvider();
    MqttSourceDto.Config config = baseConfig();

    MqttConnectionOptions options = mqttClientProvider.buildConnectionOptions(config, null);

    assertTrue(options.isAutomaticReconnect());
  }
}
