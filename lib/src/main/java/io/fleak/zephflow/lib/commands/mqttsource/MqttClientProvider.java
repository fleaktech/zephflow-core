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

import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import java.nio.charset.StandardCharsets;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptionsBuilder;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;

public class MqttClientProvider {

  public MqttClient createClient(MqttSourceDto.Config config) throws MqttException {
    return new MqttClient(config.getBrokerUrl(), config.getClientId(), new MemoryPersistence());
  }

  public MqttConnectionOptions buildConnectionOptions(
      MqttSourceDto.Config config, UsernamePasswordCredential credential) {
    MqttConnectionOptionsBuilder optionsBuilder =
        new MqttConnectionOptionsBuilder().cleanStart(config.isCleanStart());

    if (config.getSessionExpiryIntervalSeconds() != null) {
      optionsBuilder.sessionExpiryInterval(config.getSessionExpiryIntervalSeconds());
    }

    if (credential != null) {
      optionsBuilder.username(credential.getUsername());
      optionsBuilder.password(credential.getPassword().getBytes(StandardCharsets.UTF_8));
    }

    return optionsBuilder.build();
  }
}
