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

import io.fleak.zephflow.lib.commands.source.CommitStrategy;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.commands.source.NoCommitStrategy;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.mqttv5.client.MqttClient;

@Slf4j
public record MqttSourceFetcher(
    MqttClient mqttClient,
    BlockingQueue<byte[]> messageQueue,
    long receiveTimeoutMs,
    int maxBatchSize)
    implements Fetcher<SerializedEvent> {

  @Override
  public List<SerializedEvent> fetch() {
    List<SerializedEvent> events = new ArrayList<>();
    try {
      byte[] firstPayload = messageQueue.poll(receiveTimeoutMs, TimeUnit.MILLISECONDS);
      if (firstPayload == null) {
        return events;
      }
      events.add(new SerializedEvent(null, firstPayload, null));

      for (int index = 1; index < maxBatchSize; index++) {
        byte[] nextPayload = messageQueue.poll();
        if (nextPayload == null) {
          break;
        }
        events.add(new SerializedEvent(null, nextPayload, null));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    log.debug("Fetched {} messages from MQTT", events.size());
    return events;
  }

  @Override
  public CommitStrategy commitStrategy() {
    return NoCommitStrategy.INSTANCE;
  }

  @Override
  public void close() {
    closeQuietly(mqttClient);
  }

  static void closeQuietly(MqttClient client) {
    if (client == null) {
      return;
    }
    try {
      if (client.isConnected()) {
        client.disconnect();
      }
    } catch (Exception e) {
      log.warn("Failed to disconnect MQTT client", e);
    }
    try {
      client.close();
    } catch (Exception e) {
      log.warn("Failed to close MQTT client", e);
    }
  }
}
