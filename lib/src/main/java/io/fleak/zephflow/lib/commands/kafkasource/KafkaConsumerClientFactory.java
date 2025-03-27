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

import io.fleak.zephflow.lib.kafka.KafkaHealthMonitor;
import io.fleak.zephflow.lib.kafka.ScheduledKafkaHealthMonitor;
import java.io.Serializable;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/** Created by bolei on 9/24/24 Created for unit test purpose */
@Slf4j
public class KafkaConsumerClientFactory implements Serializable {
  public KafkaConsumer<byte[], byte[]> createKafkaConsumer(Properties consumerProps) {
    return new KafkaConsumer<>(consumerProps);
  }

  public KafkaHealthMonitor createAndStartHealthMonitor(Properties consumerProps) {
    return createAndStartHealthMonitor(
        consumerProps,
        10000,
        (e) ->
            log.error(
                "KafkaConsumer {} health check failed}",
                consumerProps.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG),
                e));
  }

  public KafkaHealthMonitor createAndStartHealthMonitor(
      Properties consumerProps,
      long checkIntervalMillis,
      ScheduledKafkaHealthMonitor.HealthListener listener) {
    var adminClient = AdminClient.create(consumerProps);
    var monitor = new ScheduledKafkaHealthMonitor(adminClient, checkIntervalMillis, listener);

    monitor.start();
    return monitor;
  }
}
