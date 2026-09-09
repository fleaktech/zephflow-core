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
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

/** Created by bolei on 9/24/24 Created for unit test purpose */
@Slf4j
public class KafkaConsumerClientFactory implements Serializable {
  private static final long TOPIC_CHECK_TIMEOUT_MS = 30_000;
  private static final Duration ADMIN_CLOSE_TIMEOUT = Duration.ofSeconds(5);

  public KafkaConsumer<byte[], byte[]> createKafkaConsumer(Properties consumerProps) {
    return new KafkaConsumer<>(consumerProps);
  }

  /**
   * Fails when the topic does not exist on the broker, or when the principal is not allowed to
   * describe it. describeTopics never triggers broker-side auto-creation, so this is safe to call
   * before subscribing. Connectivity problems are only logged: the consumer keeps its existing
   * retry behaviour for those.
   *
   * <p>An authorization failure is treated like a missing topic on purpose: Kafka answers
   * TOPIC_AUTHORIZATION_FAILED for any topic the principal cannot describe instead of revealing
   * whether it exists, and every principal that can read a topic can also describe it, so a denial
   * here means the consumer could not have read the topic either.
   *
   * @throws IllegalArgumentException if the broker reports the topic as unknown or unauthorized
   */
  public void verifyTopicExists(Properties consumerProps, String topic) {
    Object bootstrapServers = consumerProps.get(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG);
    AdminClient adminClient = AdminClient.create(consumerProps);
    try {
      verifyTopicExists(adminClient, bootstrapServers, topic);
    } finally {
      adminClient.close(ADMIN_CLOSE_TIMEOUT);
    }
  }

  static void verifyTopicExists(AdminClient adminClient, Object bootstrapServers, String topic) {
    try {
      adminClient
          .describeTopics(List.of(topic))
          .topicNameValues()
          .get(topic)
          .get(TOPIC_CHECK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof UnknownTopicOrPartitionException) {
        throw new IllegalArgumentException(
            String.format("Kafka topic '%s' was not found on broker %s", topic, bootstrapServers),
            cause);
      }
      if (cause instanceof TopicAuthorizationException) {
        throw new IllegalArgumentException(
            String.format(
                "Not authorized to describe Kafka topic '%s' on broker %s; a consumer that can"
                    + " read a topic can also describe it, so check the topic name and the"
                    + " principal's permissions",
                topic, bootstrapServers),
            cause);
      }
      log.warn(
          "Could not verify Kafka topic {} on broker {}; continuing", topic, bootstrapServers, e);
    } catch (TimeoutException e) {
      log.warn(
          "Timed out verifying Kafka topic {} on broker {}; continuing", topic, bootstrapServers);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while verifying Kafka topic " + topic, e);
    }
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
