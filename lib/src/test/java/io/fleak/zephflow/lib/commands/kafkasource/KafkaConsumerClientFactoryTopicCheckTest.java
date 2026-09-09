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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.junit.jupiter.api.Test;

/** How the topic check interprets the broker's describeTopics answer. */
class KafkaConsumerClientFactoryTopicCheckTest {

  private static final String TOPIC = "orders";
  private static final String BROKER = "broker:9092";

  @Test
  void unknownTopicFailsWithTheTopicName() {
    var cause = new UnknownTopicOrPartitionException("Topic orders not found.");
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> KafkaConsumerClientFactory.verifyTopicExists(failing(cause), BROKER, TOPIC));
    assertTrue(
        e.getMessage().contains("'orders' was not found on broker broker:9092"), e.getMessage());
    assertSame(cause, e.getCause());
  }

  // Brokers answer TOPIC_AUTHORIZATION_FAILED for any topic the principal cannot describe, whether
  // or not it exists, so a mistyped topic on an ACL-protected cluster arrives here.
  @Test
  void unauthorizedTopicFailsLikeAMissingOne() {
    var cause = new TopicAuthorizationException(Set.of(TOPIC));
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () -> KafkaConsumerClientFactory.verifyTopicExists(failing(cause), BROKER, TOPIC));
    assertTrue(
        e.getMessage().contains("Not authorized to describe Kafka topic 'orders'"), e.getMessage());
    assertTrue(e.getMessage().contains("topic name"), e.getMessage());
    assertSame(cause, e.getCause());
  }

  // Anything else (broker unreachable, request timed out, ...) keeps the consumer's existing
  // connection-retry behaviour instead of failing the job.
  @Test
  void otherFailuresAreLoggedAndIgnored() {
    var cause = new org.apache.kafka.common.errors.TimeoutException("no response");
    assertDoesNotThrow(
        () -> KafkaConsumerClientFactory.verifyTopicExists(failing(cause), BROKER, TOPIC));
  }

  @Test
  void existingTopicPasses() {
    KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
    future.complete(new TopicDescription(TOPIC, false, List.of()));
    assertDoesNotThrow(
        () -> KafkaConsumerClientFactory.verifyTopicExists(describing(future), BROKER, TOPIC));
  }

  private static AdminClient failing(Throwable cause) {
    KafkaFutureImpl<TopicDescription> future = new KafkaFutureImpl<>();
    future.completeExceptionally(cause);
    return describing(future);
  }

  private static AdminClient describing(KafkaFuture<TopicDescription> future) {
    DescribeTopicsResult result = mock();
    when(result.topicNameValues()).thenReturn(Map.of(TOPIC, future));
    AdminClient adminClient = mock();
    when(adminClient.describeTopics(anyCollection())).thenReturn(result);
    return adminClient;
  }
}
