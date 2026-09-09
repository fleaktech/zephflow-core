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
package io.fleak.zephflow.lib.kafka;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers
class KafkaConnectionValidatorContractTest {
  @Container
  static final KafkaContainer ANONYMOUS = new KafkaContainer("apache/kafka-native:3.8.0");

  @Container
  static final KafkaContainer AUTHENTICATED =
      new KafkaContainer("apache/kafka-native:3.8.0")
          .withEnv(
              "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
              "BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT")
          .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
          .withEnv(
              "KAFKA_AUTHORIZER_CLASS_NAME",
              "org.apache.kafka.metadata.authorizer.StandardAuthorizer")
          .withEnv("KAFKA_SUPER_USERS", "User:ANONYMOUS")
          .withEnv("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "false")
          .withEnv(
              "KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG",
              "org.apache.kafka.common.security.plain.PlainLoginModule required user_test=\"test-password\";");

  @Test
  void emptyTopicSucceedsWithoutProducingCommittingJoiningOrCreatingTopics() throws Exception {
    Properties properties = properties(ANONYMOUS.getBootstrapServers());
    try (Admin observer = Admin.create(properties)) {
      observer
          .createTopics(List.of(new NewTopic("empty", 1, (short) 1)))
          .all()
          .get(10, TimeUnit.SECONDS);
      var topicsBefore = observer.listTopics().names().get(10, TimeUnit.SECONDS);
      var groupsBefore = observer.listConsumerGroups().all().get(10, TimeUnit.SECONDS);
      TopicPartition partition = new TopicPartition("empty", 0);
      long offsetBefore =
          observer
              .listOffsets(Map.of(partition, OffsetSpec.latest()))
              .all()
              .get(10, TimeUnit.SECONDS)
              .get(partition)
              .offset();
      properties.put("group.id", "validation-must-not-create-this-group");
      assertEquals(
          KafkaConnectionValidator.Status.SUCCESS,
          new KafkaConnectionValidator().validate(properties, Duration.ofSeconds(5)));
      assertEquals(topicsBefore, observer.listTopics().names().get(10, TimeUnit.SECONDS));
      assertEquals(groupsBefore, observer.listConsumerGroups().all().get(10, TimeUnit.SECONDS));
      assertEquals(
          offsetBefore,
          observer
              .listOffsets(Map.of(partition, OffsetSpec.latest()))
              .all()
              .get(10, TimeUnit.SECONDS)
              .get(partition)
              .offset());
      assertEquals(0, offsetBefore);
    }
  }

  @Test
  void nativeConfigProviderResolvesBrokerAndIgnoresConsumerProducerClasses(@TempDir Path directory)
      throws Exception {
    Path config = directory.resolve("kafka.properties");
    Files.writeString(config, "bootstrap=" + ANONYMOUS.getBootstrapServers());
    Properties properties = properties("${file:" + config + ":bootstrap}");
    properties.put("config.providers", "file");
    properties.put(
        "config.providers.file.class",
        "org.apache.kafka.common.config.provider.FileConfigProvider");
    properties.put("key.deserializer", "not.a.ConsumerDeserializer");
    properties.put("value.serializer", "not.a.ProducerSerializer");
    assertEquals(
        KafkaConnectionValidator.Status.SUCCESS,
        new KafkaConnectionValidator().validate(properties, Duration.ofSeconds(5)));
    assertEquals("${file:" + config + ":bootstrap}", properties.getProperty("bootstrap.servers"));
  }

  @Test
  void realAuthenticationSuccessAndFailureAreDistinct() {
    Properties properties = properties(AUTHENTICATED.getBootstrapServers());
    properties.put("security.protocol", "SASL_PLAINTEXT");
    properties.put("sasl.mechanism", "PLAIN");
    properties.put(
        "sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test-password\";");
    assertEquals(
        KafkaConnectionValidator.Status.SUCCESS,
        new KafkaConnectionValidator().validate(properties, Duration.ofSeconds(5)));
    properties.put(
        "sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"SENTINEL_PASSWORD\";");
    assertEquals(
        KafkaConnectionValidator.Status.AUTHENTICATION_FAILED,
        new KafkaConnectionValidator().validate(properties, Duration.ofSeconds(5)));
  }

  @Test
  void authenticatedPrincipalNeedsNoClusterDescribePermission() throws Exception {
    Properties properties = properties(AUTHENTICATED.getBootstrapServers());
    properties.put("security.protocol", "SASL_PLAINTEXT");
    properties.put("sasl.mechanism", "PLAIN");
    properties.put(
        "sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test-password\";");
    try (Admin observer = Admin.create(properties)) {
      var failure =
          assertThrows(
              java.util.concurrent.ExecutionException.class,
              () ->
                  observer
                      .describeAcls(org.apache.kafka.common.acl.AclBindingFilter.ANY)
                      .values()
                      .get(5, TimeUnit.SECONDS));
      assertInstanceOf(
          org.apache.kafka.common.errors.ClusterAuthorizationException.class, failure.getCause());
    }
    assertEquals(
        KafkaConnectionValidator.Status.SUCCESS,
        new KafkaConnectionValidator().validate(properties, Duration.ofSeconds(5)));
  }

  @Test
  void refusedPortAndTlsMismatchStayBounded() {
    assertTimeoutPreemptively(
        Duration.ofSeconds(5),
        () -> {
          assertEquals(
              KafkaConnectionValidator.Status.UNAVAILABLE,
              new KafkaConnectionValidator()
                  .validate(properties("127.0.0.1:1"), Duration.ofMillis(500)));
          Properties tls = properties(ANONYMOUS.getBootstrapServers());
          tls.put("security.protocol", "SSL");
          var status = new KafkaConnectionValidator().validate(tls, Duration.ofMillis(500));
          assertTrue(
              status == KafkaConnectionValidator.Status.TLS_FAILED
                  || status == KafkaConnectionValidator.Status.UNAVAILABLE);
        });
  }

  private static Properties properties(String bootstrap) {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", bootstrap);
    return properties;
  }
}
