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

import static io.fleak.zephflow.lib.utils.JsonUtils.*;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_TAG_ENV;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_TAG_SERVICE;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.ScramCredentialInfo;
import org.apache.kafka.clients.admin.ScramMechanism;
import org.apache.kafka.clients.admin.UserScramCredentialUpsertion;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Integration test for SASL auth on the Kafka sink against a <b>real broker</b>. Uses a {@code
 * SASL_PLAINTEXT} + {@code PLAIN} listener (no TLS certs, no SCRAM provisioning) so the test is
 * deterministic. The broker-side JAAS declares a fixed user/pass; the same user/pass is provided to
 * the sink as a {@link UsernamePasswordCredential} keyed by {@code credentialId}, and the engine
 * assembles the SASL config in memory at runtime. Covers {@code PLAIN} and {@code
 * SCRAM-SHA-256}/{@code SCRAM-SHA-512} on the same listener; full {@code SASL_SSL} (TLS) is
 * deferred to a live-stack phase. Tagged {@code integration}; requires Docker.
 */
@Tag("integration")
@Testcontainers
class KafkaSinkSaslIntegrationTest {

  private static final String TOPIC = "sasl_topic";
  private static final String TOPIC_NEG = "sasl_topic_neg";
  private static final String TOPIC_SCRAM_512 = "scram_topic_512";
  private static final String TOPIC_SCRAM_256 = "scram_topic_256";
  private static final String USERNAME = "kafkauser";
  private static final String PASSWORD = "kafkapass";
  private static final String CREDENTIAL_ID = "kafka_sasl_cred";

  private static final String BROKER_JAAS =
      "org.apache.kafka.common.security.plain.PlainLoginModule required "
          + "username=\""
          + USERNAME
          + "\" password=\""
          + PASSWORD
          + "\" user_"
          + USERNAME
          + "=\""
          + PASSWORD
          + "\";";

  @Container
  private static final KafkaContainer KAFKA =
      new KafkaContainer("apache/kafka-native:3.8.0")
          .withEnv(
              "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
              "BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT")
          .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN,SCRAM-SHA-256,SCRAM-SHA-512")
          .withEnv(
              "KAFKA_LISTENER_NAME_PLAINTEXT_SASL_ENABLED_MECHANISMS",
              "PLAIN,SCRAM-SHA-256,SCRAM-SHA-512")
          .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG", BROKER_JAAS)
          .withEnv(
              "KAFKA_LISTENER_NAME_PLAINTEXT_SCRAM___SHA___512_SASL_JAAS_CONFIG",
              "org.apache.kafka.common.security.scram.ScramLoginModule required;")
          .withEnv(
              "KAFKA_LISTENER_NAME_PLAINTEXT_SCRAM___SHA___256_SASL_JAAS_CONFIG",
              "org.apache.kafka.common.security.scram.ScramLoginModule required;");

  @BeforeAll
  static void setup() throws Exception {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.putAll(clientSaslProps(USERNAME, PASSWORD));
    try (AdminClient admin = AdminClient.create(props)) {
      admin
          .createTopics(
              List.of(
                  new NewTopic(TOPIC, 1, (short) 1),
                  new NewTopic(TOPIC_NEG, 1, (short) 1),
                  new NewTopic(TOPIC_SCRAM_512, 1, (short) 1),
                  new NewTopic(TOPIC_SCRAM_256, 1, (short) 1)))
          .all()
          .get(30, TimeUnit.SECONDS);
      admin
          .alterUserScramCredentials(
              List.of(
                  new UserScramCredentialUpsertion(
                      USERNAME,
                      new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_512, 4096),
                      PASSWORD)))
          .all()
          .get(30, TimeUnit.SECONDS);
      admin
          .alterUserScramCredentials(
              List.of(
                  new UserScramCredentialUpsertion(
                      USERNAME,
                      new ScramCredentialInfo(ScramMechanism.SCRAM_SHA_256, 4096),
                      PASSWORD)))
          .all()
          .get(30, TimeUnit.SECONDS);
    }
  }

  @AfterAll
  static void stop() {
    if (KAFKA.isRunning()) {
      KAFKA.stop();
    }
  }

  @Test
  void writeToSink_authenticatesAndDelivers() throws Exception {
    KafkaSinkCommand sink =
        (KafkaSinkCommand)
            new KafkaSinkCommandFactory().createCommand("sasl-node", jobContext(PASSWORD));
    KafkaSinkDto.Config config =
        KafkaSinkDto.Config.builder()
            .topic(TOPIC)
            .broker(KAFKA.getBootstrapServers())
            .encodingType(EncodingType.JSON_OBJECT.toString())
            .securityProtocol("SASL_PLAINTEXT")
            .saslMechanism("PLAIN")
            .credentialId(CREDENTIAL_ID)
            .build();
    sink.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));
    sink.initialize(new MetricClientProvider.NoopMetricClientProvider());

    var ctx = sink.getExecutionContext();
    sink.writeToSink(records(1, 10), "user", ctx);

    List<Integer> ids =
        drain(TOPIC, "sasl-consumer-" + System.nanoTime(), Duration.ofSeconds(20), 10);
    assertEquals(10, new HashSet<>(ids).size(), "all SASL-authenticated records delivered");

    sink.terminate();
  }

  @Test
  void wrongPassword_failsAuthentication() throws Exception {
    KafkaSinkCommand sink =
        (KafkaSinkCommand)
            new KafkaSinkCommandFactory().createCommand("sasl-bad-node", jobContext("wrong-pass"));
    KafkaSinkDto.Config config =
        KafkaSinkDto.Config.builder()
            .topic(TOPIC_NEG)
            .broker(KAFKA.getBootstrapServers())
            .encodingType(EncodingType.JSON_OBJECT.toString())
            .securityProtocol("SASL_PLAINTEXT")
            .saslMechanism("PLAIN")
            .credentialId(CREDENTIAL_ID)
            .build();
    sink.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));
    sink.initialize(new MetricClientProvider.NoopMetricClientProvider());

    var ctx = sink.getExecutionContext();
    sink.writeToSink(records(100, 105), "user", ctx);

    List<Integer> ids =
        drain(TOPIC_NEG, "sasl-bad-consumer-" + System.nanoTime(), Duration.ofSeconds(10), 1);
    assertTrue(ids.isEmpty(), "no records should be delivered with a wrong password");

    sink.terminate();
  }

  @Test
  void writeToSink_scramSha512_authenticatesAndDelivers() throws Exception {
    assertScramDelivers("SCRAM-SHA-512", TOPIC_SCRAM_512);
  }

  @Test
  void writeToSink_scramSha256_authenticatesAndDelivers() throws Exception {
    assertScramDelivers("SCRAM-SHA-256", TOPIC_SCRAM_256);
  }

  // ===== helpers =====

  private void assertScramDelivers(String mechanism, String topic) throws Exception {
    KafkaSinkCommand sink =
        (KafkaSinkCommand)
            new KafkaSinkCommandFactory().createCommand("scram-node", jobContext(PASSWORD));
    KafkaSinkDto.Config config =
        KafkaSinkDto.Config.builder()
            .topic(topic)
            .broker(KAFKA.getBootstrapServers())
            .encodingType(EncodingType.JSON_OBJECT.toString())
            .securityProtocol("SASL_PLAINTEXT")
            .saslMechanism(mechanism)
            .credentialId(CREDENTIAL_ID)
            .build();
    sink.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));
    sink.initialize(new MetricClientProvider.NoopMetricClientProvider());

    var ctx = sink.getExecutionContext();
    sink.writeToSink(records(1, 10), "user", ctx);

    List<Integer> ids =
        drain(topic, "scram-consumer-" + System.nanoTime(), Duration.ofSeconds(20), 10);
    assertEquals(10, new HashSet<>(ids).size(), mechanism + "-authenticated records delivered");

    sink.terminate();
  }

  private static Map<String, Object> clientSaslProps(String username, String password) {
    Map<String, Object> props = new HashMap<>();
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
    props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
    props.put(
        SaslConfigs.SASL_JAAS_CONFIG,
        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\""
            + username
            + "\" password=\""
            + password
            + "\";");
    return props;
  }

  private static List<RecordFleakData> records(int fromInclusive, int toInclusive) {
    List<RecordFleakData> out = new ArrayList<>();
    for (int id = fromInclusive; id <= toInclusive; id++) {
      out.add((RecordFleakData) FleakData.wrap(Map.of("id", id)));
    }
    return out;
  }

  private List<Integer> drain(String topic, String group, Duration overall, int expected) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.putAll(clientSaslProps(USERNAME, PASSWORD));
    List<Integer> ids = new ArrayList<>();
    try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props)) {
      consumer.subscribe(Collections.singletonList(topic));
      long deadline = System.nanoTime() + overall.toNanos();
      while (System.nanoTime() < deadline && new HashSet<>(ids).size() < expected) {
        ConsumerRecords<byte[], byte[]> polled = consumer.poll(Duration.ofMillis(500));
        StreamSupport.stream(polled.spliterator(), false)
            .map(
                r -> fromJsonString(new String(r.value()), new TypeReference<RecordFleakData>() {}))
            .map(rec -> ((Number) rec.unwrap().get("id")).intValue())
            .forEach(ids::add);
      }
    }
    return ids;
  }

  private static JobContext jobContext(String password) {
    Map<String, Serializable> other = new HashMap<>();
    other.put(
        CREDENTIAL_ID,
        OBJECT_MAPPER.convertValue(
            new UsernamePasswordCredential(USERNAME, password), new TypeReference<>() {}));
    return JobContext.builder()
        .metricTags(Map.of(METRIC_TAG_SERVICE, "test_service", METRIC_TAG_ENV, "test_env"))
        .otherProperties(other)
        .build();
  }
}
