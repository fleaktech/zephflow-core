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

import static io.fleak.zephflow.lib.utils.MiscUtils.lookupUsernamePasswordCredential;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.kafkasink.KafkaSinkDto;
import io.fleak.zephflow.lib.commands.kafkasource.KafkaSourceDto;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public final class KafkaClientProperties {
  private KafkaClientProperties() {}

  public static Properties source(KafkaSourceDto.Config config) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBroker());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getGroupId());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "5000");
    props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, "1048576");
    props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, "1000");
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
    props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "10485760");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    // Never create topics on the broker as a side effect of subscribing (KIP-361). A node that
    // relies on auto-creation can opt back in with allow.auto.create.topics=true in properties.
    props.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");

    if (config.getProperties() != null) {
      props.putAll(config.getProperties());
    }
    return props;
  }

  public static Properties sink(KafkaSinkDto.Config config, JobContext jobContext) {
    boolean storeAndForwardEnabled = config.isStoreAndForwardEnabled() && !isTestMode(jobContext);
    boolean waitForBrokerAcks =
        storeAndForwardEnabled
            || config.getDeliveryMode() != KafkaSinkDto.DeliveryMode.FIRE_AND_FORGET;
    Properties props = getProperties(config, waitForBrokerAcks);

    boolean isTestMode = isTestMode(jobContext);
    if (isTestMode) {
      props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "10000");
    }

    if (storeAndForwardEnabled) {
      props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "5000");
      props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "2000");
      props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "5000");
    }

    if (config.getProperties() != null) {
      props.putAll(config.getProperties());
    }

    if (waitForBrokerAcks) {
      enableIdempotenceIfConfigurationAllows(props);
    }

    applyCredentialSasl(props, config, jobContext);

    return props;
  }

  private static void applyCredentialSasl(
      Properties props, KafkaSinkDto.Config config, JobContext jobContext) {
    String protocol = StringUtils.trimToNull(config.getSecurityProtocol());
    if (protocol == null) {
      return;
    }
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol);
    if (!protocol.startsWith("SASL_")) {
      return;
    }
    String mechanism = StringUtils.trimToNull(config.getSaslMechanism());
    Preconditions.checkArgument(
        mechanism != null, "saslMechanism is required when securityProtocol is %s", protocol);
    props.put(SaslConfigs.SASL_MECHANISM, mechanism);
    UsernamePasswordCredential cred =
        lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
    props.put(
        SaslConfigs.SASL_JAAS_CONFIG,
        buildJaasConfig(mechanism, cred.getUsername(), cred.getPassword()));
  }

  private static String buildJaasConfig(String mechanism, String username, String password) {
    String loginModule =
        switch (mechanism) {
          case "PLAIN" -> PlainLoginModule.class.getName();
          case "SCRAM-SHA-256", "SCRAM-SHA-512" -> ScramLoginModule.class.getName();
          default -> throw new IllegalArgumentException("Unsupported SASL mechanism: " + mechanism);
        };
    return String.format(
        "%s required username=\"%s\" password=\"%s\";",
        loginModule, escapeJaas(username), escapeJaas(password));
  }

  private static String escapeJaas(String v) {
    return v.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private static Properties getProperties(KafkaSinkDto.Config config, boolean waitForBrokerAcks) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBroker());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

    props.put(ProducerConfig.BATCH_SIZE_CONFIG, "65536");
    props.put(ProducerConfig.LINGER_MS_CONFIG, "10");
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "67108864");
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
    props.put(ProducerConfig.ACKS_CONFIG, waitForBrokerAcks ? "all" : "1");
    props.put(ProducerConfig.RETRIES_CONFIG, "3");
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
    return props;
  }

  private static void enableIdempotenceIfConfigurationAllows(Properties producerProperties) {
    Object acks = producerProperties.get(ProducerConfig.ACKS_CONFIG);
    Integer retries = producerPropertyAsInteger(producerProperties, ProducerConfig.RETRIES_CONFIG);
    Integer maxInFlightRequests =
        producerPropertyAsInteger(
            producerProperties, ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
    boolean idempotenceSupported =
        ("all".equals(acks) || "-1".equals(acks))
            && retries != null
            && retries > 0
            && maxInFlightRequests != null
            && maxInFlightRequests <= 5
            && !producerProperties.containsKey(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG);
    if (idempotenceSupported) {
      producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    }
  }

  private static Integer producerPropertyAsInteger(Properties producerProperties, String key) {
    try {
      return Integer.parseInt(String.valueOf(producerProperties.get(key)).trim());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static boolean isTestMode(JobContext jobContext) {
    return jobContext != null
        && Boolean.TRUE.equals(jobContext.getOtherProperties().get(JobContext.FLAG_TEST_MODE));
  }
}
