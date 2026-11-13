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

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.api.ConfigurationValidationException.at;
import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.ConfigurationIssue;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.lib.commands.sink.SinkEncoding;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig.Mode;
import io.fleak.zephflow.lib.serdes.encode.WriterCapability;
import io.fleak.zephflow.lib.serdes.encode.WriterCapability.Unit;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;

public class KafkaSinkConfigValidator implements ConfigValidator {

  public static final WriterCapability CAPABILITY =
      new WriterCapability(
          COMMAND_NAME_KAFKA_SINK,
          Unit.ONE_RECORD,
          Set.of("csv", "json", "kv"),
          Set.of(Mode.OBJECT, Mode.ARRAY, Mode.LINES),
          false,
          Set.of());

  private static final Set<String> SUPPORTED_SASL_MECHANISMS =
      Set.of("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512");

  private static final Set<String> MANAGED_PROPERTY_KEYS =
      Set.of(
          SaslConfigs.SASL_JAAS_CONFIG,
          SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG,
          SslConfigs.SSL_KEY_PASSWORD_CONFIG,
          SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
          CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
          SaslConfigs.SASL_MECHANISM);

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    KafkaSinkDto.Config config = (KafkaSinkDto.Config) commandConfig;

    if (StringUtils.isBlank(config.getBroker())) {
      throw at(
          of("broker"),
          new IllegalArgumentException("Broker must be provided"),
          "broker is required and must not be blank.");
    }
    if (StringUtils.isBlank(config.getTopic())) {
      throw at(
          of("topic"),
          new IllegalArgumentException("Topic must be provided"),
          "topic is required and must not be blank.");
    }
    SinkEncoding.validate(config.getEncoding(), CAPABILITY);
    if (StringUtils.isNotBlank(config.getPartitionKeyFieldExpressionStr())) {
      PathExpression.fromString(
          config.getPartitionKeyFieldExpressionStr(), of("partitionKeyFieldExpressionStr"));
    }
    if (config.isStoreAndForwardEnabled()) {
      if (config.getDeliveryMode() == KafkaSinkDto.DeliveryMode.FIRE_AND_FORGET) {
        var original =
            new IllegalArgumentException(
                "storeAndForwardEnabled requires waiting for broker acks and cannot be combined with"
                    + " deliveryMode FIRE_AND_FORGET");
        throw new ConfigurationValidationException(
            original,
            List.of("storeAndForwardEnabled", "deliveryMode").stream()
                .map(
                    field ->
                        new ConfigurationIssue(
                            of(field),
                            "storeAndForwardEnabled true cannot be combined with deliveryMode FIRE_AND_FORGET; broker acknowledgements are required."))
                .toList());
      }
      if (config.getLocalStoreMaxBytes() <= 0) {
        throw at(
            of("localStoreMaxBytes"),
            new IllegalArgumentException(
                "localStoreMaxBytes must be positive when storeAndForwardEnabled. Got: "
                    + config.getLocalStoreMaxBytes()),
            "localStoreMaxBytes must be greater than 0 when storeAndForwardEnabled is true.");
      }
      if (config.getForwardRetryIntervalMillis() <= 0) {
        throw at(
            of("forwardRetryIntervalMillis"),
            new IllegalArgumentException(
                "forwardRetryIntervalMillis must be positive when storeAndForwardEnabled. Got: "
                    + config.getForwardRetryIntervalMillis()),
            "forwardRetryIntervalMillis must be greater than 0 when storeAndForwardEnabled is true.");
      }
    }

    validateAssetManagedSecurity(config, jobContext);
  }

  private void validateAssetManagedSecurity(KafkaSinkDto.Config config, JobContext jobContext) {
    String protocol = StringUtils.trimToNull(config.getSecurityProtocol());
    if (protocol == null) {
      return;
    }
    try {
      parseEnum(SecurityProtocol.class, protocol);
    } catch (IllegalArgumentException failure) {
      throw at(
          of("securityProtocol"),
          failure,
          "Allowed securityProtocol values: " + Arrays.toString(SecurityProtocol.values()) + ".");
    }
    if (protocol.startsWith("SASL_")) {
      String mechanism = StringUtils.trimToNull(config.getSaslMechanism());
      if (mechanism == null || !SUPPORTED_SASL_MECHANISMS.contains(mechanism)) {
        throw at(
            of("saslMechanism"),
            new IllegalArgumentException(
                "saslMechanism must be one of "
                    + SUPPORTED_SASL_MECHANISMS
                    + " when securityProtocol is "
                    + protocol),
            "When securityProtocol uses SASL, saslMechanism must be one of "
                + new TreeSet<>(SUPPORTED_SASL_MECHANISMS)
                + ".");
      }
      if (StringUtils.isBlank(config.getCredentialId())) {
        throw at(
            of("credentialId"),
            new IllegalArgumentException(
                "credentialId is required when securityProtocol is " + protocol),
            "credentialId is required and must not be blank when securityProtocol uses SASL.");
      }
      if (enforceCredentials(jobContext)) {
        lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
      }
    }
    rejectManagedProperties(config.getProperties());
  }

  private static void rejectManagedProperties(Map<String, String> properties) {
    if (properties == null) {
      return;
    }
    for (String key : properties.keySet()) {
      if (key != null && MANAGED_PROPERTY_KEYS.contains(key.toLowerCase(Locale.ROOT))) {
        throw at(
            of("properties", key),
            new IllegalArgumentException(
                "property '"
                    + key
                    + "' is managed by the connection profile/credential and must not be set in properties"),
            "This property is managed by the connection profile or credential and must not be set in properties.");
      }
    }
  }
}
