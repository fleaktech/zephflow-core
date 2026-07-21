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

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;

public class KafkaSinkConfigValidator implements ConfigValidator {

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
      throw new IllegalArgumentException("Broker must be provided");
    }
    if (StringUtils.isBlank(config.getTopic())) {
      throw new IllegalArgumentException("Topic must be provided");
    }
    EncodingType encodingType = parseEnum(EncodingType.class, config.getEncodingType());
    SerializerFactory.validateEncodingType(encodingType);
    if (StringUtils.isNotBlank(config.getPartitionKeyFieldExpressionStr())) {
      PathExpression.fromString(config.getPartitionKeyFieldExpressionStr());
    }
    if (config.isStoreAndForwardEnabled()) {
      if (config.getLocalStoreMaxBytes() <= 0) {
        throw new IllegalArgumentException(
            "localStoreMaxBytes must be positive when storeAndForwardEnabled. Got: "
                + config.getLocalStoreMaxBytes());
      }
      if (config.getForwardRetryIntervalMillis() <= 0) {
        throw new IllegalArgumentException(
            "forwardRetryIntervalMillis must be positive when storeAndForwardEnabled. Got: "
                + config.getForwardRetryIntervalMillis());
      }
    }

    validateAssetManagedSecurity(config, jobContext);
  }

  private void validateAssetManagedSecurity(KafkaSinkDto.Config config, JobContext jobContext) {
    String protocol = StringUtils.trimToNull(config.getSecurityProtocol());
    if (protocol == null) {
      return;
    }
    parseEnum(SecurityProtocol.class, protocol);
    if (protocol.startsWith("SASL_")) {
      String mechanism = StringUtils.trimToNull(config.getSaslMechanism());
      if (mechanism == null || !SUPPORTED_SASL_MECHANISMS.contains(mechanism)) {
        throw new IllegalArgumentException(
            "saslMechanism must be one of "
                + SUPPORTED_SASL_MECHANISMS
                + " when securityProtocol is "
                + protocol);
      }
      if (StringUtils.isBlank(config.getCredentialId())) {
        throw new IllegalArgumentException(
            "credentialId is required when securityProtocol is " + protocol);
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
        throw new IllegalArgumentException(
            "property '"
                + key
                + "' is managed by the connection profile/credential and must not be set in properties");
      }
    }
  }
}
