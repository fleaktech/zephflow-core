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

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class KafkaSinkConfigValidatorTest {

  private final KafkaSinkConfigValidator validator = new KafkaSinkConfigValidator();

  @Test
  void validateConfig_validConfig() {
    KafkaSinkDto.Config config =
        KafkaSinkDto.Config.builder()
            .broker("localhost:9092")
            .topic("test-topic")
            .encodingType(EncodingType.JSON_OBJECT.name())
            .partitionKeyFieldExpressionStr("$.user_id")
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", null));
  }

  @Test
  void validateConfig_missingBroker() {
    KafkaSinkDto.Config config =
        KafkaSinkDto.Config.builder()
            .broker("") // Empty string instead of null
            .topic("test-topic")
            .encodingType(EncodingType.JSON_OBJECT.name())
            .build();

    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", null));
    assertEquals("Broker must be provided", exception.getMessage());
  }

  @Test
  void validateConfig_missingTopic() {
    KafkaSinkDto.Config config =
        KafkaSinkDto.Config.builder()
            .broker("localhost:9092")
            .topic("") // Empty string instead of null
            .encodingType(EncodingType.JSON_OBJECT.name())
            .build();

    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", null));
    assertEquals("Topic must be provided", exception.getMessage());
  }

  @Test
  void validateConfig_invalidEncodingType() {
    KafkaSinkDto.Config config =
        KafkaSinkDto.Config.builder()
            .broker("localhost:9092")
            .topic("test-topic")
            .encodingType("INVALID_TYPE")
            .build();

    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", null));
    assertTrue(exception.getMessage().contains("Invalid value for enum EncodingType"));
  }

  @Test
  void validateConfig_unsupportedEncodingType_stringLine() {
    KafkaSinkDto.Config config =
        KafkaSinkDto.Config.builder()
            .broker("localhost:9092")
            .topic("test-topic")
            .encodingType(EncodingType.STRING_LINE.name())
            .build();

    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", null));
    assertTrue(exception.getMessage().contains("Unsupported serialization encoding type"));
  }

  @Test
  void validateConfig_unsupportedEncodingType_text() {
    KafkaSinkDto.Config config =
        KafkaSinkDto.Config.builder()
            .broker("localhost:9092")
            .topic("test-topic")
            .encodingType(EncodingType.TEXT.name())
            .build();

    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", null));
    assertTrue(exception.getMessage().contains("Unsupported serialization encoding type"));
  }

  @Test
  void validateConfig_unsupportedEncodingType_xml() {
    KafkaSinkDto.Config config =
        KafkaSinkDto.Config.builder()
            .broker("localhost:9092")
            .topic("test-topic")
            .encodingType(EncodingType.XML.name())
            .build();

    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", null));
    assertTrue(exception.getMessage().contains("Unsupported serialization encoding type"));
  }

  @Test
  void validateConfig_allSupportedEncodingTypes() {
    for (EncodingType type : SerializerFactory.SUPPORTED_ENCODING_TYPES) {
      KafkaSinkDto.Config config =
          KafkaSinkDto.Config.builder()
              .broker("localhost:9092")
              .topic("test-topic")
              .encodingType(type.name())
              .build();

      assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", null));
    }
  }

  private static KafkaSinkDto.Config.ConfigBuilder base() {
    return KafkaSinkDto.Config.builder()
        .broker("localhost:9092")
        .topic("test-topic")
        .encodingType(EncodingType.JSON_OBJECT.name());
  }

  @Test
  void validateConfig_noSecurityProtocol_ok() {
    assertDoesNotThrow(() -> validator.validateConfig(base().build(), "test-node", null));
  }

  @Test
  void validateConfig_saslWithCredentialAndEnforce_ok() {
    Map<String, Serializable> other = new HashMap<>();
    other.put("enforce_cred", Boolean.TRUE);
    other.put(
        "credential_2",
        OBJECT_MAPPER.convertValue(
            TestUtils.USERNAME_PASSWORD_CREDENTIAL, new TypeReference<>() {}));
    JobContext jobContext = TestUtils.buildJobContext(other);

    KafkaSinkDto.Config config =
        base()
            .securityProtocol("SASL_SSL")
            .saslMechanism("SCRAM-SHA-512")
            .credentialId("credential_2")
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", jobContext));
  }

  @Test
  void validateConfig_saslWithoutCredentialId_throws() {
    KafkaSinkDto.Config config = base().securityProtocol("SASL_SSL").saslMechanism("PLAIN").build();

    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", null));
    assertTrue(exception.getMessage().contains("credentialId is required"));
  }

  @Test
  void validateConfig_unsupportedSaslMechanism_throws() {
    KafkaSinkDto.Config config =
        base()
            .securityProtocol("SASL_SSL")
            .saslMechanism("MD5")
            .credentialId("credential_2")
            .build();

    assertThrows(
        IllegalArgumentException.class, () -> validator.validateConfig(config, "test-node", null));
  }

  @Test
  void validateConfig_saslWithBlankMechanism_throwsActionable() {
    KafkaSinkDto.Config config =
        base().securityProtocol("SASL_SSL").credentialId("credential_2").build();

    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", null));
    assertTrue(exception.getMessage().contains("saslMechanism must be one of"));
  }

  @Test
  void validateConfig_invalidSecurityProtocol_throws() {
    KafkaSinkDto.Config config = base().securityProtocol("BOGUS").build();

    assertThrows(
        IllegalArgumentException.class, () -> validator.validateConfig(config, "test-node", null));
  }

  @Test
  void validateConfig_assetManagedRejectsSecretProperties() {
    for (String key :
        List.of(
            "sasl.jaas.config", "ssl.truststore.password", "security.protocol", "sasl.mechanism")) {
      KafkaSinkDto.Config config =
          base()
              .securityProtocol("SASL_SSL")
              .saslMechanism("SCRAM-SHA-512")
              .credentialId("credential_2")
              .properties(Map.of(key, "x"))
              .build();

      assertThrows(
          IllegalArgumentException.class,
          () -> validator.validateConfig(config, "test-node", TestUtils.JOB_CONTEXT),
          "expected rejection for property " + key);
    }
  }

  @Test
  void validateConfig_assetManagedRejectsSecretProperties_caseInsensitive() {
    KafkaSinkDto.Config config =
        base()
            .securityProtocol("SASL_SSL")
            .saslMechanism("SCRAM-SHA-512")
            .credentialId("credential_2")
            .properties(Map.of("SASL.JAAS.Config", "x"))
            .build();

    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "test-node", TestUtils.JOB_CONTEXT));
  }

  @Test
  void validateConfig_legacyRawSaslProperties_ok() {
    KafkaSinkDto.Config config =
        base()
            .properties(
                Map.of(
                    "security.protocol", "SASL_SSL",
                    "sasl.mechanism", "SCRAM-SHA-512",
                    "sasl.jaas.config",
                        "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"u\" password=\"p\";"))
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", null));
  }

  @Test
  void validateConfig_saslCredentialAbsentButEnforceOff_ok() {
    KafkaSinkDto.Config config =
        base()
            .securityProtocol("SASL_SSL")
            .saslMechanism("SCRAM-SHA-512")
            .credentialId("absent_cred")
            .build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", TestUtils.JOB_CONTEXT));
  }
}
