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
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.apache.kafka.common.security.scram.ScramLoginModule;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class KafkaSinkSaslCommandTest {

  private static final String CREDENTIAL_ID = "credential_2";

  static class CapturingProducerFactory extends KafkaProducerClientFactory {
    Properties captured;

    @Override
    @SuppressWarnings("unchecked")
    KafkaProducer<byte[], byte[]> createKafkaProducer(Properties props) {
      this.captured = props;
      return (KafkaProducer<byte[], byte[]>) Mockito.mock(KafkaProducer.class);
    }
  }

  private static Properties captureProps(KafkaSinkDto.Config config, JobContext jobContext) {
    CapturingProducerFactory factory = new CapturingProducerFactory();
    KafkaSinkCommand cmd =
        (KafkaSinkCommand) new KafkaSinkCommandFactory(factory).createCommand("node", jobContext);
    cmd.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));
    cmd.initialize(new MetricClientProvider.NoopMetricClientProvider());
    return factory.captured;
  }

  private static KafkaSinkDto.Config.ConfigBuilder baseConfig() {
    return KafkaSinkDto.Config.builder()
        .broker("localhost:9092")
        .topic("t")
        .encodingType(EncodingType.JSON_OBJECT.name());
  }

  @Test
  void scramPath_buildsScramJaasConfig() {
    KafkaSinkDto.Config config =
        baseConfig()
            .securityProtocol("SASL_SSL")
            .saslMechanism("SCRAM-SHA-512")
            .credentialId(CREDENTIAL_ID)
            .build();

    Properties props = captureProps(config, TestUtils.JOB_CONTEXT);

    assertEquals("SASL_SSL", props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    assertEquals("SCRAM-SHA-512", props.getProperty(SaslConfigs.SASL_MECHANISM));
    assertEquals(
        ScramLoginModule.class.getName()
            + " required username=\"MY_USER_NAME\" password=\"MY_PASSWORD\";",
        props.getProperty(SaslConfigs.SASL_JAAS_CONFIG));
  }

  @Test
  void plainPath_buildsPlainJaasConfig() {
    KafkaSinkDto.Config config =
        baseConfig()
            .securityProtocol("SASL_SSL")
            .saslMechanism("PLAIN")
            .credentialId(CREDENTIAL_ID)
            .build();

    Properties props = captureProps(config, TestUtils.JOB_CONTEXT);

    assertEquals("SASL_SSL", props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    assertEquals("PLAIN", props.getProperty(SaslConfigs.SASL_MECHANISM));
    assertEquals(
        PlainLoginModule.class.getName()
            + " required username=\"MY_USER_NAME\" password=\"MY_PASSWORD\";",
        props.getProperty(SaslConfigs.SASL_JAAS_CONFIG));
  }

  @Test
  void jaasConfig_escapesQuotesAndBackslashes() {
    Map<String, Serializable> other =
        new HashMap<>(
            Map.of(
                "cred_special",
                OBJECT_MAPPER.convertValue(
                    new UsernamePasswordCredential("a\"b", "c\\d"), new TypeReference<>() {})));
    JobContext jobContext = TestUtils.buildJobContext(other);

    KafkaSinkDto.Config config =
        baseConfig()
            .securityProtocol("SASL_PLAINTEXT")
            .saslMechanism("PLAIN")
            .credentialId("cred_special")
            .build();

    Properties props = captureProps(config, jobContext);

    assertEquals(
        PlainLoginModule.class.getName() + " required username=\"a\\\"b\" password=\"c\\\\d\";",
        props.getProperty(SaslConfigs.SASL_JAAS_CONFIG));
  }

  @Test
  void applyCredentialSasl_isLastMutation_preservesUserProps() {
    KafkaSinkDto.Config config =
        baseConfig()
            .securityProtocol("SASL_SSL")
            .saslMechanism("SCRAM-SHA-512")
            .credentialId(CREDENTIAL_ID)
            .properties(Map.of("acks", "all"))
            .build();

    Properties props = captureProps(config, TestUtils.JOB_CONTEXT);

    assertEquals("all", props.getProperty("acks"));
    assertEquals("SASL_SSL", props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    assertEquals("SCRAM-SHA-512", props.getProperty(SaslConfigs.SASL_MECHANISM));
    assertNotNull(props.getProperty(SaslConfigs.SASL_JAAS_CONFIG));
  }

  @Test
  void legacy_noSecurityProtocol_isNoOp() {
    KafkaSinkDto.Config config = baseConfig().build();

    Properties props = captureProps(config, TestUtils.JOB_CONTEXT);

    assertNull(props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    assertNull(props.getProperty(SaslConfigs.SASL_MECHANISM));
    assertNull(props.getProperty(SaslConfigs.SASL_JAAS_CONFIG));
  }

  @Test
  void legacy_rawSaslInProperties_isPreserved() {
    String rawJaas = ScramLoginModule.class.getName() + " required username=\"u\" password=\"p\";";
    Map<String, String> rawProps = new HashMap<>();
    rawProps.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
    rawProps.put(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
    rawProps.put(SaslConfigs.SASL_JAAS_CONFIG, rawJaas);
    KafkaSinkDto.Config config = baseConfig().properties(rawProps).build();

    Properties props = captureProps(config, TestUtils.JOB_CONTEXT);

    assertEquals("SASL_SSL", props.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    assertEquals("SCRAM-SHA-512", props.getProperty(SaslConfigs.SASL_MECHANISM));
    assertEquals(rawJaas, props.getProperty(SaslConfigs.SASL_JAAS_CONFIG));
  }

  @Test
  void missingCredentialAtRuntime_throws() {
    KafkaSinkDto.Config config =
        baseConfig()
            .securityProtocol("SASL_SSL")
            .saslMechanism("SCRAM-SHA-512")
            .credentialId("does_not_exist")
            .build();

    CapturingProducerFactory factory = new CapturingProducerFactory();
    KafkaSinkCommand cmd =
        (KafkaSinkCommand)
            new KafkaSinkCommandFactory(factory).createCommand("node", TestUtils.JOB_CONTEXT);
    cmd.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));

    assertThrows(
        RuntimeException.class,
        () -> cmd.initialize(new MetricClientProvider.NoopMetricClientProvider()));
  }
}
