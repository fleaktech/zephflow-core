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
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.SaslAuthenticationException;
import org.junit.jupiter.api.Test;

class KafkaConnectionValidatorTest {
  @Test
  void metadataOnlyAndOverridesTimeouts() {
    Admin admin = mock(Admin.class);
    DescribeClusterResult result = mock(DescribeClusterResult.class);
    when(admin.describeCluster(any())).thenReturn(result);
    when(result.nodes())
        .thenReturn(KafkaFuture.completedFuture(List.of(new Node(1, "broker", 9092))));
    Properties properties = new Properties();
    properties.put("bootstrap.servers", "broker:9092");
    properties.put("default.api.timeout.ms", "9999999");
    properties.put("group.id", "unused");
    properties.put("metric.reporters", "evil.Provider");
    KafkaConnectionValidator validator =
        new KafkaConnectionValidator(
            actual -> {
              assertFalse(actual.containsKey("group.id"));
              assertFalse(actual.containsKey("metric.reporters"));
              assertEquals("false", actual.getProperty("enable.metrics.push"));
              assertTrue(Integer.parseInt(actual.getProperty("default.api.timeout.ms")) <= 1000);
              return admin;
            });
    assertEquals(
        KafkaConnectionValidator.Status.SUCCESS,
        validator.validate(properties, Duration.ofSeconds(1)));
    verify(admin).describeCluster(any());
    verify(admin).close(any(Duration.class));
    verifyNoMoreInteractions(admin);
    assertEquals("9999999", properties.getProperty("default.api.timeout.ms"));
  }

  @Test
  void classifiesAuthenticationWithoutExposingProviderText() {
    KafkaConnectionValidator validator =
        new KafkaConnectionValidator(
            properties -> {
              throw new SaslAuthenticationException("SENTINEL_PASSWORD");
            });
    assertEquals(
        KafkaConnectionValidator.Status.AUTHENTICATION_FAILED,
        validator.validate(properties(), Duration.ofSeconds(1)));
  }

  @Test
  void clusterDescribeDenialStillProvesReachability() {
    KafkaConnectionValidator validator =
        new KafkaConnectionValidator(
            properties -> {
              throw new ClusterAuthorizationException("cluster describe denied");
            });
    assertEquals(
        KafkaConnectionValidator.Status.SUCCESS,
        validator.validate(properties(), Duration.ofSeconds(1)));
  }

  @Test
  void rejectsProviderBeforeLoadingAnyClient() {
    KafkaConnectionValidator validator =
        new KafkaConnectionValidator(
            properties -> {
              fail("client created");
              return null;
            });
    Properties properties = properties();
    properties.put("sasl.client.callback.handler.class", "evil.Provider");
    assertEquals(
        KafkaConnectionValidator.Status.UNSUPPORTED,
        validator.validate(properties, Duration.ofSeconds(1)));
    properties.remove("sasl.client.callback.handler.class");
    properties.put("ssl.truststore.location", "/missing/secret");
    assertEquals(
        KafkaConnectionValidator.Status.UNSUPPORTED,
        validator.validate(properties, Duration.ofSeconds(1)));
  }

  @Test
  void unsupportedJaasAndDefaultKerberosCannotLoadProviderClasses() {
    KafkaConnectionValidator validator =
        new KafkaConnectionValidator(
            properties -> {
              fail("client created");
              return null;
            });
    Properties properties = properties();
    properties.put("sasl.jaas.config", "com.sun.security.auth.module.JndiLoginModule required;");
    assertEquals(
        KafkaConnectionValidator.Status.UNSUPPORTED,
        validator.validate(properties, Duration.ofSeconds(1)));
    properties.put(
        "sasl.jaas.config",
        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"u\" password=\"p\";");
    properties.put("security.protocol", "SASL_SSL");
    assertEquals(
        KafkaConnectionValidator.Status.UNSUPPORTED,
        validator.validate(properties, Duration.ofSeconds(1)));
  }

  @Test
  void tlsRootCauseWinsOverConfigurationWrapper() {
    KafkaConnectionValidator validator =
        new KafkaConnectionValidator(
            properties -> {
              throw new IllegalArgumentException(
                  new javax.net.ssl.SSLHandshakeException("SENTINEL_PASSWORD"));
            });
    assertEquals(
        KafkaConnectionValidator.Status.TLS_FAILED,
        validator.validate(properties(), Duration.ofSeconds(1)));
  }

  private Properties properties() {
    Properties properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    return properties;
  }
}
