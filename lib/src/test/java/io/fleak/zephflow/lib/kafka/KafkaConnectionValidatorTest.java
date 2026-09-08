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
              assertEquals("unused", actual.getProperty("group.id"));
              assertEquals(List.of(), actual.get("metric.reporters"));
              assertEquals("kafka-deployment-preflight", actual.getProperty("client.id"));
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
    assertEquals("evil.Provider", properties.getProperty("metric.reporters"));
    assertFalse(properties.containsKey("enable.metrics.push"));
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
  void nativeSecurityAndProviderConfigurationReachesClientUnchanged() {
    Properties effective = properties();
    effective.put("security.protocol", "SASL_SSL");
    effective.put("sasl.mechanism", "GSSAPI");
    effective.put(
        "sasl.jaas.config",
        "com.sun.security.auth.module.Krb5LoginModule required useTicketCache=true;");
    effective.put("sasl.client.callback.handler.class", "example.CallbackHandler");
    effective.put("sasl.login.callback.handler.class", "example.LoginCallbackHandler");
    effective.put("sasl.login.class", "example.Login");
    effective.put("security.providers", "example.SecurityProvider");
    effective.put("ssl.provider", "example.TlsProvider");
    effective.put("ssl.engine.factory.class", "example.SslEngineFactory");
    effective.put("ssl.truststore.location", "/native/client/resolves/this/file");
    effective.put("config.providers", "file");
    effective.put(
        "config.providers.file.class",
        "org.apache.kafka.common.config.provider.FileConfigProvider");
    effective.put("config.providers.file.param.allowed.paths", "/native/configuration");
    Properties original = new Properties();
    original.putAll(effective);
    KafkaConnectionValidator validator =
        new KafkaConnectionValidator(
            actual -> {
              effective.forEach(
                  (key, value) -> assertEquals(value, actual.get(key), key.toString()));
              throw new SaslAuthenticationException("native-client-reached");
            });
    assertEquals(
        KafkaConnectionValidator.Status.AUTHENTICATION_FAILED,
        validator.validate(effective, Duration.ofSeconds(1)));
    assertEquals(original, effective);
  }

  @Test
  void expiredBudgetDoesNotCreateClient() {
    KafkaConnectionValidator validator =
        new KafkaConnectionValidator(
            properties -> {
              fail("client created after deadline");
              return null;
            });
    assertEquals(
        KafkaConnectionValidator.Status.UNAVAILABLE,
        validator.validate(properties(), Duration.ZERO));
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
