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

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import javax.net.ssl.SSLException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.SslAuthenticationException;
import org.apache.logging.log4j.LogManager;

public final class KafkaConnectionValidator {
  public enum Status {
    SUCCESS,
    INVALID_CONFIGURATION,
    AUTHENTICATION_FAILED,
    TLS_FAILED,
    UNAVAILABLE,
    UNSUPPORTED
  }

  private static final Set<String> SUPPORTED_LOGIN_MODULES =
      Set.of(
          "org.apache.kafka.common.security.plain.PlainLoginModule",
          "org.apache.kafka.common.security.scram.ScramLoginModule");
  private static final Set<String> SUPPORTED_SASL_MECHANISMS =
      Set.of("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512");
  private final Function<Properties, Admin> clientFactory;

  public KafkaConnectionValidator() {
    this(Admin::create);
  }

  KafkaConnectionValidator(Function<Properties, Admin> clientFactory) {
    this.clientFactory = clientFactory;
  }

  public Status validate(Properties effectiveProperties, Duration budget) {
    long deadline = System.nanoTime() + Math.max(0, budget.toNanos());
    Admin admin = null;
    Status status;
    try {
      if (budget.isZero() || budget.isNegative()) {
        return Status.UNAVAILABLE;
      }
      Properties properties = connectionProperties(effectiveProperties);
      int timeout = (int) Math.max(1, Math.min(10000, remaining(deadline).toMillis()));
      properties.put(AdminClientConfig.CLIENT_ID_CONFIG, "kafka-deployment-preflight");
      properties.put("enable.metrics.push", "false");
      properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, Integer.toString(timeout));
      properties.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, Integer.toString(timeout));
      properties.put("socket.connection.setup.timeout.ms", Integer.toString(timeout));
      properties.put("socket.connection.setup.timeout.max.ms", Integer.toString(timeout));
      admin = clientFactory.apply(properties);
      long remainingMillis = remaining(deadline).toMillis();
      if (remainingMillis <= 0) {
        status = Status.UNAVAILABLE;
      } else {
        var nodes =
            admin
                .describeCluster(
                    new DescribeClusterOptions()
                        .timeoutMs((int) Math.min(timeout, remainingMillis)))
                .nodes()
                .get(remainingMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
        status = nodes == null || nodes.isEmpty() ? Status.UNAVAILABLE : Status.SUCCESS;
      }
    } catch (UnsupportedOperationException failure) {
      status = Status.UNSUPPORTED;
    } catch (Exception failure) {
      status = classify(failure);
      if (failure instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
    } finally {
      if (admin != null) {
        try {
          admin.close(remaining(deadline));
        } catch (Exception ignored) {
        }
      }
    }
    LogManager.getLogger(KafkaConnectionValidator.class)
        .info("Kafka connection validation completed: {}", status);
    return status;
  }

  static Properties connectionProperties(Properties effective) throws java.io.IOException {
    Properties properties = new Properties();
    for (var entry : effective.entrySet()) {
      String key = entry.getKey().toString();
      String value = String.valueOf(entry.getValue());
      if ((key.contains("callback.handler.class")
              || key.equals("sasl.login.class")
              || key.equals("security.providers")
              || key.equals("ssl.provider")
              || key.equals("ssl.engine.factory.class")
              || key.equals("config.providers")
              || key.startsWith("config.providers."))
          && !value.isBlank()) {
        throw new UnsupportedOperationException();
      }
      if (key.startsWith("ssl.")
          && key.endsWith(".location")
          && !value.isBlank()
          && (!Files.isRegularFile(Path.of(value.trim()))
              || !Files.isReadable(Path.of(value.trim())))) {
        throw new UnsupportedOperationException();
      }
      if (key.equals("sasl.mechanism") && !SUPPORTED_SASL_MECHANISMS.contains(value.trim())) {
        throw new UnsupportedOperationException();
      }
      if (key.equals("sasl.jaas.config")) {
        var tokenizer = new java.io.StreamTokenizer(new java.io.StringReader(value));
        tokenizer.slashSlashComments(true);
        tokenizer.slashStarComments(true);
        tokenizer.wordChars('-', '-');
        tokenizer.wordChars('_', '_');
        tokenizer.wordChars('$', '$');
        if (tokenizer.nextToken() == java.io.StreamTokenizer.TT_EOF) {
          throw new IllegalArgumentException();
        }
        if (tokenizer.sval != null && !SUPPORTED_LOGIN_MODULES.contains(tokenizer.sval)) {
          throw new UnsupportedOperationException();
        }
        var entries =
            org.apache.kafka.common.security.JaasContext.loadClientContext(
                    java.util.Map.of(
                        "sasl.jaas.config",
                        new org.apache.kafka.common.config.types.Password(value)))
                .configuration()
                .getAppConfigurationEntry("KafkaClient");
        if (entries == null
            || entries.length != 1
            || !SUPPORTED_LOGIN_MODULES.contains(entries[0].getLoginModuleName())) {
          throw new UnsupportedOperationException();
        }
      }
      if (AdminClientConfig.configNames().contains(key)
          && (key.equals("bootstrap.servers")
              || key.equals("security.protocol")
              || key.equals("client.dns.lookup")
              || key.startsWith("ssl.")
              || key.startsWith("sasl."))) {
        properties.put(key, entry.getValue());
      }
    }
    String protocol = properties.getProperty("security.protocol", "PLAINTEXT").trim();
    if (protocol.startsWith("SASL_")
        && (!properties.containsKey("sasl.jaas.config")
            || !SUPPORTED_SASL_MECHANISMS.contains(
                properties.getProperty("sasl.mechanism", "GSSAPI").trim()))) {
      throw new UnsupportedOperationException();
    }
    return properties;
  }

  private static Status classify(Throwable failure) {
    boolean invalidConfiguration = false;
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof ClusterAuthorizationException) return Status.SUCCESS;
      if (cause instanceof SslAuthenticationException || cause instanceof SSLException)
        return Status.TLS_FAILED;
      if (cause instanceof AuthenticationException) return Status.AUTHENTICATION_FAILED;
      if (cause instanceof ConfigException || cause instanceof IllegalArgumentException)
        invalidConfiguration = true;
    }
    return invalidConfiguration ? Status.INVALID_CONFIGURATION : Status.UNAVAILABLE;
  }

  private static Duration remaining(long deadline) {
    return Duration.ofNanos(Math.max(0, deadline - System.nanoTime()));
  }
}
