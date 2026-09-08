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

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import javax.net.ssl.SSLException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.SslAuthenticationException;

public final class KafkaConnectionValidator {
  public enum Status {
    SUCCESS,
    INVALID_CONFIGURATION,
    AUTHENTICATION_FAILED,
    TLS_FAILED,
    UNAVAILABLE
  }

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
      Properties properties = new Properties();
      properties.putAll(effectiveProperties);
      int timeout = (int) Math.max(1, Math.min(10000, remaining(deadline).toMillis()));
      properties.put(AdminClientConfig.CLIENT_ID_CONFIG, "kafka-deployment-preflight");
      properties.put("enable.metrics.push", "false");
      properties.put(AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG, List.of());
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
    return status;
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
