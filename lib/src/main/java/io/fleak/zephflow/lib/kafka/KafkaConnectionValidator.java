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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import javax.net.ssl.SSLException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.SslAuthenticationException;
import org.apache.kafka.common.errors.TopicAuthorizationException;

public final class KafkaConnectionValidator {
  public enum Status {
    SUCCESS,
    INVALID_CONFIGURATION,
    AUTHENTICATION_FAILED,
    TLS_FAILED,
    /** The broker denied the principal access to the node's topic or consumer group. */
    AUTHORIZATION_FAILED,
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
    return validate(effectiveProperties, budget, null, null);
  }

  /**
   * Checks that the broker is reachable with these client properties and, once it is, that the
   * principal may describe {@code topic} and {@code groupId} (either may be null to skip it). A
   * principal that can read or write a topic, or join a group, can also describe it. The access
   * checks are best effort: only an authorization denial fails them, so a broker that does not
   * support an API or answers slowly keeps the reachability result.
   */
  public Status validate(
      Properties effectiveProperties, Duration budget, String topic, String groupId) {
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
      status = describeCluster(admin, timeout, deadline);
      if (status == Status.SUCCESS) {
        status = checkAccess(admin, topic, groupId, timeout, deadline);
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

  private static Status describeCluster(Admin admin, int timeout, long deadline)
      throws InterruptedException {
    long remainingMillis = remaining(deadline).toMillis();
    if (remainingMillis <= 0) return Status.UNAVAILABLE;
    try {
      var nodes =
          admin
              .describeCluster(
                  new DescribeClusterOptions().timeoutMs((int) Math.min(timeout, remainingMillis)))
              .nodes()
              .get(remainingMillis, TimeUnit.MILLISECONDS);
      return nodes == null || nodes.isEmpty() ? Status.UNAVAILABLE : Status.SUCCESS;
    } catch (ExecutionException | TimeoutException | RuntimeException failure) {
      return classify(failure);
    }
  }

  private static Status checkAccess(
      Admin admin, String topic, String groupId, int timeout, long deadline)
      throws InterruptedException {
    if (topic != null && !topic.isBlank()) {
      long remainingMillis = remaining(deadline).toMillis();
      if (remainingMillis <= 0) return Status.SUCCESS;
      try {
        admin
            .describeTopics(
                List.of(topic),
                new DescribeTopicsOptions().timeoutMs((int) Math.min(timeout, remainingMillis)))
            .topicNameValues()
            .get(topic)
            .get(remainingMillis, TimeUnit.MILLISECONDS);
      } catch (ExecutionException | TimeoutException | RuntimeException failure) {
        if (hasCause(failure, TopicAuthorizationException.class))
          return Status.AUTHORIZATION_FAILED;
      }
    }
    if (groupId != null && !groupId.isBlank()) {
      long remainingMillis = remaining(deadline).toMillis();
      if (remainingMillis <= 0) return Status.SUCCESS;
      try {
        admin
            .describeConsumerGroups(
                List.of(groupId),
                new DescribeConsumerGroupsOptions()
                    .timeoutMs((int) Math.min(timeout, remainingMillis)))
            .describedGroups()
            .get(groupId)
            .get(remainingMillis, TimeUnit.MILLISECONDS);
      } catch (ExecutionException | TimeoutException | RuntimeException failure) {
        if (hasCause(failure, GroupAuthorizationException.class))
          return Status.AUTHORIZATION_FAILED;
      }
    }
    return Status.SUCCESS;
  }

  private static boolean hasCause(Throwable failure, Class<? extends Throwable> type) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (type.isInstance(cause)) return true;
    }
    return false;
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
