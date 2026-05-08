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
package io.fleak.zephflow.lib.gcp;

import org.apache.commons.lang3.StringUtils;

/**
 * Utilities for normalising Pub/Sub topic / subscription names. The user-facing config accepts
 * either a short ID ({@code my-topic}) or a fully-qualified resource path ({@code
 * projects/my-project/topics/my-topic}); these helpers return the fully-qualified path either way.
 */
public final class PubSubResourceNames {

  private PubSubResourceNames() {}

  /** Returns the fully-qualified topic name. */
  public static String topicName(String topic, String projectId) {
    return resolve(topic, projectId, "topics");
  }

  /** Returns the fully-qualified subscription name. */
  public static String subscriptionName(String subscription, String projectId) {
    return resolve(subscription, projectId, "subscriptions");
  }

  private static String resolve(String input, String projectId, String resourceTypePlural) {
    if (StringUtils.isBlank(input)) {
      throw new IllegalArgumentException(resourceTypePlural + " name is required");
    }
    String trimmed = input.strip();
    String typeMarker = "/" + resourceTypePlural + "/";
    if (trimmed.startsWith("projects/") && trimmed.contains(typeMarker)) {
      return trimmed;
    }
    if (StringUtils.isBlank(projectId)) {
      throw new IllegalArgumentException(
          "projectId is required to resolve a short " + resourceTypePlural + " ID");
    }
    return "projects/" + projectId + typeMarker + trimmed;
  }
}
