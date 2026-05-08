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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class PubSubResourceNamesTest {

  @Test
  void topicName_shortId_isExpanded() {
    assertEquals(
        "projects/my-project/topics/my-topic",
        PubSubResourceNames.topicName("my-topic", "my-project"));
  }

  @Test
  void topicName_fullyQualified_isReturnedAsIs() {
    String full = "projects/other-project/topics/my-topic";
    assertEquals(full, PubSubResourceNames.topicName(full, "my-project"));
  }

  @Test
  void topicName_fullyQualifiedWithSurroundingWhitespace_isTrimmed() {
    String full = "projects/other-project/topics/my-topic";
    assertEquals(full, PubSubResourceNames.topicName("  " + full + "  ", "my-project"));
  }

  @Test
  void topicName_blankInput_throws() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> PubSubResourceNames.topicName("  ", "my-project"));
    assertTrue(ex.getMessage().contains("topics name is required"));
  }

  @Test
  void topicName_shortIdWithBlankProjectId_throws() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class, () -> PubSubResourceNames.topicName("my-topic", ""));
    assertTrue(ex.getMessage().contains("projectId is required"));
  }

  @Test
  void topicName_fullyQualifiedWithBlankProjectId_isStillReturnedAsIs() {
    String full = "projects/other-project/topics/my-topic";
    assertEquals(full, PubSubResourceNames.topicName(full, null));
  }

  @Test
  void subscriptionName_shortId_isExpanded() {
    assertEquals(
        "projects/my-project/subscriptions/my-sub",
        PubSubResourceNames.subscriptionName("my-sub", "my-project"));
  }

  @Test
  void subscriptionName_fullyQualified_isReturnedAsIs() {
    String full = "projects/other-project/subscriptions/my-sub";
    assertEquals(full, PubSubResourceNames.subscriptionName(full, "my-project"));
  }

  @Test
  void subscriptionName_inputThatLooksFullyQualifiedButForTopics_isExpandedAsShortId() {
    // Defensive: a "projects/.../topics/..." string passed where a subscription
    // is expected gets treated as a short ID (preserving the existing behaviour
    // for users who paste the wrong path — Pub/Sub will reject it server-side
    // with a clearer "Resource not found" rather than a malformed-name error).
    String topicLike = "projects/p/topics/t";
    assertEquals(
        "projects/my-project/subscriptions/" + topicLike,
        PubSubResourceNames.subscriptionName(topicLike, "my-project"));
  }
}
