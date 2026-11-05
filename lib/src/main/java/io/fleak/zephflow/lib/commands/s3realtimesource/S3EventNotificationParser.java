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
package io.fleak.zephflow.lib.commands.s3realtimesource;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Parses a standard AWS S3 event notification (direct, or wrapped in an SNS envelope) into the list
 * of {@link S3ObjectRef}s it references. Only {@code ObjectCreated:*} events are kept; other S3
 * messages (e.g. {@code s3:TestEvent}, {@code ObjectRemoved:*}) yield an empty list, and JSON that
 * is not an S3 notification at all throws {@link NotS3NotificationException}.
 */
public final class S3EventNotificationParser {

  private static final String EVENT_NAME_OBJECT_CREATED_PREFIX = "ObjectCreated:";
  private static final String TEST_EVENT = "s3:TestEvent";
  private static final Set<String> SNS_CONTROL_TYPES =
      Set.of("SubscriptionConfirmation", "UnsubscribeConfirmation");

  private S3EventNotificationParser() {}

  /** Well-formed JSON that is not an S3 event notification, e.g. a queue of ordinary events. */
  public static final class NotS3NotificationException extends IllegalArgumentException {
    NotS3NotificationException(String reason) {
      super("not an S3 event notification: " + reason);
    }
  }

  public static List<S3ObjectRef> parse(String body) {
    JsonNode root;
    try {
      root = OBJECT_MAPPER.readTree(body);
      // SNS subscription handshakes carry no S3 event; acknowledge them like s3:TestEvent.
      if (SNS_CONTROL_TYPES.contains(root.path("Type").asText())) {
        return List.of();
      }
      // Unwrap SNS -> SQS delivery: the S3 event is a JSON string in the "Message" field.
      if ("Notification".equals(root.path("Type").asText()) && root.hasNonNull("Message")) {
        root = OBJECT_MAPPER.readTree(root.path("Message").asText());
      }
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("malformed S3 event notification", e);
    }

    if (!root.isObject()) {
      throw new NotS3NotificationException("not a JSON object");
    }
    if (TEST_EVENT.equals(root.path("Event").asText())) {
      return List.of();
    }
    JsonNode records = root.path("Records");
    if (!records.isArray()) {
      throw new NotS3NotificationException("no \"Records\" array");
    }

    List<S3ObjectRef> refs = new ArrayList<>();
    for (JsonNode record : records) {
      if (!record.path("s3").isObject()) {
        throw new NotS3NotificationException("a record has no \"s3\" object");
      }
      String eventName = record.path("eventName").asText("");
      if (!eventName.startsWith(EVENT_NAME_OBJECT_CREATED_PREFIX)) {
        continue;
      }
      JsonNode s3 = record.path("s3");
      JsonNode bucketNode = s3.path("bucket").path("name");
      JsonNode keyNode = s3.path("object").path("key");
      if (bucketNode.isMissingNode() || keyNode.isMissingNode()) {
        continue;
      }
      String key = URLDecoder.decode(keyNode.asText(), StandardCharsets.UTF_8);
      JsonNode regionNode = record.path("awsRegion");
      String region = regionNode.isTextual() ? regionNode.textValue() : null;
      refs.add(new S3ObjectRef(bucketNode.asText(), key, region));
    }
    return refs;
  }
}
