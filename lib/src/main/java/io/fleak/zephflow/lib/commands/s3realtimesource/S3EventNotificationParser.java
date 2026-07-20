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

/**
 * Parses a standard AWS S3 event notification (direct, or wrapped in an SNS envelope) into the list
 * of {@link S3ObjectRef}s it references. Only {@code ObjectCreated:*} events are kept; everything
 * else (e.g. {@code s3:TestEvent}, {@code ObjectRemoved:*}) yields an empty list.
 */
public final class S3EventNotificationParser {

  private static final String EVENT_NAME_OBJECT_CREATED_PREFIX = "ObjectCreated:";

  private S3EventNotificationParser() {}

  public static List<S3ObjectRef> parse(String body) {
    JsonNode root;
    try {
      root = OBJECT_MAPPER.readTree(body);
      // Unwrap SNS -> SQS delivery: the S3 event is a JSON string in the "Message" field.
      if ("Notification".equals(root.path("Type").asText()) && root.hasNonNull("Message")) {
        root = OBJECT_MAPPER.readTree(root.path("Message").asText());
      }
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("malformed S3 event notification", e);
    }

    JsonNode records = root.path("Records");
    if (!records.isArray()) {
      return List.of();
    }

    List<S3ObjectRef> refs = new ArrayList<>();
    for (JsonNode record : records) {
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
      refs.add(new S3ObjectRef(bucketNode.asText(), key));
    }
    return refs;
  }
}
