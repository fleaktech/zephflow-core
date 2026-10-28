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
package io.fleak.zephflow.lib.serdes.decode.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.IOException;

final class JsonDecoding {

  /** Size limits are enforced by the budget layer, so Jackson's own length limits are lifted. */
  static final JsonFactory FACTORY =
      JsonFactory.builder()
          .streamReadConstraints(
              StreamReadConstraints.builder()
                  .maxStringLength(Integer.MAX_VALUE)
                  .maxNameLength(Integer.MAX_VALUE)
                  .build())
          .build();

  static final ObjectMapper MAPPER = new ObjectMapper(FACTORY);

  private JsonDecoding() {}

  /** Parses exactly one JSON object from {@code bytes}; anything after it is an error. */
  static RecordFleakData parseObject(byte[] bytes) throws IOException {
    try (JsonParser parser = FACTORY.createParser(bytes)) {
      JsonNode node = MAPPER.readTree(parser);
      if (node == null || node.isMissingNode()) {
        throw new IllegalArgumentException("no JSON value");
      }
      if (parser.nextToken() != null) {
        throw new IllegalArgumentException("unexpected content after the JSON value");
      }
      return toRecord(node);
    }
  }

  static RecordFleakData toRecord(JsonNode node) {
    if (!(node instanceof ObjectNode object)) {
      throw new IllegalArgumentException("expected a JSON object, got " + describe(node));
    }
    return JsonUtils.fromJsonPayload(object);
  }

  static String describe(JsonNode node) {
    return switch (node.getNodeType()) {
      case OBJECT -> "a JSON object";
      case ARRAY -> "a JSON array";
      case STRING -> "a JSON string";
      case NUMBER -> "a JSON number";
      case BOOLEAN -> "a JSON boolean";
      case NULL -> "null";
      default -> node.getNodeType().toString();
    };
  }

  static String describe(JsonToken token) {
    return switch (token) {
      case START_OBJECT -> "a JSON object";
      case START_ARRAY -> "a JSON array";
      case VALUE_STRING -> "a JSON string";
      case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> "a JSON number";
      case VALUE_TRUE, VALUE_FALSE -> "a JSON boolean";
      case VALUE_NULL -> "null";
      default -> token.toString();
    };
  }

  static long byteOffset(JsonLocation location) {
    return location == null ? -1 : location.getByteOffset();
  }
}
