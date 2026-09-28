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
package io.fleak.zephflow.lib.serdes.des.jsonarr;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static io.fleak.zephflow.lib.utils.JsonUtils.fromJsonBytes;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fleak.zephflow.lib.serdes.des.IncrementalSupport;
import io.fleak.zephflow.lib.serdes.des.MultipleEventsTypedDeserializer;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/** Created by bolei on 9/18/24 */
public class JsonArrayTypedDeserializer extends MultipleEventsTypedDeserializer<ObjectNode> {
  @Override
  public void deserializeIncrementally(
      byte[] value, Consumer<TypedOutcome<ObjectNode>> consumer, BooleanSupplier stop) {
    IncrementalSupport.visitJson(value, 0, value.length, false, false, consumer, stop);
  }

  /** Reads one element; the elements after it are not trailing tokens, they are the rest. */
  private static final ObjectReader ELEMENT_READER =
      OBJECT_MAPPER
          .readerFor(JsonNode.class)
          .without(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

  @Override
  public boolean supportsStreaming() {
    return true;
  }

  /**
   * Walks the array one element at a time, so only the current element is held in memory. A syntax
   * error leaves the parser with no way to find the next element, so it ends the parse; an element
   * that parses but is not an object is reported on its own and the walk continues.
   */
  @Override
  public void deserializeStream(InputStream input, Consumer<StreamedOutcome<ObjectNode>> consumer)
      throws IOException {
    try (JsonParser parser = OBJECT_MAPPER.getFactory().createParser(input)) {
      try {
        if (parser.nextToken() != JsonToken.START_ARRAY) {
          consumer.accept(malformed(-1, new IllegalArgumentException("Expected a JSON array")));
          return;
        }
      } catch (JsonProcessingException e) {
        consumer.accept(malformed(-1, e));
        return;
      }
      int index = 0;
      while (true) {
        int elementIndex = index + 1;
        JsonNode element;
        try {
          JsonToken token = parser.nextToken();
          if (token == JsonToken.END_ARRAY) {
            break;
          }
          if (token == null) {
            consumer.accept(
                malformed(elementIndex, new IllegalArgumentException("Incomplete JSON array")));
            return;
          }
          element = ELEMENT_READER.readValue(parser);
        } catch (JsonProcessingException e) {
          consumer.accept(malformed(elementIndex, e));
          return;
        }
        index = elementIndex;
        // Delivered outside the try: a consumer failure must propagate, not read as bad input.
        if (element instanceof ObjectNode objectNode) {
          consumer.accept(new StreamedOutcome<>(objectNode, index, null, null));
        } else {
          consumer.accept(
              new StreamedOutcome<>(
                  null,
                  index,
                  String.valueOf(element).getBytes(StandardCharsets.UTF_8),
                  new IllegalArgumentException(
                      "expected a JSON object, but array element %d is a %s"
                          .formatted(index, element == null ? "null" : element.getNodeType()))));
        }
      }
      try {
        if (parser.nextToken() != null) {
          consumer.accept(
              malformed(-1, new IllegalArgumentException("Trailing JSON content after the array")));
        }
      } catch (JsonProcessingException e) {
        consumer.accept(malformed(-1, e));
      }
    }
  }

  private static StreamedOutcome<ObjectNode> malformed(int index, Exception error) {
    return new StreamedOutcome<>(null, index, new byte[0], error);
  }

  @Override
  protected List<ObjectNode> deserializeToMultipleTypedEvent(byte[] value) {
    return fromJsonBytes(value, new TypeReference<>() {});
  }
}
