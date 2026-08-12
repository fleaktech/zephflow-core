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
package io.fleak.zephflow.lib.serdes.des.jsonobjline;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fleak.zephflow.lib.serdes.des.LineOrientedTypedDeserializer;
import java.util.ArrayList;
import java.util.List;

/**
 * Reads one JSON value per line. A line holding an object yields one event; a line holding an array
 * of objects is flattened into one event per element:
 *
 * <pre>
 * {"a":1}
 * [{"a":2},{"a":3}]   -&gt; two events
 * </pre>
 *
 * <p>An array line can only ever mean several events (an event is always a JSON object), so
 * flattening is unambiguous. Blank lines are ignored.
 *
 * <p>Created by bolei on 3/17/25
 */
public class JsonObjectLineTypedDeserializer extends LineOrientedTypedDeserializer<ObjectNode> {

  @Override
  protected List<ObjectNode> deserializeLine(String line) {
    JsonNode jsonNode;
    try {
      jsonNode = OBJECT_MAPPER.readTree(line);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("failed to parse json object line: " + line, e);
    }
    if (jsonNode instanceof ObjectNode objectNode) {
      return List.of(objectNode);
    }
    if (!jsonNode.isArray()) {
      throw new IllegalArgumentException(
          "expected a JSON object or an array of JSON objects per line, but this line is a %s: %s"
              .formatted(jsonNode.getNodeType(), line));
    }
    List<ObjectNode> events = new ArrayList<>();
    for (JsonNode element : jsonNode) {
      if (!(element instanceof ObjectNode objectNode)) {
        throw new IllegalArgumentException(
            "expected an array of JSON objects, but element %d of this line is a %s: %s"
                .formatted(events.size(), element.getNodeType(), line));
      }
      events.add(objectNode);
    }
    return events;
  }
}
