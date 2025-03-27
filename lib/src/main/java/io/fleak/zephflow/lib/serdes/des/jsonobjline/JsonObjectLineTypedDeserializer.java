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
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fleak.zephflow.lib.serdes.des.MultipleEventsTypedDeserializer;
import java.util.List;

/** Created by bolei on 3/17/25 */
public class JsonObjectLineTypedDeserializer extends MultipleEventsTypedDeserializer<ObjectNode> {
  @Override
  protected List<ObjectNode> deserializeToMultipleTypedEvent(byte[] value) {
    String rawStr = new String(value);
    return rawStr
        .lines()
        .map(
            l -> {
              try {
                return OBJECT_MAPPER.readTree(l);
              } catch (JsonProcessingException e) {
                throw new IllegalArgumentException("failed to parse json object line: " + l, e);
              }
            })
        .map(jn -> (ObjectNode) jn)
        .toList();
  }
}
