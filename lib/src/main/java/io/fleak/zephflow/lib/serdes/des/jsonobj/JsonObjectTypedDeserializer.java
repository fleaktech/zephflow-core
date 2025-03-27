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
package io.fleak.zephflow.lib.serdes.des.jsonobj;

import static io.fleak.zephflow.lib.utils.JsonUtils.fromJsonBytes;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fleak.zephflow.lib.serdes.des.SingleEventTypedDeserializer;

/** Created by bolei on 9/16/24 */
public class JsonObjectTypedDeserializer extends SingleEventTypedDeserializer<ObjectNode> {
  @Override
  protected ObjectNode deserializeOne(byte[] value) {
    return fromJsonBytes(value, new TypeReference<>() {});
  }
}
