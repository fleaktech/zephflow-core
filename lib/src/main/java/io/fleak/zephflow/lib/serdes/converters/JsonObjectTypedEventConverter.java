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
package io.fleak.zephflow.lib.serdes.converters;

import static io.fleak.zephflow.lib.utils.JsonUtils.fromJsonPayload;
import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonPayload;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.TypedEventContainer;

/** Created by bolei on 9/16/24 */
public class JsonObjectTypedEventConverter extends TypedEventConverter<ObjectNode> {

  @Override
  protected RecordFleakData payloadToFleakData(ObjectNode payload) {
    return fromJsonPayload(payload);
  }

  @Override
  public TypedEventContainer<ObjectNode> fleakDataToTypedEvent(RecordFleakData recordFleakData) {
    ObjectNode objectNode = toJsonPayload(recordFleakData);
    return new TypedEventContainer<>(objectNode, null);
  }
}
