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
package io.fleak.zephflow.lib.serdes.ser.jsonarr;

import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.ser.MultipleEventsTypedSerializer;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.util.List;

/**
 * Serializes a list of RecordFleakData directly to JSON array bytes without intermediate ObjectNode
 * conversion. This eliminates the double serialization overhead of FleakData -> ObjectNode ->
 * ArrayNode -> String -> bytes.
 */
public class RecordFleakDataArrayTypedSerializer
    extends MultipleEventsTypedSerializer<RecordFleakData> {
  @Override
  protected byte[] serializeToMultipleTypedEvent(List<RecordFleakData> events) throws Exception {
    ArrayFleakData arr = new ArrayFleakData();
    arr.getArrayPayload().addAll(events);
    return JsonUtils.OBJECT_MAPPER.writeValueAsBytes(arr);
  }
}
