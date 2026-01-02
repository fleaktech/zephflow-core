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
package io.fleak.zephflow.lib.serdes.ser.jsonobj;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.ser.SingleEventTypedSerializer;
import io.fleak.zephflow.lib.utils.JsonUtils;

/**
 * Serializes RecordFleakData directly to JSON bytes without intermediate ObjectNode conversion.
 * This eliminates the double serialization overhead of FleakData -> ObjectNode -> String -> bytes.
 */
public class RecordFleakDataSingleTypedSerializer
    extends SingleEventTypedSerializer<RecordFleakData> {
  @Override
  protected byte[] serializeOne(RecordFleakData payload) throws Exception {
    return JsonUtils.OBJECT_MAPPER.writeValueAsBytes(payload);
  }
}
