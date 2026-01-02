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
package io.fleak.zephflow.lib.serdes.ser.jsonobjline;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.ser.MultipleEventsTypedSerializer;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.ByteArrayOutputStream;
import java.util.List;

/**
 * Serializes RecordFleakData directly to JSON Lines format without intermediate ObjectNode
 * conversion. This eliminates the double serialization overhead of FleakData -> ObjectNode ->
 * String.
 */
public class RecordFleakDataLineTypedSerializer
    extends MultipleEventsTypedSerializer<RecordFleakData> {

  private static final ObjectMapper MAPPER = JsonUtils.OBJECT_MAPPER;

  @Override
  protected byte[] serializeToMultipleTypedEvent(List<RecordFleakData> events) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    boolean first = true;
    for (RecordFleakData event : events) {
      if (!first) {
        baos.write('\n');
      }
      first = false;
      MAPPER.writeValue(baos, event);
    }
    return baos.toByteArray();
  }
}
