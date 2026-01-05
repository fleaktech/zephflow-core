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
package io.fleak.zephflow.api.structure;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;

/**
 * Serializes FleakData directly to JSON without intermediate object creation. This avoids the
 * overhead of unwrap() which creates Map/List objects that then need to be serialized.
 */
public class FleakDataSerializer extends StdSerializer<FleakData> {

  public FleakDataSerializer() {
    this(null);
  }

  protected FleakDataSerializer(Class<FleakData> t) {
    super(t);
  }

  @Override
  public void serialize(FleakData value, JsonGenerator gen, SerializerProvider provider)
      throws IOException {
    if (value == null) {
      gen.writeNull();
      return;
    }

    if (value instanceof RecordFleakData record) {
      gen.writeStartObject();
      for (var entry : record.getPayload().entrySet()) {
        gen.writeFieldName(entry.getKey());
        serialize(entry.getValue(), gen, provider);
      }
      gen.writeEndObject();
    } else if (value instanceof ArrayFleakData array) {
      gen.writeStartArray();
      for (FleakData item : array.getArrayPayload()) {
        serialize(item, gen, provider);
      }
      gen.writeEndArray();
    } else if (value instanceof StringPrimitiveFleakData s) {
      gen.writeString(s.getStringValue());
    } else if (value instanceof NumberPrimitiveFleakData n) {
      if (n.getNumberType() == NumberPrimitiveFleakData.NumberType.LONG) {
        gen.writeNumber((long) n.getNumberValue());
      } else {
        gen.writeNumber(n.getNumberValue());
      }
    } else if (value instanceof BooleanPrimitiveFleakData b) {
      gen.writeBoolean(b.isTrueValue());
    } else {
      gen.writeNull();
    }
  }
}
