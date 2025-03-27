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

/** Created by bolei on 3/7/25 */
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

    // Simply use the unwrap method that returns the native Java representation
    Object unwrapped = value.unwrap();
    provider.defaultSerializeValue(unwrapped, gen);
  }
}
