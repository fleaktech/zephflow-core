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
package io.fleak.zephflow.lib.serdes.ser;

import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.csv.CsvSerializerFactory;
import io.fleak.zephflow.lib.serdes.ser.jsonarr.JsonArraySerializerFactory;
import io.fleak.zephflow.lib.serdes.ser.jsonobj.JsonObjectSerializerFactory;
import io.fleak.zephflow.lib.serdes.ser.jsonobjline.JsonObjectLineSerializerFactory;

/** Created by bolei on 9/16/24 */
public interface SerializerFactory<T> {
  FleakSerializer<T> createSerializer();

  static SerializerFactory<?> createSerializerFactory(EncodingType encodingType) {
    return switch (encodingType) {
      case CSV -> new CsvSerializerFactory();
      case JSON_OBJECT -> new JsonObjectSerializerFactory();
      case JSON_ARRAY -> new JsonArraySerializerFactory();
      case JSON_OBJECT_LINE -> new JsonObjectLineSerializerFactory();
      default -> throw new UnsupportedOperationException("unsupported serialization encoding type");
    };
  }
}
