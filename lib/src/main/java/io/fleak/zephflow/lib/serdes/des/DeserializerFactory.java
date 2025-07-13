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
package io.fleak.zephflow.lib.serdes.des;

import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.des.csv.CsvDeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.jsonarr.JsonArrayDeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.jsonobj.JsonObjectDeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.jsonobjline.JsonObjectLineDeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.strline.StringLineDeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.text.TextDeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.xml.XmlDeserializerFactory;

/** Created by bolei on 9/16/24 */
public interface DeserializerFactory<T> {
  FleakDeserializer<T> createDeserializer();

  static DeserializerFactory<?> createDeserializerFactory(EncodingType encodingType) {
    return switch (encodingType) {
      case CSV -> new CsvDeserializerFactory();
      case JSON_OBJECT -> new JsonObjectDeserializerFactory();
      case JSON_ARRAY -> new JsonArrayDeserializerFactory();
      case JSON_OBJECT_LINE -> new JsonObjectLineDeserializerFactory();
      case STRING_LINE -> new StringLineDeserializerFactory();
      case TEXT -> new TextDeserializerFactory();
      case XML -> new XmlDeserializerFactory();
    };
  }
}
