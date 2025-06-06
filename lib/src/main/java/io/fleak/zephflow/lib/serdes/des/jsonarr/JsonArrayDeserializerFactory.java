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
package io.fleak.zephflow.lib.serdes.des.jsonarr;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.converters.JsonObjectTypedEventConverter;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import io.fleak.zephflow.lib.serdes.des.MultipleEventsDeserializer;

/** Created by bolei on 9/18/24 */
public class JsonArrayDeserializerFactory implements DeserializerFactory<ObjectNode> {
  @Override
  public FleakDeserializer<ObjectNode> createDeserializer() {
    var typedEventConverter = new JsonObjectTypedEventConverter();
    var jsonArrayTypedDeserializer = new JsonArrayTypedDeserializer();
    return new MultipleEventsDeserializer<>(
        EncodingType.JSON_ARRAY, typedEventConverter, jsonArrayTypedDeserializer);
  }
}
