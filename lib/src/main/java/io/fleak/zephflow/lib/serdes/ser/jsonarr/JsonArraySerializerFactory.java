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

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.converters.JsonObjectTypedEventConverter;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import io.fleak.zephflow.lib.serdes.ser.MultipleEventsSerializer;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import java.util.List;

/** Created by bolei on 9/17/24 */
public class JsonArraySerializerFactory implements SerializerFactory<ObjectNode> {
  @Override
  public FleakSerializer<ObjectNode> createSerializer() {
    var typedEventConverter = new JsonObjectTypedEventConverter();
    var jsonArrayTypedSerializer = new JsonArrayTypedSerializer();
    return new MultipleEventsSerializer<>(
        List.of(EncodingType.JSON_ARRAY), typedEventConverter, jsonArrayTypedSerializer);
  }
}
