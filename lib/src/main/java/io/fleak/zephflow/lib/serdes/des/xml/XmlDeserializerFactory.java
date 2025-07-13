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
package io.fleak.zephflow.lib.serdes.des.xml;

import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.converters.ObjectMapTypedEventConverter;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import io.fleak.zephflow.lib.serdes.des.SingleEventDeserializer;
import java.util.Map;

/** Created by bolei on 7/11/25 */
public class XmlDeserializerFactory implements DeserializerFactory<Map<String, Object>> {
  @Override
  public FleakDeserializer<Map<String, Object>> createDeserializer() {
    var typedEventConverter = new ObjectMapTypedEventConverter();
    XmlTypedDeserializer xmlTypedDeserializer = new XmlTypedDeserializer();
    return new SingleEventDeserializer<>(
        EncodingType.XML, typedEventConverter, xmlTypedDeserializer);
  }
}
