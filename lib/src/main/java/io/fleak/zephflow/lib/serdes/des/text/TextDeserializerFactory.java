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
package io.fleak.zephflow.lib.serdes.des.text;

import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.converters.StringTypedEventConverter;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import io.fleak.zephflow.lib.serdes.des.SingleEventDeserializer;
import java.util.List;

/** Created by bolei on 3/24/25 */
public class TextDeserializerFactory implements DeserializerFactory<String> {
  @Override
  public FleakDeserializer<String> createDeserializer() {
    var typedEventConverter = new StringTypedEventConverter();
    var textTypedDeserializer = new TextTypedDeserializer();
    return new SingleEventDeserializer<>(
        List.of(EncodingType.TEXT), typedEventConverter, textTypedDeserializer);
  }
}
