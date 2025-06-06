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

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.TypedEventContainer;
import io.fleak.zephflow.lib.serdes.converters.TypedEventConverter;
import java.util.List;

/** Created by bolei on 9/16/24 */
public class MultipleEventsDeserializer<T> extends FleakDeserializer<T> {
  private final MultipleEventsTypedDeserializer<T> multipleEventsTypedDeserializer;

  public MultipleEventsDeserializer(
      EncodingType encodingType,
      TypedEventConverter<T> typedEventConverter,
      MultipleEventsTypedDeserializer<T> multipleEventsTypedDeserializer) {
    super(encodingType, typedEventConverter);
    this.multipleEventsTypedDeserializer = multipleEventsTypedDeserializer;
  }

  @Override
  public List<RecordFleakData> deserialize(SerializedEvent serializedEvent) throws Exception {
    List<TypedEventContainer<T>> typedEvents =
        multipleEventsTypedDeserializer.deserializeMultiple(serializedEvent);
    return typedEvents.stream().map(typedEventConverter::typedEventToFleakData).toList();
  }
}
