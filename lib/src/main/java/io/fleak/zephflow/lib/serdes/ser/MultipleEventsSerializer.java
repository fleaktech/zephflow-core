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

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.TypedEventContainer;
import io.fleak.zephflow.lib.serdes.converters.TypedEventConverter;
import java.util.List;

/** Created by bolei on 9/16/24 */
public class MultipleEventsSerializer<T> extends FleakSerializer<T> {
  private final MultipleEventsTypedSerializer<T> multipleEventsTypedSerializer;

  public MultipleEventsSerializer(
      EncodingType encodingType,
      TypedEventConverter<T> typedEventConverter,
      MultipleEventsTypedSerializer<T> multipleEventsTypedSerializer) {
    super(encodingType, typedEventConverter);
    this.multipleEventsTypedSerializer = multipleEventsTypedSerializer;
  }

  @Override
  public SerializedEvent serialize(List<RecordFleakData> events) throws Exception {
    List<TypedEventContainer<T>> typedEvents =
        events.stream().map(typedEventConverter::fleakDataToTypedEvent).toList();

    return multipleEventsTypedSerializer.serializeMultiple(typedEvents);
  }
}
