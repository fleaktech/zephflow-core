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
package io.fleak.zephflow.lib.serdes.converters;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.TypedEventContainer;
import org.apache.commons.collections4.MapUtils;

/** Created by bolei on 9/16/24 */
public abstract class TypedEventConverter<T> {
  public RecordFleakData typedEventToFleakData(TypedEventContainer<T> typedEvent) {
    RecordFleakData recordFleakData = payloadToFleakData(typedEvent.payload());
    if (MapUtils.isNotEmpty(typedEvent.getMetadataPayload())) {
      recordFleakData.getPayload().putAll(typedEvent.getMetadataPayload());
    }
    return recordFleakData;
  }

  protected abstract RecordFleakData payloadToFleakData(T payload);

  public abstract TypedEventContainer<T> fleakDataToTypedEvent(RecordFleakData recordFleakData);
}
