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

import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.TypedEventContainer;
import java.util.List;

/** Created by bolei on 9/16/24 */
public abstract class MultipleEventsTypedSerializer<T> {
  public SerializedEvent serializeMultiple(List<TypedEventContainer<T>> typedEvents)
      throws Exception {
    List<T> typedValues = typedEvents.stream().map(TypedEventContainer::payload).toList();
    byte[] bytes = serializeToMultipleTypedEvent(typedValues);

    // when we deserialize multiple events from bytes, we use the same metadata to set all the
    // events in the batch. However, we cannot assume all the events have the same metadata when
    // serializing. That's why we skip metadata here
    return new SerializedEvent(null, bytes, null);
  }

  protected abstract byte[] serializeToMultipleTypedEvent(List<T> typedValues) throws Exception;
}
