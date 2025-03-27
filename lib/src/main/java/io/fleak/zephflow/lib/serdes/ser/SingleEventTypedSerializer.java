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

/** Created by bolei on 9/16/24 */
public abstract class SingleEventTypedSerializer<T> {
  public SerializedEvent serialize(TypedEventContainer<T> typedEventContainer) throws Exception {
    byte[] value = serializeOne(typedEventContainer.payload());
    return SerializedEvent.create(typedEventContainer.metadataWithKey(), value);
  }

  protected abstract byte[] serializeOne(T payload) throws Exception;
}
