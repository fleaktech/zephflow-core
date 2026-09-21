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

import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.TypedEventContainer;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/** Created by bolei on 9/16/24 */
public abstract class MultipleEventsTypedDeserializer<T> {
  /** Typed decoding outcome; raw ranges refer to the unchanged input bytes. */
  public record TypedOutcome<T>(
      T value, int index, int rawOffset, int rawLength, Exception error) {}

  /** Legacy third-party subclasses remain compatible; incremental support must be explicit. */
  public void deserializeIncrementally(
      byte[] value, Consumer<TypedOutcome<T>> consumer, BooleanSupplier stopRequested) {
    throw new UnsupportedOperationException(
        "Incremental decoding is not implemented for this format");
  }

  public List<TypedEventContainer<T>> deserializeMultiple(SerializedEvent serializedEvent)
      throws Exception {
    Map<String, String> metadata = SerializedEvent.metadataWithKey(serializedEvent);
    List<T> typedValues = deserializeToMultipleTypedEvent(serializedEvent.value());
    return typedValues.stream().map(tv -> new TypedEventContainer<>(tv, metadata)).toList();
  }

  protected abstract List<T> deserializeToMultipleTypedEvent(byte[] value) throws Exception;
}
