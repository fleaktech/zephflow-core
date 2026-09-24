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
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/** Created by bolei on 9/16/24 */
public abstract class MultipleEventsTypedDeserializer<T> {
  /** Typed decoding outcome; raw ranges refer to the unchanged input bytes. */
  public record TypedOutcome<T>(
      T value, int index, int rawOffset, int rawLength, Exception error) {}

  /**
   * One record read from a stream: exactly one of {@code value} and {@code error} is non-null.
   *
   * @param index 1-based record number within the payload, or {@code -1} when the fault is not
   *     about any one record (for example, the document is not an array at all)
   * @param raw the failing record's own text, for dead-letter quarantine; null for a record that
   *     parsed, and empty when malformed input leaves no record to point at
   */
  public record StreamedOutcome<T>(T value, int index, byte[] raw, Exception error) {}

  /**
   * Whether each newline-delimited line is an independent record, so a payload can be split at any
   * newline and each piece parsed on its own.
   */
  public boolean isLineDelimited() {
    return false;
  }

  /** Whether {@link #deserializeStream} is implemented. */
  public boolean supportsStreaming() {
    return false;
  }

  /**
   * Parses records one at a time straight from {@code input}, so memory is bounded by one record
   * rather than the payload.
   *
   * <p>Malformed input is reported to {@code consumer} as an error outcome, never thrown: a bad
   * record is reported and parsing moves on to the next one, and input the parser cannot recover
   * from is reported once and ends the parse, keeping every record already delivered. A failure
   * reading {@code input} is thrown, as is anything {@code consumer} throws.
   */
  public void deserializeStream(InputStream input, Consumer<StreamedOutcome<T>> consumer)
      throws IOException {
    throw new UnsupportedOperationException("Streamed decoding is not implemented for this format");
  }

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
