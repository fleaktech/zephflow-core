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
import io.fleak.zephflow.lib.serdes.FleakSerdes;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.converters.TypedEventConverter;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/** Created by bolei on 9/16/24 */
public abstract class FleakDeserializer<T> extends FleakSerdes<T> {

  protected FleakDeserializer(
      EncodingType encodingType, TypedEventConverter<T> typedEventConverter) {
    super(encodingType, typedEventConverter);
  }

  public abstract List<RecordFleakData> deserialize(SerializedEvent serializedEvent)
      throws Exception;

  /**
   * Emits ordered outcomes without collecting a multi-record payload. Stop is checked before the
   * next parse/output. Consumer exceptions propagate and are never classified as decoding failures.
   * Multi-record implementations must override this entry point rather than call their list API.
   */
  public void deserializeIncrementally(
      SerializedEvent event, Consumer<IncrementalRecord> consumer, BooleanSupplier stopRequested) {
    throw new UnsupportedOperationException(
        "Incremental decoding requires an explicit implementation");
  }

  /**
   * Deserializes without throwing on malformed input: returns the records that parsed plus an error
   * per record that didn't, so a caller can emit the good records and quarantine the bad ones.
   *
   * <p>The default treats the payload as one indivisible unit. Line-oriented formats override this
   * to report per-line errors.
   */
  public DeserializationOutcome deserializeWithErrors(SerializedEvent serializedEvent) {
    try {
      return DeserializationOutcome.success(deserialize(serializedEvent));
    } catch (Exception e) {
      return DeserializationOutcome.wholePayloadFailure(serializedEvent.value(), e);
    }
  }

  /**
   * Whether this format can be deserialized one newline-delimited chunk at a time. Only formats
   * where a line is an independent record can; whole-document formats (json array, xml, csv) need
   * the entire payload at once.
   */
  public boolean supportsChunkedPayloads() {
    return false;
  }
}
