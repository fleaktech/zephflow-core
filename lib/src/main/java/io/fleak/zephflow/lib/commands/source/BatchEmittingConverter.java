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
package io.fleak.zephflow.lib.commands.source;

import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.List;

/**
 * A converter that emits a fetched record's events itself, in batches, while it converts the
 * record, instead of returning them all at once. {@link SimpleSourceCommand} hands it one fetched
 * record at a time and commits after the record's last batch, so a large record never has to be
 * held whole.
 *
 * <p>The converter owns failure handling for what it emits: an exception from {@link
 * Emitter#accept} is the downstream's and is the converter's to dead-letter. An exception escaping
 * {@link #emit} is dead-lettered with the fetched record, as a failed {@code accept} is for other
 * converters.
 */
public interface BatchEmittingConverter<T> extends RawDataConverter<T> {

  /** Converts {@code sourceRecord}, handing each batch of its events to {@code emitter}. */
  void emit(T sourceRecord, SourceExecutionContext<?> context, Emitter emitter);

  /** Not used: {@link SimpleSourceCommand} calls {@link #emit} for this kind of converter. */
  @Override
  default ConvertedResult<T> convert(T sourceRecord, SourceExecutionContext<?> context) {
    throw new UnsupportedOperationException(
        getClass().getSimpleName() + " emits its own batches; call emit");
  }

  /** Where a {@link BatchEmittingConverter} sends its batches. */
  interface Emitter {

    /** Sends one batch downstream. Throws what the downstream throws. */
    void accept(List<RecordFleakData> batch);

    /**
     * True once the source has been told to stop. The converter checks it between batches and
     * stops; a failure it sees after the stop is a consequence of the stop (closed clients), not
     * reported, and the fetched record is not committed.
     */
    boolean stopped();
  }
}
