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
package io.fleak.zephflow.lib.serdes.decode;

import io.fleak.zephflow.api.structure.RecordFleakData;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/** Compiled from a DecodingConfig. Immutable and thread-safe; compile once, reuse. */
public interface RecordDecoder {

  /** Reads no bytes. The input is opened on the first {@link RecordCursor#next()}. */
  RecordCursor open(ByteInput input, DecodeLimits limits);

  default DecodeResult decodeAll(byte[] bytes, DecodeLimits limits) {
    List<RecordFleakData> records = new ArrayList<>();
    List<DecodeOutcome> problems = new ArrayList<>();
    try (RecordCursor cursor = open(ByteInput.ofBytes(bytes), limits)) {
      DecodeOutcome outcome;
      while ((outcome = cursor.next()) != null) {
        if (outcome instanceof DecodeOutcome.Decoded decoded) {
          records.add(decoded.record());
        } else {
          problems.add(outcome);
        }
      }
    } catch (SourceReadException e) {
      throw new UncheckedIOException(e);
    }
    return new DecodeResult(records, problems);
  }
}
