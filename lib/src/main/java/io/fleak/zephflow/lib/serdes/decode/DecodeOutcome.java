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

/**
 * One result of a {@link RecordCursor}. When {@code terminal} is true the cursor ends after this
 * outcome.
 */
public sealed interface DecodeOutcome {

  long recordIndex();

  /** Null when the format cannot locate the record in the decompressed stream. */
  RawSpan span();

  default boolean terminal() {
    return false;
  }

  record Decoded(RecordFleakData record, long recordIndex, RawSpan span) implements DecodeOutcome {}

  record Failed(
      ErrorKind kind, byte[] raw, long recordIndex, RawSpan span, Exception error, boolean terminal)
      implements DecodeOutcome {}

  /** A record or unit over its limit. The content is not kept; only a short preview. */
  record Oversized(long recordIndex, RawSpan span, long sizeSoFar, byte[] preview, boolean terminal)
      implements DecodeOutcome {}
}
