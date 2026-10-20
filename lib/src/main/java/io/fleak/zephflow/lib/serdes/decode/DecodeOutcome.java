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

  /**
   * The outcome stands for the records from {@code recordIndex} to {@code recordIndex + recordCount
   * - 1}. It is more than 1 only for the records an avro block or a parquet row group loses, and 0
   * when no record is lost (an avro block with no records, or bytes left after a block's records);
   * a terminal outcome counts only its own record.
   */
  default long recordCount() {
    return 1;
  }

  default boolean terminal() {
    return false;
  }

  record Decoded(RecordFleakData record, long recordIndex, RawSpan span) implements DecodeOutcome {}

  record Failed(
      ErrorKind kind,
      byte[] raw,
      long recordIndex,
      long recordCount,
      RawSpan span,
      Exception error,
      boolean terminal)
      implements DecodeOutcome {

    public Failed(
        ErrorKind kind,
        byte[] raw,
        long recordIndex,
        RawSpan span,
        Exception error,
        boolean terminal) {
      this(kind, raw, recordIndex, 1, span, error, terminal);
    }
  }

  /**
   * A record or unit over {@code limit}, whose value was {@code limitValue}. The content is not
   * kept; only a short preview.
   */
  record Oversized(
      long recordIndex,
      long recordCount,
      RawSpan span,
      LimitKind limit,
      long limitValue,
      long sizeSoFar,
      byte[] preview,
      boolean terminal)
      implements DecodeOutcome {}
}
