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
package io.fleak.zephflow.lib.serdes.decode.internal;

import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;

/**
 * A format's own iteration. It may throw anything; {@link GuardedCursor} turns exceptions into
 * outcomes using the flags and the pending position reported here.
 */
public interface FormatCursor extends AutoCloseable {

  /** The next outcome, or null at the end of the input. */
  DecodeOutcome next() throws Exception;

  /** recordIndex of the first record not yet returned. */
  long pendingRecordIndex();

  /** Offset of the first byte of the first record not yet returned, or -1 when unknown. */
  long pendingOffset();

  default long normalizedEndOffset() {
    return -1;
  }

  @Override
  void close();
}
