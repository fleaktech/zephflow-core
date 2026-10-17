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
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Failed;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Oversized;
import io.fleak.zephflow.lib.serdes.decode.ErrorKind;
import io.fleak.zephflow.lib.serdes.decode.RawSpan;
import io.fleak.zephflow.lib.serdes.decode.RecordCursor;
import io.fleak.zephflow.lib.serdes.decode.SourceReadException;

/**
 * The one place that applies the classification rule. When the format throws, reaches the end, or
 * gives up with a terminal outcome, the flags decide in priority order read, decompress, budget;
 * otherwise the failure is malformed data. Records decoded from bytes delivered before a failure
 * pass through unchanged, so nothing that decoded is withheld.
 */
public final class GuardedCursor implements RecordCursor {

  /** Creates the format cursor, opening the input. Called on the first {@link #next()}. */
  @FunctionalInterface
  public interface Starter {
    FormatCursor start(DecodeFlags flags) throws Exception;
  }

  private static final byte[] EMPTY = new byte[0];

  private final Starter starter;
  private final DecodeFlags flags = new DecodeFlags();
  private FormatCursor format;
  private boolean done;

  public GuardedCursor(Starter starter) {
    this.starter = starter;
  }

  @Override
  public DecodeOutcome next() throws SourceReadException {
    if (done) {
      if (flags.getReadFailure() != null) {
        throw new SourceReadException("reading the input failed", flags.getReadFailure());
      }
      return null;
    }
    try {
      if (format == null) {
        format = starter.start(flags);
      }
      DecodeOutcome outcome = format.next();
      if (outcome == null) {
        done = true;
        return fromFlags(null);
      }
      if (outcome.terminal()) {
        done = true;
        DecodeOutcome flagged = fromFlags(null);
        if (flagged != null) {
          return flagged;
        }
      }
      return outcome;
    } catch (Exception e) {
      done = true;
      DecodeOutcome outcome = fromFlags(e);
      return outcome != null ? outcome : malformed(e);
    }
  }

  private DecodeOutcome fromFlags(Exception cause) throws SourceReadException {
    if (flags.getReadFailure() != null) {
      throw cause instanceof SourceReadException sre
          ? sre
          : new SourceReadException("reading the input failed", flags.getReadFailure());
    }
    if (flags.getDecompressFailure() != null) {
      Throwable failure = flags.getDecompressFailure();
      return new Failed(
          ErrorKind.CORRUPT_COMPRESSION,
          EMPTY,
          pendingRecordIndex(),
          pendingSpan(),
          failure instanceof Exception ex ? ex : new Exception(failure),
          true);
    }
    if (flags.getBudgetExceeded() != null) {
      DecodeFlags.BudgetExceeded exceeded = flags.getBudgetExceeded();
      return new Oversized(
          pendingRecordIndex(),
          exceeded.recordStart() < 0 ? null : RawSpan.toEnd(exceeded.recordStart()),
          exceeded.sizeSoFar(),
          exceeded.preview(),
          true);
    }
    return null;
  }

  private Failed malformed(Exception e) {
    return new Failed(ErrorKind.MALFORMED, EMPTY, pendingRecordIndex(), pendingSpan(), e, true);
  }

  private long pendingRecordIndex() {
    return format == null ? 1 : format.pendingRecordIndex();
  }

  private RawSpan pendingSpan() {
    long offset = format == null ? 0 : format.pendingOffset();
    return offset < 0 ? null : RawSpan.toEnd(offset);
  }

  @Override
  public void close() {
    done = true;
    if (format != null) {
      format.close();
    }
  }
}
