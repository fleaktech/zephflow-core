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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.ErrorKind;
import io.fleak.zephflow.lib.serdes.decode.LimitKind;
import io.fleak.zephflow.lib.serdes.decode.RawSpan;
import io.fleak.zephflow.lib.serdes.decode.SourceReadException;
import java.io.IOException;
import org.junit.jupiter.api.Test;

/** The classification rule itself, independent of any format: flags decide, not exceptions. */
class GuardedCursorTest {

  private interface Step {
    DecodeOutcome run(DecodeFlags flags) throws Exception;
  }

  private static GuardedCursor cursor(Step step) {
    return new GuardedCursor(
        flags ->
            new FormatCursor() {
              @Override
              public DecodeOutcome next() throws Exception {
                return step.run(flags);
              }

              @Override
              public long pendingRecordIndex() {
                return 7;
              }

              @Override
              public long pendingOffset() {
                return 42;
              }

              @Override
              public void close() {}
            });
  }

  @Test
  void readFlagWinsEvenWhenAParserWrappedTheFailure() {
    IOException cause = new IOException("reset");
    GuardedCursor cursor =
        cursor(
            flags -> {
              flags.markRead(cause);
              flags.markDecompress(new IOException("later"));
              throw new IllegalStateException("parser wrapped it");
            });
    SourceReadException thrown = assertThrows(SourceReadException.class, cursor::next);
    assertSame(cause, thrown.getCause());
    assertThrows(SourceReadException.class, cursor::next);
  }

  @Test
  void readFlagSetByASwallowingLibraryTurnsACleanEndIntoAReadFailure() {
    GuardedCursor cursor =
        cursor(
            flags -> {
              flags.markRead(new IOException("swallowed"));
              return null;
            });
    assertThrows(SourceReadException.class, cursor::next);
  }

  @Test
  void decompressFlagBeatsBudgetAndMalformed() throws Exception {
    IOException corrupt = new IOException("bad data");
    GuardedCursor cursor =
        cursor(
            flags -> {
              flags.markBudget(
                  new DecodeFlags.BudgetExceeded(
                      0, LimitKind.MAX_RECORD_BYTES, 8, 10, new byte[0]));
              flags.markDecompress(corrupt);
              throw new IllegalArgumentException("parse failed");
            });
    DecodeOutcome.Failed failed = (DecodeOutcome.Failed) cursor.next();
    assertEquals(
        new DecodeOutcome.Failed(
            ErrorKind.CORRUPT_COMPRESSION, failed.raw(), 7, RawSpan.toEnd(42), corrupt, true),
        failed);
    assertArrayEquals(new byte[0], failed.raw());
    assertNull(cursor.next());
  }

  @Test
  void budgetFlagGivesATerminalOversized() throws Exception {
    byte[] preview = {'x'};
    GuardedCursor cursor =
        cursor(
            flags -> {
              flags.markBudget(
                  new DecodeFlags.BudgetExceeded(
                      5, LimitKind.MAX_CODEC_MEMORY_BYTES, 64, 99, preview));
              throw new IOException("over budget");
            });
    assertEquals(
        new DecodeOutcome.Oversized(
            7, 1, RawSpan.toEnd(5), LimitKind.MAX_CODEC_MEMORY_BYTES, 64, 99, preview, true),
        cursor.next());
    assertNull(cursor.next());
  }

  @Test
  void anyOtherExceptionIsMalformedAndTerminal() throws Exception {
    IllegalStateException bug = new IllegalStateException("bug");
    GuardedCursor cursor =
        cursor(
            flags -> {
              throw bug;
            });
    DecodeOutcome.Failed failed = (DecodeOutcome.Failed) cursor.next();
    assertEquals(
        new DecodeOutcome.Failed(
            ErrorKind.MALFORMED, failed.raw(), 7, RawSpan.toEnd(42), bug, true),
        failed);
    assertNull(cursor.next());
  }

  @Test
  void aTerminalOutcomeAfterADecompressFailureIsReportedAsCorrupt() throws Exception {
    IOException corrupt = new IOException("bad data");
    int[] calls = {0};
    DecodeOutcome.Decoded decoded = new DecodeOutcome.Decoded(null, 1, new RawSpan(0, 1));
    GuardedCursor cursor =
        cursor(
            flags -> {
              if (calls[0]++ == 0) {
                flags.markDecompress(corrupt);
                return decoded;
              }
              return new DecodeOutcome.Failed(
                  ErrorKind.MALFORMED, new byte[0], 2, null, new IllegalArgumentException(), true);
            });
    assertSame(decoded, cursor.next());
    DecodeOutcome.Failed failed = (DecodeOutcome.Failed) cursor.next();
    assertEquals(
        new DecodeOutcome.Failed(
            ErrorKind.CORRUPT_COMPRESSION, failed.raw(), 7, RawSpan.toEnd(42), corrupt, true),
        failed);
    assertNull(cursor.next());
  }

  @Test
  void errorsAreNotCaught() {
    GuardedCursor cursor =
        cursor(
            flags -> {
              throw new OutOfMemoryError("real");
            });
    assertThrows(OutOfMemoryError.class, cursor::next);
  }
}
