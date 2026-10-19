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

import io.fleak.zephflow.lib.serdes.decode.LimitKind;
import java.io.IOException;
import java.io.InputStream;

/**
 * Sits between decompression and a library parser. Keeps the bytes of the current record (for raw
 * and preview) and stops the parser once the record runs past its limit plus room for the parser's
 * read-ahead, so a library never buffers an unbounded token.
 */
public final class BudgetCaptureStream extends InputStream {

  public static final int READ_AHEAD_SLACK = 64 * 1024;

  private final InputStream in;
  private final long maxRecordBytes;
  private final DecodeFlags flags;
  private final boolean locatable;
  private final ByteBuilder retained = new ByteBuilder();
  private long retainedStart;
  private long position;

  public BudgetCaptureStream(InputStream in, long maxRecordBytes, DecodeFlags flags) {
    this(in, maxRecordBytes, flags, true);
  }

  /**
   * @param locatable false when record starts are only known as "the bytes read so far", because
   *     the parser buffers ahead; the budget then reports neither a start offset nor a preview
   */
  public BudgetCaptureStream(
      InputStream in, long maxRecordBytes, DecodeFlags flags, boolean locatable) {
    this.in = in;
    this.maxRecordBytes = maxRecordBytes;
    this.flags = flags;
    this.locatable = locatable;
  }

  /** Starts counting a new record at {@code offset}; bytes before it are dropped. */
  public void startRecord(long offset) {
    if (!locatable) {
      retainedStart = Math.min(offset, position);
      return;
    }
    long drop = Math.min(offset, position) - retainedStart;
    if (drop > 0) {
      retained.dropFront((int) drop);
      retainedStart += drop;
    }
  }

  public long position() {
    return position;
  }

  /** Bytes in [from, to); empty when outside what is retained. */
  public byte[] bytes(long from, long to) {
    if (from < retainedStart || to > position || from > to) {
      return new byte[0];
    }
    return retained.copy((int) (from - retainedStart), (int) (to - retainedStart));
  }

  @Override
  public int read() throws IOException {
    byte[] one = new byte[1];
    int n = read(one, 0, 1);
    return n < 0 ? -1 : one[0] & 0xFF;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    int n = in.read(b, off, len);
    if (n <= 0) {
      return n;
    }
    if (locatable) {
      retained.append(b, off, n);
    }
    position += n;
    long size = position - retainedStart;
    if (maxRecordBytes > 0 && size - READ_AHEAD_SLACK > maxRecordBytes) {
      int keep = locatable ? Math.min(retained.size(), Previews.PREVIEW_BYTES) : 0;
      flags.markBudget(
          new DecodeFlags.BudgetExceeded(
              locatable ? retainedStart : -1,
              LimitKind.MAX_RECORD_BYTES,
              maxRecordBytes,
              size,
              retained.copy(0, keep)));
      retained.clear();
      throw new IOException("record exceeds maxRecordBytes " + maxRecordBytes);
    }
    return n;
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}
