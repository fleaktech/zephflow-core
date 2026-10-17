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

import io.fleak.zephflow.lib.serdes.decode.RandomAccessInput;
import io.fleak.zephflow.lib.serdes.decode.SourceReadException;
import java.io.IOException;
import java.io.InputStream;

/**
 * Any failure of the caller's random access is a read failure, and so is a range that ends early.
 * Ranges are checked against the length before they are requested.
 */
final class GuardedRandomAccess implements RandomAccessInput {

  private final RandomAccessInput in;
  private final DecodeFlags flags;
  private long length = -1;

  GuardedRandomAccess(RandomAccessInput in, DecodeFlags flags) {
    this.in = in;
    this.flags = flags;
  }

  @Override
  public long length() throws SourceReadException {
    if (length < 0) {
      long reported;
      try {
        reported = in.length();
      } catch (IOException | RuntimeException e) {
        throw fail("reading the input length failed: " + e.getMessage(), e);
      }
      if (reported < 0) {
        throw fail("the input reported a negative length: " + reported, null);
      }
      length = reported;
    }
    return length;
  }

  @Override
  public InputStream read(long offset, long count) throws SourceReadException {
    if (offset < 0 || count < 0 || offset > length() - count) {
      throw new IllegalArgumentException(
          "range " + offset + "+" + count + " is outside the input of " + length() + " bytes");
    }
    InputStream range;
    try {
      range = in.read(offset, count);
    } catch (IOException | RuntimeException e) {
      throw fail("reading the input failed: " + e.getMessage(), e);
    }
    if (range == null) {
      throw fail("the input returned no stream for range " + offset + "+" + count, null);
    }
    return new ExactRangeStream(new ReadGuardStream(range, flags), count, flags);
  }

  private SourceReadException fail(String message, Exception cause) {
    SourceReadException failure = new SourceReadException(message, cause);
    flags.markRead(failure);
    return failure;
  }

  /** Stops after the range and treats an early end as a read failure. */
  private static final class ExactRangeStream extends InputStream {

    private final InputStream in;
    private final DecodeFlags flags;
    private long left;

    ExactRangeStream(InputStream in, long count, DecodeFlags flags) {
      this.in = in;
      this.left = count;
      this.flags = flags;
    }

    @Override
    public int read() throws IOException {
      byte[] one = new byte[1];
      return read(one, 0, 1) < 0 ? -1 : one[0] & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (len == 0) {
        return 0;
      }
      if (left == 0) {
        return -1;
      }
      int n = in.read(b, off, (int) Math.min(len, left));
      if (n < 0) {
        SourceReadException failure =
            new SourceReadException("the input range ended " + left + " bytes early", null);
        flags.markRead(failure);
        throw failure;
      }
      left -= n;
      return n;
    }

    @Override
    public void close() throws IOException {
      in.close();
    }
  }
}
