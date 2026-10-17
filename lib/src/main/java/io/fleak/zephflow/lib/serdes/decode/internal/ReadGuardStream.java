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

import io.fleak.zephflow.lib.serdes.decode.SourceReadException;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.locks.LockSupport;

/** Any failure of the caller's stream is a read failure, whatever its type. */
public final class ReadGuardStream extends FilterInputStream {

  private static final int MAX_EMPTY_READS = 1000;
  private static final long EMPTY_READ_PAUSE_NANOS = 1_000_000;

  private final DecodeFlags flags;

  public ReadGuardStream(InputStream in, DecodeFlags flags) {
    super(in);
    this.flags = flags;
  }

  @Override
  public int read() throws IOException {
    failIfAlreadyFailed();
    try {
      return in.read();
    } catch (IOException | RuntimeException e) {
      throw fail(e);
    }
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    failIfAlreadyFailed();
    try {
      // A read that returns 0 breaks the InputStream contract; retry instead of letting a
      // reader mistake it for the end of the input.
      for (int attempt = 0; attempt < MAX_EMPTY_READS; attempt++) {
        int n = in.read(b, off, len);
        if (n != 0 || len == 0) {
          return n;
        }
        LockSupport.parkNanos(EMPTY_READ_PAUSE_NANOS);
      }
      throw new IOException("the input returned no bytes " + MAX_EMPTY_READS + " times in a row");
    } catch (IOException | RuntimeException e) {
      throw fail(e);
    }
  }

  @Override
  public long skip(long n) throws IOException {
    failIfAlreadyFailed();
    try {
      return in.skip(n);
    } catch (IOException | RuntimeException e) {
      throw fail(e);
    }
  }

  @Override
  public int available() {
    return 0;
  }

  @Override
  public boolean markSupported() {
    return false;
  }

  @Override
  public void close() {
    try {
      in.close();
    } catch (IOException | RuntimeException ignored) {
      // Closing after the bytes were consumed cannot lose data.
    }
  }

  private void failIfAlreadyFailed() throws SourceReadException {
    if (flags.getReadFailure() != null) {
      throw new SourceReadException("input already failed", flags.getReadFailure());
    }
  }

  private SourceReadException fail(Exception e) {
    flags.markRead(e);
    return e instanceof SourceReadException sre
        ? sre
        : new SourceReadException("reading the input failed: " + e.getMessage(), e);
  }
}
