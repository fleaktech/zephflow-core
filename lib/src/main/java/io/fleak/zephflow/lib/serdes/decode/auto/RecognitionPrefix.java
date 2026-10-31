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
package io.fleak.zephflow.lib.serdes.decode.auto;

import io.fleak.zephflow.lib.commands.filedecode.LocalSpool;
import io.fleak.zephflow.lib.serdes.decode.LimitKind;
import io.fleak.zephflow.lib.serdes.decode.internal.BudgetCaptureStream;
import io.fleak.zephflow.lib.serdes.decode.internal.DecodeFlags;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.RandomAccessFile;

final class RecognitionPrefix implements AutoCloseable {
  private final InputStream source;
  private final long documentLimit;
  private final DecodeFlags flags;
  private final LocalSpool spool;
  private final RandomAccessFile cached;
  private long length;
  private boolean exhausted;

  RecognitionPrefix(InputStream source, long documentLimit, DecodeFlags flags) throws IOException {
    this.source = source;
    this.documentLimit = documentLimit;
    this.flags = flags;
    spool = LocalSpool.create("auto-format-");
    try {
      cached = new RandomAccessFile(spool.getPath().resolve("prefix").toFile(), "rw");
    } catch (IOException exception) {
      spool.close();
      throw exception;
    }
  }

  InputStream probe() {
    return stream(true);
  }

  InputStream replay() {
    return stream(false);
  }

  byte[] whole() throws IOException {
    if (!exhausted) throw new IllegalStateException("The document has not reached its end");
    if (documentLimit > 0 && length > documentLimit) throw exceeded();
    byte[] bytes = new byte[Math.toIntExact(length)];
    cached.seek(0);
    cached.readFully(bytes);
    return bytes;
  }

  private InputStream stream(boolean retain) {
    return new InputStream() {
      private long position;

      @Override
      public int read() throws IOException {
        byte[] one = new byte[1];
        return read(one, 0, 1) < 0 ? -1 : one[0] & 255;
      }

      @Override
      public int read(byte[] bytes, int offset, int count) throws IOException {
        if (count == 0) return 0;
        if (Thread.currentThread().isInterrupted()) {
          var failure = new InterruptedIOException("Format recognition interrupted");
          flags.markRead(failure);
          throw failure;
        }
        if (position < length) {
          cached.seek(position);
          int copied = cached.read(bytes, offset, (int) Math.min(count, length - position));
          position += copied;
          return copied;
        }
        if (exhausted) return -1;
        if (retain && documentLimit > 0) {
          long remaining = documentLimit + BudgetCaptureStream.READ_AHEAD_SLACK - length;
          if (remaining <= 0) throw exceeded();
          count = (int) Math.min(count, remaining);
        }
        int copied = source.read(bytes, offset, count);
        if (copied < 0) {
          exhausted = true;
          return -1;
        }
        if (retain) {
          cached.seek(length);
          cached.write(bytes, offset, copied);
          length += copied;
        }
        position += copied;
        return copied;
      }
    };
  }

  private IOException exceeded() {
    flags.markBudget(
        new DecodeFlags.BudgetExceeded(
            0, LimitKind.MAX_DOCUMENT_BYTES, documentLimit, length, new byte[0]));
    return new IOException("Document recognition exceeds maxDocumentBytes " + documentLimit);
  }

  @Override
  public void close() throws IOException {
    try {
      cached.close();
    } finally {
      spool.close();
    }
  }
}
