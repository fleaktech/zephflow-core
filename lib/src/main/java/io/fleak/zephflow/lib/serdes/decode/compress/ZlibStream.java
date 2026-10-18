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
package io.fleak.zephflow.lib.serdes.decode.compress;

import io.fleak.zephflow.lib.serdes.decode.internal.DecodeFlags;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * The zlib format (RFC 1950) with the same rules as gzip: streams that follow each other are all
 * read, bytes after the last stream that do not start another are corrupt, and what the inflater
 * wrote before damaged data is handed on first. A stream that needs a preset dictionary is corrupt,
 * since none can be supplied.
 */
final class ZlibStream extends InputStream {

  private final InputStream in;
  private final DecodeFlags flags;
  private final byte[] buf = new byte[64 * 1024];
  private int pos;
  private int limit;
  private final Inflater inflater = new Inflater();
  private int streams;
  private boolean started;
  private boolean done;
  private IOException failure;

  ZlibStream(InputStream in, DecodeFlags flags) {
    this.in = in;
    this.flags = flags;
  }

  @Override
  public int read() throws IOException {
    byte[] one = new byte[1];
    int n = read(one, 0, 1);
    return n < 0 ? -1 : one[0] & 0xFF;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (failure != null) {
      throw failure;
    }
    if (len == 0 || done) {
      return done ? -1 : 0;
    }
    while (true) {
      if (!started) {
        if (!startStream()) {
          done = true;
          return -1;
        }
        started = true;
      }
      if (inflater.finished()) {
        pos = limit - inflater.getRemaining();
        streams++;
        inflater.reset();
        started = false;
        continue;
      }
      if (inflater.needsInput()) {
        if (pos == limit && !fill()) {
          throw corrupt("zlib stream " + (streams + 1) + " is truncated", null);
        }
        inflater.setInput(buf, pos, limit - pos);
        pos = limit;
      }
      long writtenBefore = inflater.getBytesWritten();
      int n;
      try {
        n = inflater.inflate(b, off, len);
      } catch (DataFormatException e) {
        int written = (int) (inflater.getBytesWritten() - writtenBefore);
        CorruptCompressionException corrupt = corrupt("zlib data is corrupt: " + e.getMessage(), e);
        if (written > 0) {
          return written;
        }
        throw corrupt;
      }
      if (n > 0) {
        return n;
      }
      if (inflater.needsDictionary()) {
        throw corrupt("zlib stream " + (streams + 1) + " needs a preset dictionary", null);
      }
      if (!inflater.finished() && !inflater.needsInput()) {
        throw corrupt("zlib inflater made no progress", null);
      }
    }
  }

  /** False at a clean end; otherwise checks that the next two bytes are a zlib header. */
  private boolean startStream() throws IOException {
    while (limit - pos < 2) {
      if (!fillKeeping()) {
        if (pos == limit) {
          return false;
        }
        throw corrupt(trailing(), null);
      }
    }
    int cmf = buf[pos] & 0xFF;
    int flg = buf[pos + 1] & 0xFF;
    if ((cmf & 0x0F) != 8 || ((cmf << 8) | flg) % 31 != 0) {
      throw corrupt(trailing(), null);
    }
    return true;
  }

  private String trailing() {
    return streams == 0
        ? "input is not in zlib format"
        : "unexpected bytes after zlib stream " + streams;
  }

  private boolean fill() throws IOException {
    int n = in.read(buf, 0, buf.length);
    if (n <= 0) {
      pos = 0;
      limit = 0;
      return false;
    }
    pos = 0;
    limit = n;
    return true;
  }

  /** Appends to what is left in the buffer instead of replacing it. */
  private boolean fillKeeping() throws IOException {
    if (pos > 0) {
      System.arraycopy(buf, pos, buf, 0, limit - pos);
      limit -= pos;
      pos = 0;
    }
    int n = in.read(buf, limit, buf.length - limit);
    if (n <= 0) {
      return false;
    }
    limit += n;
    return true;
  }

  private CorruptCompressionException corrupt(String message, Throwable cause) {
    CorruptCompressionException e = new CorruptCompressionException(message, cause);
    flags.markDecompress(e);
    failure = e;
    return e;
  }

  @Override
  public void close() throws IOException {
    inflater.end();
    in.close();
  }
}
