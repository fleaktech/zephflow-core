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
import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

/**
 * Gzip (RFC 1952) with our own member loop: every member of a concatenated stream is read, bytes
 * are handed on as soon as they are inflated, and trailing bytes that are not another member are
 * reported as corrupt instead of being ignored. The JDK and commons-compress readers either swallow
 * errors at member boundaries or drop already-inflated bytes when they fail, so neither is used.
 */
final class GzipMemberStream extends InputStream {

  static final int MAX_HEADER_BYTES = 1024 * 1024;

  private static final int ID1 = 0x1f;
  private static final int ID2 = 0x8b;
  private static final int CM_DEFLATE = 8;
  private static final int FHCRC = 0x02;
  private static final int FEXTRA = 0x04;
  private static final int FNAME = 0x08;
  private static final int FCOMMENT = 0x10;
  private static final int RESERVED_FLAGS = 0xE0;

  private enum State {
    HEADER,
    BODY,
    DONE,
    FAILED
  }

  private final InputStream in;
  private final DecodeFlags flags;
  private final byte[] buf = new byte[64 * 1024];
  private int pos;
  private int limit;
  private final Inflater inflater = new Inflater(true);
  private final CRC32 crc = new CRC32();
  private long memberSize;
  private int members;
  private State state = State.HEADER;

  GzipMemberStream(InputStream in, DecodeFlags flags) {
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
    if (len == 0) {
      return 0;
    }
    while (true) {
      switch (state) {
        case DONE -> {
          return -1;
        }
        case FAILED -> throw new CorruptCompressionException("gzip stream already failed");
        case HEADER -> {
          if (!readHeader()) {
            state = State.DONE;
            return -1;
          }
          state = State.BODY;
        }
        case BODY -> {
          int n = inflate(b, off, len);
          if (n > 0) {
            return n;
          }
        }
      }
    }
  }

  private int inflate(byte[] b, int off, int len) throws IOException {
    while (true) {
      if (inflater.needsInput()) {
        if (pos == limit && !fill()) {
          throw corrupt("gzip member " + (members + 1) + " is truncated");
        }
        inflater.setInput(buf, pos, limit - pos);
        pos = limit;
      }
      long writtenBefore = inflater.getBytesWritten();
      int n;
      try {
        n = inflater.inflate(b, off, len);
      } catch (DataFormatException e) {
        // The inflater still counts what it wrote before hitting the damage; hand those bytes on
        // and fail on the next read, so nothing decoded before the damaged spot is lost.
        int written = (int) (inflater.getBytesWritten() - writtenBefore);
        CorruptCompressionException failure = corrupt("gzip data is corrupt: " + e.getMessage(), e);
        if (written > 0) {
          crc.update(b, off, written);
          return written;
        }
        throw failure;
      }
      if (n > 0) {
        // Hand the bytes on before reading the trailer, so a bad trailer cannot take them along.
        crc.update(b, off, n);
        memberSize += n;
        return n;
      }
      if (inflater.finished()) {
        finishMember();
        return 0;
      }
      if (!inflater.needsInput()) {
        throw corrupt("gzip inflater made no progress");
      }
    }
  }

  private void finishMember() throws IOException {
    pos = limit - inflater.getRemaining();
    long expectedCrc = readLittleEndianInt();
    long expectedSize = readLittleEndianInt();
    if (expectedCrc != crc.getValue()) {
      throw corrupt("gzip member " + (members + 1) + " CRC mismatch");
    }
    if (expectedSize != (memberSize & 0xFFFFFFFFL)) {
      throw corrupt("gzip member " + (members + 1) + " size mismatch");
    }
    members++;
    inflater.reset();
    crc.reset();
    memberSize = 0;
    state = State.HEADER;
  }

  /** False at a clean end: no bytes at all, or nothing after the last member. */
  private boolean readHeader() throws IOException {
    int first = readByte();
    if (first < 0) {
      return false;
    }
    int second = readByte();
    if (first != ID1 || second != ID2) {
      throw corrupt(
          members == 0
              ? "input is not in gzip format"
              : "unexpected bytes after gzip member " + members);
    }
    int headerBytes = 2;
    int method = requireByte();
    int flg = requireByte();
    headerBytes += 2;
    if (method != CM_DEFLATE) {
      throw corrupt("unsupported gzip compression method " + method);
    }
    if ((flg & RESERVED_FLAGS) != 0) {
      throw corrupt("gzip header has reserved flags set");
    }
    for (int i = 0; i < 6; i++) {
      requireByte();
    }
    headerBytes += 6;
    if ((flg & FEXTRA) != 0) {
      int xlen = requireByte() | (requireByte() << 8);
      headerBytes += 2 + xlen;
      checkHeaderSize(headerBytes);
      for (int i = 0; i < xlen; i++) {
        requireByte();
      }
    }
    if ((flg & FNAME) != 0) {
      headerBytes = skipZeroTerminated(headerBytes);
    }
    if ((flg & FCOMMENT) != 0) {
      headerBytes = skipZeroTerminated(headerBytes);
    }
    if ((flg & FHCRC) != 0) {
      requireByte();
      requireByte();
    }
    return true;
  }

  private int skipZeroTerminated(int headerBytes) throws IOException {
    int count = headerBytes;
    while (requireByte() != 0) {
      checkHeaderSize(++count);
    }
    return count + 1;
  }

  private void checkHeaderSize(int headerBytes) throws IOException {
    if (headerBytes > MAX_HEADER_BYTES) {
      throw corrupt("gzip header is longer than " + MAX_HEADER_BYTES + " bytes");
    }
  }

  private long readLittleEndianInt() throws IOException {
    long value = 0;
    for (int i = 0; i < 4; i++) {
      value |= (long) requireByte() << (8 * i);
    }
    return value;
  }

  private int requireByte() throws IOException {
    int b = readByte();
    if (b < 0) {
      throw corrupt("gzip member " + (members + 1) + " is truncated");
    }
    return b;
  }

  private int readByte() throws IOException {
    if (pos == limit && !fill()) {
      return -1;
    }
    return buf[pos++] & 0xFF;
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

  private CorruptCompressionException corrupt(String message) {
    return corrupt(message, null);
  }

  private CorruptCompressionException corrupt(String message, Throwable cause) {
    state = State.FAILED;
    CorruptCompressionException e = new CorruptCompressionException(message, cause);
    flags.markDecompress(e);
    return e;
  }

  @Override
  public void close() throws IOException {
    inflater.end();
    in.close();
  }
}
