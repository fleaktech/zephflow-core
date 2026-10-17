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

import java.io.IOException;
import java.io.InputStream;

/**
 * Splits bytes into lines on LF, CRLF and CR. A line over the limit is not kept: its first bytes
 * become a preview and the rest is counted and discarded until the next line ending, so memory
 * stays bounded and the next line is read normally.
 */
public final class LineFramer implements AutoCloseable {

  public sealed interface Frame {}

  /** A line without its terminator. */
  public record Line(byte[] bytes, long lineNumber, long offset) implements Frame {}

  public record OversizedLine(long lineNumber, long offset, long size, byte[] preview)
      implements Frame {}

  private static final int CHUNK = 64 * 1024;
  private static final byte[] UTF8_BOM = {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};

  private final InputStream in;
  private final long maxRecordBytes;
  private final byte[] chunk = new byte[CHUNK];
  private int pos;
  private int limit;
  private long chunkOffset;
  private boolean eof;
  private boolean started;
  private boolean skipLeadingLf;

  private final ByteBuilder line = new ByteBuilder();
  private final ByteBuilder preview = new ByteBuilder();
  private long lineSize;
  private boolean oversized;
  private long lineStart;
  private long lineNumber = 1;

  public LineFramer(InputStream in, long maxRecordBytes) {
    this.in = in;
    this.maxRecordBytes = maxRecordBytes;
  }

  /** The next line, an oversized line, or null at the end. */
  public Frame next() throws IOException {
    if (!started) {
      started = true;
      skipBom();
    }
    while (true) {
      if (pos == limit) {
        if (eof) {
          return lineSize > 0 || oversized ? emit() : null;
        }
        fill();
        continue;
      }
      if (skipLeadingLf) {
        skipLeadingLf = false;
        if (chunk[pos] == '\n') {
          pos++;
          lineStart++;
          continue;
        }
      }
      int end = pos;
      while (end < limit && chunk[end] != '\n' && chunk[end] != '\r') {
        end++;
      }
      append(pos, end - pos);
      pos = end;
      if (end == limit) {
        continue;
      }
      byte terminator = chunk[pos++];
      long afterTerminator = chunkOffset + pos;
      if (terminator == '\r') {
        if (pos < limit) {
          if (chunk[pos] == '\n') {
            pos++;
            afterTerminator++;
          }
        } else {
          skipLeadingLf = true;
        }
      }
      Frame frame = emit();
      lineStart = afterTerminator;
      return frame;
    }
  }

  public long pendingLineNumber() {
    return lineNumber;
  }

  public long pendingOffset() {
    return lineStart;
  }

  /** Reads only as far as needed to tell, so no bytes past a failure point are requested. */
  private void skipBom() throws IOException {
    while (limit < UTF8_BOM.length && !eof && prefixMatchesBom()) {
      int n = in.read(chunk, limit, chunk.length - limit);
      if (n < 0) {
        eof = true;
      } else {
        limit += n;
      }
    }
    if (limit >= UTF8_BOM.length
        && chunk[0] == UTF8_BOM[0]
        && chunk[1] == UTF8_BOM[1]
        && chunk[2] == UTF8_BOM[2]) {
      pos = UTF8_BOM.length;
      lineStart = UTF8_BOM.length;
    }
  }

  private boolean prefixMatchesBom() {
    for (int i = 0; i < limit && i < UTF8_BOM.length; i++) {
      if (chunk[i] != UTF8_BOM[i]) {
        return false;
      }
    }
    return true;
  }

  private void fill() throws IOException {
    chunkOffset += limit;
    pos = 0;
    limit = 0;
    int n = in.read(chunk, 0, chunk.length);
    if (n < 0) {
      eof = true;
    } else {
      limit = n;
    }
  }

  private void append(int from, int len) {
    if (len == 0) {
      return;
    }
    if (!oversized && maxRecordBytes > 0 && lineSize + len > maxRecordBytes) {
      oversized = true;
      int keep = (int) Math.min(line.size(), Previews.PREVIEW_BYTES);
      preview.append(line.copy(0, keep), 0, keep);
      line.clear();
    }
    if (oversized) {
      int room = Previews.PREVIEW_BYTES - preview.size();
      if (room > 0) {
        preview.append(chunk, from, Math.min(room, len));
      }
    } else {
      line.append(chunk, from, len);
    }
    lineSize += len;
  }

  private Frame emit() {
    Frame frame =
        oversized
            ? new OversizedLine(lineNumber, lineStart, lineSize, preview.toByteArray())
            : new Line(line.toByteArray(), lineNumber, lineStart);
    lineNumber++;
    lineSize = 0;
    oversized = false;
    line.clear();
    preview.clear();
    return frame;
  }

  @Override
  public void close() {
    try {
      in.close();
    } catch (IOException ignored) {
      // Nothing left to read.
    }
  }
}
