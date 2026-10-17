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
package io.fleak.zephflow.lib.serdes.decode.csv;

import io.fleak.zephflow.lib.serdes.decode.internal.ByteBuilder;
import io.fleak.zephflow.lib.serdes.decode.internal.Previews;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Splits UTF-8 bytes into CSV records. All special characters are ASCII and never occur inside a
 * multi-byte UTF-8 sequence, so the scan works on bytes and every record has an exact span. A
 * record over the limit is scanned to its end without being kept, so the next record is read
 * normally; only a quote left open until the end of the input makes it terminal.
 */
final class CsvScanner implements AutoCloseable {

  sealed interface Frame {}

  /** {@code problem} is null for a well-formed record. */
  record Row(List<byte[]> fields, long offset, byte[] bytes, String problem) implements Frame {}

  record OversizedRow(long offset, long size, byte[] preview, boolean terminal) implements Frame {}

  record UnclosedQuote(long offset, byte[] bytes) implements Frame {}

  private enum State {
    FIELD_START,
    UNQUOTED,
    ESCAPED_UNQUOTED,
    QUOTED,
    ESCAPED_QUOTED,
    QUOTE_SEEN,
    AFTER_QUOTE,
    COMMENT
  }

  private static final int CHUNK = 64 * 1024;
  private static final byte[] UTF8_BOM = {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF};

  private final InputStream in;
  private final CsvDialect dialect;
  private final long maxRecordBytes;
  private final boolean skipBom;
  private final byte[] chunk = new byte[CHUNK];
  private int pos;
  private int limit;
  private long chunkOffset;
  private boolean eof;
  private boolean started;
  private boolean skipLeadingLf;
  private long recordStart;

  private final List<byte[]> fields = new ArrayList<>();
  private final ByteBuilder field = new ByteBuilder();
  private final ByteBuilder raw = new ByteBuilder();
  private final ByteBuilder preview = new ByteBuilder();
  private int keep;
  private long size;
  private boolean oversized;
  private String problem;

  CsvScanner(InputStream in, CsvDialect dialect, long maxRecordBytes, boolean skipBom) {
    this.in = in;
    this.dialect = dialect;
    this.maxRecordBytes = maxRecordBytes;
    this.skipBom = skipBom;
  }

  /** The next record, or null at the end. Empty lines and comment lines are skipped. */
  Frame next() throws IOException {
    if (!started) {
      started = true;
      if (skipBom) {
        skipBom();
      }
      skipLines(dialect.skipLines());
    }
    while (true) {
      if (skipLeadingLf) {
        skipLeadingLf = false;
        if (peek() == '\n') {
          pos++;
        }
      }
      recordStart = offset();
      reset();
      Frame frame = scan();
      if (frame != null || atEnd()) {
        recordStart = offset();
        return frame;
      }
    }
  }

  /** Offset of the first byte of the record being read, or of the next one between records. */
  long pendingOffset() {
    return recordStart;
  }

  /** Null for an empty or comment line, which the caller skips. */
  private Frame scan() throws IOException {
    State state = State.FIELD_START;
    boolean any = false;
    while (true) {
      int b = nextByte();
      if (b < 0) {
        return switch (state) {
          case COMMENT -> null;
          case QUOTED, ESCAPED_QUOTED -> unclosed();
          case ESCAPED_UNQUOTED -> {
            problem = "escape character at the end of the input";
            endField();
            yield row();
          }
          default -> {
            if (!any) {
              yield null;
            }
            endField();
            yield row();
          }
        };
      }
      if (state == State.COMMENT) {
        if (b == '\n' || b == '\r') {
          skipLeadingLf = b == '\r';
          return null;
        }
        continue;
      }
      if ((b == '\n' || b == '\r')
          && state != State.QUOTED
          && state != State.ESCAPED_QUOTED
          && state != State.ESCAPED_UNQUOTED) {
        skipLeadingLf = b == '\r';
        if (!any) {
          return null;
        }
        endField();
        return row();
      }
      if (!any && b == dialect.commentMarker()) {
        state = State.COMMENT;
        continue;
      }
      any = true;
      consume(b);
      state = step(state, b);
    }
  }

  private State step(State state, int b) {
    return switch (state) {
      case FIELD_START -> {
        if (b == dialect.quote()) {
          yield State.QUOTED;
        }
        if (b == dialect.delimiter()) {
          endField();
          yield State.FIELD_START;
        }
        if (b == dialect.escape()) {
          yield State.ESCAPED_UNQUOTED;
        }
        if (dialect.ignoreSurroundingSpaces() && isSpace(b)) {
          yield State.FIELD_START;
        }
        add(b, false);
        yield State.UNQUOTED;
      }
      case UNQUOTED -> {
        if (b == dialect.delimiter()) {
          endField();
          yield State.FIELD_START;
        }
        if (b == dialect.escape()) {
          yield State.ESCAPED_UNQUOTED;
        }
        add(b, false);
        yield State.UNQUOTED;
      }
      case ESCAPED_UNQUOTED -> {
        add(b, true);
        yield State.UNQUOTED;
      }
      case QUOTED -> {
        if (b == dialect.quote()) {
          yield State.QUOTE_SEEN;
        }
        if (b == dialect.escape()) {
          yield State.ESCAPED_QUOTED;
        }
        add(b, true);
        yield State.QUOTED;
      }
      case ESCAPED_QUOTED -> {
        add(b, true);
        yield State.QUOTED;
      }
      case QUOTE_SEEN, AFTER_QUOTE -> {
        if (state == State.QUOTE_SEEN && b == dialect.quote()) {
          add(b, true);
          yield State.QUOTED;
        }
        if (b == dialect.delimiter()) {
          endField();
          yield State.FIELD_START;
        }
        if (dialect.ignoreSurroundingSpaces() && isSpace(b)) {
          yield State.AFTER_QUOTE;
        }
        if (problem == null) {
          problem = "unexpected character after a closing quote";
        }
        add(b, false);
        yield State.UNQUOTED;
      }
      case COMMENT -> State.COMMENT;
    };
  }

  private boolean isSpace(int b) {
    return (b == ' ' || b == '\t') && b != dialect.delimiter();
  }

  /** Counts a byte of the record; once the record is over the limit only a preview is kept. */
  private void consume(int b) {
    size++;
    if (!oversized && maxRecordBytes > 0 && size > maxRecordBytes) {
      oversized = true;
      int kept = Math.min(raw.size(), Previews.PREVIEW_BYTES);
      preview.append(raw.copy(0, kept), 0, kept);
      raw.clear();
      field.clear();
      fields.clear();
    }
    if (!oversized) {
      raw.append(b);
    } else if (preview.size() < Previews.PREVIEW_BYTES) {
      preview.append(b);
    }
  }

  /** {@code kept} bytes are never trimmed as surrounding spaces. */
  private void add(int b, boolean kept) {
    if (oversized) {
      return;
    }
    field.append(b);
    if (kept || !isSpace(b)) {
      keep = field.size();
    }
  }

  private void endField() {
    if (!oversized) {
      fields.add(field.copy(0, dialect.ignoreSurroundingSpaces() ? keep : field.size()));
    }
    field.clear();
    keep = 0;
  }

  private Frame row() {
    if (oversized) {
      return new OversizedRow(recordStart, size, preview.toByteArray(), false);
    }
    return new Row(List.copyOf(fields), recordStart, raw.toByteArray(), problem);
  }

  private Frame unclosed() {
    if (oversized) {
      return new OversizedRow(recordStart, size, preview.toByteArray(), true);
    }
    return new UnclosedQuote(recordStart, raw.toByteArray());
  }

  private void reset() {
    fields.clear();
    field.clear();
    raw.clear();
    preview.clear();
    keep = 0;
    size = 0;
    oversized = false;
    problem = null;
  }

  private void skipLines(int count) throws IOException {
    for (int i = 0; i < count; i++) {
      int b;
      while ((b = nextByte()) >= 0 && b != '\n' && b != '\r') {
        // Skipped line content.
      }
      if (b < 0) {
        return;
      }
      if (b == '\r' && peek() == '\n') {
        pos++;
      }
    }
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
    if (limit >= UTF8_BOM.length && prefixMatchesBom()) {
      pos = UTF8_BOM.length;
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

  private long offset() {
    return chunkOffset + pos;
  }

  private boolean atEnd() throws IOException {
    return peek() < 0;
  }

  private int peek() throws IOException {
    while (pos == limit && !eof) {
      fill();
    }
    return pos < limit ? chunk[pos] & 0xFF : -1;
  }

  private int nextByte() throws IOException {
    int b = peek();
    if (b >= 0) {
      pos++;
    }
    return b;
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

  @Override
  public void close() {
    try {
      in.close();
    } catch (IOException ignored) {
      // Nothing left to read.
    }
  }
}
