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
package io.fleak.zephflow.lib.serdes.decode.text;

import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Decoded;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Oversized;
import io.fleak.zephflow.lib.serdes.decode.LimitKind;
import io.fleak.zephflow.lib.serdes.decode.RawSpan;
import io.fleak.zephflow.lib.serdes.decode.internal.FormatCursor;
import io.fleak.zephflow.lib.serdes.decode.internal.LineFramer;
import io.fleak.zephflow.lib.serdes.decode.internal.LineFramer.Frame;
import io.fleak.zephflow.lib.serdes.decode.internal.LineFramer.Line;
import io.fleak.zephflow.lib.serdes.decode.internal.LineFramer.OversizedLine;
import io.fleak.zephflow.lib.serdes.decode.internal.Previews;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Joins lines into events. An event keeps its inner line endings as they were, so its record and
 * span cover the same bytes. An event is only complete once the next one starts or the input ends;
 * until then it is pending, and a failure reports its first byte as the start of the lost range. An
 * event over the byte or line limit is reported whole as oversized, and the lines that continue it
 * are counted into it instead of becoming events of their own. Events of blank lines only are
 * skipped unless they are oversized.
 */
public final class TextMultilineFormat implements FormatCursor {

  private final InputStream in;
  private final Charset charset;
  private final MultilineRule rule;
  private final long maxRecordBytes;
  private LineFramer framer;
  private Event pending;

  public TextMultilineFormat(
      InputStream in, DecodeLimits limits, Charset charset, MultilineRule rule) {
    this.in = in;
    this.charset = charset;
    this.rule = rule;
    this.maxRecordBytes = limits.maxRecordBytes();
  }

  @Override
  public DecodeOutcome next() throws IOException {
    if (framer == null) {
      framer = TextInput.lineFramer(in, charset, maxRecordBytes);
    }
    while (true) {
      Frame frame = framer.next();
      if (frame == null) {
        Event last = pending;
        pending = null;
        return last == null ? null : last.outcome();
      }
      if (pending == null) {
        pending = new Event(frame);
      } else if (rule.startsEvent(text(frame))) {
        Event complete = pending;
        pending = new Event(frame);
        DecodeOutcome outcome = complete.outcome();
        if (outcome != null) {
          return outcome;
        }
      } else {
        pending.add(frame);
      }
    }
  }

  private static String text(Frame frame) {
    return switch (frame) {
      case Line line -> new String(line.bytes(), StandardCharsets.UTF_8);
      case OversizedLine o -> new String(o.preview(), StandardCharsets.UTF_8);
    };
  }

  @Override
  public long pendingRecordIndex() {
    if (pending != null) {
      return pending.firstLine;
    }
    return framer == null ? 1 : framer.pendingLineNumber();
  }

  @Override
  public long pendingOffset() {
    if (pending != null) {
      return pending.firstOffset;
    }
    return framer == null ? 0 : framer.pendingOffset();
  }

  @Override
  public void close() {
    TextInput.close(framer, in);
  }

  private final class Event {

    private static final byte[] CRLF = {'\r', '\n'};

    private final long firstLine;
    private final long firstOffset;
    private final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    private final ByteArrayOutputStream preview = new ByteArrayOutputStream();
    private long end;
    private byte terminator;
    private int lines;
    private long size;
    private boolean blank = true;
    private LimitKind limit;
    private long limitValue;
    private boolean oversized;

    Event(Frame first) {
      switch (first) {
        case Line line -> {
          firstLine = line.lineNumber();
          firstOffset = line.offset();
        }
        case OversizedLine o -> {
          firstLine = o.lineNumber();
          firstOffset = o.offset();
        }
      }
      end = firstOffset;
      add(first);
    }

    void add(Frame frame) {
      if (lines > 0) {
        long gap = offset(frame) - end;
        write(gap == 2 ? CRLF : new byte[] {terminator}, gap);
      }
      lines++;
      switch (frame) {
        case Line line -> {
          write(line.bytes(), line.bytes().length);
          blank &= Blank.isBlank(line.bytes());
          terminator = line.terminator();
          end = line.offset() + line.bytes().length;
        }
        case OversizedLine o -> {
          write(o.preview(), o.size());
          blank = false;
          terminator = o.terminator();
          end = o.offset() + o.size();
        }
      }
      if (rule.maxLines() > 0 && lines > rule.maxLines()) {
        becomeOversized(LimitKind.MAX_LINES, rule.maxLines());
      }
    }

    /** {@code data} holds the first bytes of {@code length}; less only for an oversized line. */
    private void write(byte[] data, long length) {
      size += length;
      if (data.length < length || (maxRecordBytes > 0 && size > maxRecordBytes)) {
        becomeOversized(LimitKind.MAX_RECORD_BYTES, maxRecordBytes);
      }
      if (oversized) {
        int room = Previews.PREVIEW_BYTES - preview.size();
        preview.write(data, 0, Math.min(room, data.length));
      } else {
        bytes.write(data, 0, data.length);
      }
    }

    /** The first limit the event goes over is the one reported. */
    private void becomeOversized(LimitKind kind, long value) {
      if (oversized) {
        return;
      }
      oversized = true;
      limit = kind;
      limitValue = value;
      byte[] kept = bytes.toByteArray();
      preview.write(kept, 0, Math.min(kept.length, Previews.PREVIEW_BYTES));
      bytes.reset();
    }

    DecodeOutcome outcome() {
      RawSpan span = new RawSpan(firstOffset, end - firstOffset);
      if (oversized) {
        return new Oversized(
            firstLine, 1, span, limit, limitValue, size, preview.toByteArray(), false);
      }
      if (blank) {
        return null;
      }
      return new Decoded(RawRecords.of(bytes.toString(StandardCharsets.UTF_8)), firstLine, span);
    }

    private static long offset(Frame frame) {
      return switch (frame) {
        case Line line -> line.offset();
        case OversizedLine o -> o.offset();
      };
    }
  }
}
