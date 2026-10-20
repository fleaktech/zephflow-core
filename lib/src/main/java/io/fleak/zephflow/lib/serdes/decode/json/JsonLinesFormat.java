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
package io.fleak.zephflow.lib.serdes.decode.json;

import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Decoded;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Failed;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Oversized;
import io.fleak.zephflow.lib.serdes.decode.ErrorKind;
import io.fleak.zephflow.lib.serdes.decode.LimitKind;
import io.fleak.zephflow.lib.serdes.decode.RawSpan;
import io.fleak.zephflow.lib.serdes.decode.internal.DecodeFlags;
import io.fleak.zephflow.lib.serdes.decode.internal.FormatCursor;
import io.fleak.zephflow.lib.serdes.decode.internal.LineFramer;
import io.fleak.zephflow.lib.serdes.decode.internal.LineFramer.Line;
import io.fleak.zephflow.lib.serdes.decode.internal.LineFramer.OversizedLine;
import io.fleak.zephflow.lib.serdes.decode.internal.Utf8TranscodingStream;
import io.fleak.zephflow.lib.serdes.decode.text.Blank;
import java.io.IOException;
import java.io.InputStream;

public final class JsonLinesFormat implements FormatCursor {

  private final InputStream in;
  private final long maxRecordBytes;
  private LineFramer framer;

  public JsonLinesFormat(InputStream in, DecodeLimits limits, DecodeFlags flags) {
    this.in = in;
    this.maxRecordBytes = limits.maxRecordBytes();
  }

  @Override
  public DecodeOutcome next() throws IOException {
    if (framer == null) {
      InputStream utf8 = JsonEncoding.toUtf8(in);
      framer = new LineFramer(utf8, maxRecordBytes, !(utf8 instanceof Utf8TranscodingStream));
    }
    while (true) {
      LineFramer.Frame frame = framer.next();
      switch (frame) {
        case null -> {
          return null;
        }
        case OversizedLine o -> {
          return new Oversized(
              o.lineNumber(),
              1,
              new RawSpan(o.offset(), o.size()),
              LimitKind.MAX_RECORD_BYTES,
              maxRecordBytes,
              o.size(),
              o.preview(),
              false);
        }
        case Line line -> {
          if (Blank.isBlank(line.bytes())) {
            continue;
          }
          return parse(line);
        }
      }
    }
  }

  private static DecodeOutcome parse(Line line) {
    RawSpan span = new RawSpan(line.offset(), line.bytes().length);
    try {
      return new Decoded(JsonDecoding.parseObject(line.bytes()), line.lineNumber(), span);
    } catch (IOException | RuntimeException e) {
      return new Failed(ErrorKind.MALFORMED, line.bytes(), line.lineNumber(), span, e, false);
    }
  }

  @Override
  public long pendingRecordIndex() {
    return framer == null ? 1 : framer.pendingLineNumber();
  }

  @Override
  public long pendingOffset() {
    return framer == null ? 0 : framer.pendingOffset();
  }

  @Override
  public void close() {
    if (framer != null) {
      framer.close();
    } else {
      try {
        in.close();
      } catch (IOException ignored) {
        // Nothing was read.
      }
    }
  }
}
