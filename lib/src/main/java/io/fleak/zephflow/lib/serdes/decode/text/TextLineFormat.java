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
import io.fleak.zephflow.lib.serdes.decode.RawSpan;
import io.fleak.zephflow.lib.serdes.decode.internal.FormatCursor;
import io.fleak.zephflow.lib.serdes.decode.internal.LineFramer;
import io.fleak.zephflow.lib.serdes.decode.internal.LineFramer.Line;
import io.fleak.zephflow.lib.serdes.decode.internal.LineFramer.OversizedLine;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public final class TextLineFormat implements FormatCursor {

  private final LineFramer framer;
  private final boolean skipBlank;

  public TextLineFormat(InputStream in, DecodeLimits limits, boolean skipBlank) {
    this.framer = new LineFramer(in, limits.maxRecordBytes());
    this.skipBlank = skipBlank;
  }

  @Override
  public DecodeOutcome next() throws IOException {
    while (true) {
      LineFramer.Frame frame = framer.next();
      switch (frame) {
        case null -> {
          return null;
        }
        case OversizedLine o -> {
          return new Oversized(
              o.lineNumber(), new RawSpan(o.offset(), o.size()), o.size(), o.preview(), false);
        }
        case Line line -> {
          if (skipBlank && Blank.isBlank(line.bytes())) {
            continue;
          }
          return new Decoded(
              RawRecords.of(new String(line.bytes(), StandardCharsets.UTF_8)),
              line.lineNumber(),
              new RawSpan(line.offset(), line.bytes().length));
        }
      }
    }
  }

  @Override
  public long pendingRecordIndex() {
    return framer.pendingLineNumber();
  }

  @Override
  public long pendingOffset() {
    return framer.pendingOffset();
  }

  @Override
  public void close() {
    framer.close();
  }
}
