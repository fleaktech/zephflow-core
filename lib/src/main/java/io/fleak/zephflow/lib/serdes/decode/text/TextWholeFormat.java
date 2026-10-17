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
import io.fleak.zephflow.lib.serdes.decode.internal.Transcoding;
import io.fleak.zephflow.lib.serdes.decode.internal.WholeInputReader;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public final class TextWholeFormat implements FormatCursor {

  private final InputStream in;
  private final Charset charset;
  private final long maxDocumentBytes;
  private boolean done;

  public TextWholeFormat(InputStream in, DecodeLimits limits, Charset charset) {
    this.in = in;
    this.charset = charset;
    this.maxDocumentBytes = limits.maxDocumentBytes();
  }

  @Override
  public DecodeOutcome next() throws IOException {
    if (done) {
      return null;
    }
    done = true;
    InputStream utf8 = Transcoding.toUtf8(in, charset);
    boolean skipBom = utf8 == in;
    return switch (WholeInputReader.read(utf8, maxDocumentBytes)) {
      case WholeInputReader.TooLarge tooLarge ->
          new Oversized(-1, RawSpan.toEnd(0), tooLarge.sizeSoFar(), tooLarge.preview(), true);
      case WholeInputReader.Whole whole -> decode(whole.bytes(), skipBom);
    };
  }

  private static DecodeOutcome decode(byte[] bytes, boolean skipBom) {
    if (bytes.length == 0) {
      return null;
    }
    int bom = skipBom && hasUtf8Bom(bytes) ? 3 : 0;
    String text = new String(bytes, bom, bytes.length - bom, StandardCharsets.UTF_8);
    return new Decoded(RawRecords.of(text), 1, new RawSpan(0, bytes.length));
  }

  private static boolean hasUtf8Bom(byte[] bytes) {
    return bytes.length >= 3
        && (bytes[0] & 0xFF) == 0xEF
        && (bytes[1] & 0xFF) == 0xBB
        && (bytes[2] & 0xFF) == 0xBF;
  }

  @Override
  public long pendingRecordIndex() {
    return -1;
  }

  @Override
  public long pendingOffset() {
    return 0;
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
