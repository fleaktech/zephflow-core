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
import io.fleak.zephflow.lib.serdes.decode.RawSpan;
import io.fleak.zephflow.lib.serdes.decode.internal.DecodeFlags;
import io.fleak.zephflow.lib.serdes.decode.internal.FormatCursor;
import io.fleak.zephflow.lib.serdes.decode.internal.WholeInputReader;
import java.io.IOException;
import java.io.InputStream;

public final class JsonObjectFormat implements FormatCursor {

  private final InputStream in;
  private final long maxDocumentBytes;
  private boolean done;

  public JsonObjectFormat(InputStream in, DecodeLimits limits, DecodeFlags flags) {
    this.in = in;
    this.maxDocumentBytes = limits.maxDocumentBytes();
  }

  @Override
  public DecodeOutcome next() throws IOException {
    if (done) {
      return null;
    }
    done = true;
    return switch (WholeInputReader.read(JsonEncoding.toUtf8(in), maxDocumentBytes)) {
      case WholeInputReader.TooLarge tooLarge ->
          new Oversized(-1, RawSpan.toEnd(0), tooLarge.sizeSoFar(), tooLarge.preview(), true);
      case WholeInputReader.Whole whole -> parse(whole.bytes());
    };
  }

  private static DecodeOutcome parse(byte[] bytes) {
    if (bytes.length == 0) {
      return null;
    }
    RawSpan span = new RawSpan(0, bytes.length);
    try {
      return new Decoded(JsonDecoding.parseObject(bytes), 1, span);
    } catch (IOException | RuntimeException e) {
      return new Failed(ErrorKind.MALFORMED, bytes, -1, span, e, true);
    }
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
