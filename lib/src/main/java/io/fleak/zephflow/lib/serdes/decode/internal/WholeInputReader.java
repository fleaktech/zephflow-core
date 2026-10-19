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

import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome.Oversized;
import io.fleak.zephflow.lib.serdes.decode.LimitKind;
import io.fleak.zephflow.lib.serdes.decode.RawSpan;
import java.io.IOException;
import java.io.InputStream;

/** Reads a whole input that must be held at once, stopping exactly at the document limit. */
public final class WholeInputReader {

  public sealed interface Result {}

  public record Whole(byte[] bytes) implements Result {}

  public record TooLarge(long maxDocumentBytes, long sizeSoFar, byte[] preview) implements Result {

    /** The whole input, over the document limit; decoding cannot go on. */
    public Oversized outcome() {
      return new Oversized(
          -1,
          1,
          RawSpan.toEnd(0),
          LimitKind.MAX_DOCUMENT_BYTES,
          maxDocumentBytes,
          sizeSoFar,
          preview,
          true);
    }
  }

  private WholeInputReader() {}

  public static Result read(InputStream in, long maxDocumentBytes) throws IOException {
    ByteBuilder bytes = new ByteBuilder();
    byte[] buf = new byte[64 * 1024];
    int n;
    while ((n = in.read(buf, 0, buf.length)) >= 0) {
      if (maxDocumentBytes > 0 && bytes.size() + (long) n > maxDocumentBytes) {
        long size = bytes.size() + (long) n;
        int keep = Math.min(bytes.size(), Previews.PREVIEW_BYTES);
        ByteBuilder preview = new ByteBuilder();
        preview.append(bytes.copy(0, keep), 0, keep);
        preview.append(buf, 0, Math.min(n, Previews.PREVIEW_BYTES - keep));
        return new TooLarge(maxDocumentBytes, size, preview.toByteArray());
      }
      bytes.append(buf, 0, n);
    }
    return new Whole(bytes.toByteArray());
  }
}
