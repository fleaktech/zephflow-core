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

import io.fleak.zephflow.lib.serdes.decode.internal.PeekedStream;
import io.fleak.zephflow.lib.serdes.decode.internal.Utf8TranscodingStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * JSON may arrive as UTF-16 or UTF-32 (PowerShell 5 writes UTF-16LE by default). The encoding is
 * detected from the first bytes with the patterns of RFC 4627 §3 (plus byte order marks) and the
 * stream is re-encoded as UTF-8, so line framing, offsets and budgets always see UTF-8 bytes.
 */
final class JsonEncoding {

  private static final Charset UTF_32BE = Charset.forName("UTF-32BE");
  private static final Charset UTF_32LE = Charset.forName("UTF-32LE");

  private JsonEncoding() {}

  static InputStream toUtf8(InputStream in) throws IOException {
    PeekedStream peeked = PeekedStream.peek(in, 4, JsonEncoding::decided);
    byte[] head = peeked.head();
    Charset charset = detect(head, head.length);
    if (charset == null) {
      return peeked;
    }
    int codeUnit = charset == UTF_32BE || charset == UTF_32LE ? 4 : 2;
    return new Utf8TranscodingStream(peeked, charset, codeUnit);
  }

  /** Two non-zero bytes that are not a UTF-16 byte order mark already mean UTF-8. */
  private static boolean decided(byte[] head) {
    return head.length >= 2 && head[0] != 0 && head[1] != 0 && !isUtf16Bom(head);
  }

  /** Null means UTF-8. */
  private static Charset detect(byte[] b, int n) {
    if (n >= 4) {
      if (b[0] == 0 && b[1] == 0 && b[2] == (byte) 0xFE && b[3] == (byte) 0xFF) {
        return UTF_32BE;
      }
      if (b[0] == (byte) 0xFF && b[1] == (byte) 0xFE && b[2] == 0 && b[3] == 0) {
        return UTF_32LE;
      }
      if (b[0] == 0 && b[1] == 0 && b[2] == 0 && b[3] != 0) {
        return UTF_32BE;
      }
      if (b[0] != 0 && b[1] == 0 && b[2] == 0 && b[3] == 0) {
        return UTF_32LE;
      }
      if (b[0] == 0 && b[1] != 0 && b[2] == 0 && b[3] != 0) {
        return StandardCharsets.UTF_16BE;
      }
      if (b[0] != 0 && b[1] == 0 && b[2] != 0 && b[3] == 0) {
        return StandardCharsets.UTF_16LE;
      }
    }
    if (n >= 2) {
      if (b[0] == (byte) 0xFE && b[1] == (byte) 0xFF) {
        return StandardCharsets.UTF_16BE;
      }
      if (b[0] == (byte) 0xFF && b[1] == (byte) 0xFE) {
        return StandardCharsets.UTF_16LE;
      }
    }
    return null;
  }

  private static boolean isUtf16Bom(byte[] b) {
    return (b[0] == (byte) 0xFE && b[1] == (byte) 0xFF)
        || (b[0] == (byte) 0xFF && b[1] == (byte) 0xFE);
  }
}
