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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PushbackInputStream;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.input.ReaderInputStream;

/**
 * JSON may arrive as UTF-16 or UTF-32 (PowerShell 5 writes UTF-16LE by default). The encoding is
 * detected from the first four bytes as in RFC 4627 §3 and the stream is transcoded to UTF-8, so
 * line framing, byte offsets and budgets always work on UTF-8 bytes.
 */
final class JsonEncoding {

  private JsonEncoding() {}

  static InputStream toUtf8(InputStream in) throws IOException {
    PushbackInputStream pushback = new PushbackInputStream(in, 4);
    byte[] head = new byte[4];
    int read = 0;
    while (read < head.length) {
      int n = pushback.read(head, read, head.length - read);
      if (n < 0) {
        break;
      }
      read += n;
    }
    if (read > 0) {
      pushback.unread(head, 0, read);
    }
    Charset charset = detect(head, read);
    if (charset == null) {
      return pushback;
    }
    InputStreamReader reader =
        new InputStreamReader(
            pushback,
            charset
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE));
    return ReaderInputStream.builder().setReader(reader).setCharset(StandardCharsets.UTF_8).get();
  }

  /** Null means UTF-8. */
  private static Charset detect(byte[] b, int n) {
    if (n >= 4) {
      boolean bomBe = b[0] == 0 && b[1] == 0 && b[2] == (byte) 0xFE && b[3] == (byte) 0xFF;
      boolean bomLe = b[0] == (byte) 0xFF && b[1] == (byte) 0xFE && b[2] == 0 && b[3] == 0;
      if (bomBe || (b[0] == 0 && b[1] == 0 && b[2] == 0 && b[3] != 0)) {
        return Charset.forName("UTF-32BE");
      }
      if (bomLe || (b[0] != 0 && b[1] == 0 && b[2] == 0 && b[3] == 0)) {
        return Charset.forName("UTF-32LE");
      }
    }
    if (n >= 2) {
      if ((b[0] == (byte) 0xFE && b[1] == (byte) 0xFF) || (b[0] == 0 && b[1] != 0)) {
        return StandardCharsets.UTF_16BE;
      }
      if ((b[0] == (byte) 0xFF && b[1] == (byte) 0xFE) || (b[0] != 0 && b[1] == 0)) {
        return StandardCharsets.UTF_16LE;
      }
    }
    return null;
  }
}
