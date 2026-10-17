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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/** Brings text in a configured charset to UTF-8, so framing, offsets and budgets see UTF-8. */
public final class Transcoding {

  public static final Charset UTF_32 = Charset.forName("UTF-32");
  public static final Charset UTF_32BE = Charset.forName("UTF-32BE");
  public static final Charset UTF_32LE = Charset.forName("UTF-32LE");

  private static final Charset UTF_16LE_BOM = Charset.forName("x-UTF-16LE-BOM");

  /** JDK UTF-32 charsets whose decoders drop a byte order mark on their own. */
  private static final Map<Charset, Charset> BOM_DROPPING =
      Map.of(
          Charset.forName("X-UTF-32BE-BOM"), UTF_32BE,
          Charset.forName("X-UTF-32LE-BOM"), UTF_32LE);

  private Transcoding() {}

  /**
   * Returns {@code in} itself for UTF-8, otherwise a re-encoding stream. Unmarked UTF-16 and UTF-32
   * take their byte order from a byte order mark and default to big-endian (RFC 2781);
   * x-UTF-16LE-BOM does the same but defaults to little-endian, as its JDK decoder does.
   */
  public static InputStream toUtf8(InputStream in, Charset charset) throws IOException {
    if (charset.equals(StandardCharsets.UTF_8)) {
      return in;
    }
    if (charset.equals(StandardCharsets.UTF_16) || charset.equals(UTF_16LE_BOM)) {
      PeekedStream peeked =
          PeekedStream.peek(
              in,
              2,
              head -> startsDifferently(head, 0xFF, 0xFE) && startsDifferently(head, 0xFE, 0xFF));
      byte[] head = peeked.head();
      boolean little =
          head.length == 2 && !startsDifferently(head, 0xFF, 0xFE)
              || charset.equals(UTF_16LE_BOM) && startsDifferently(head, 0xFE, 0xFF);
      return new Utf8TranscodingStream(
          peeked, little ? StandardCharsets.UTF_16LE : StandardCharsets.UTF_16BE);
    }
    if (charset.equals(UTF_32)) {
      PeekedStream peeked =
          PeekedStream.peek(in, 4, head -> startsDifferently(head, 0xFF, 0xFE, 0, 0));
      boolean little =
          peeked.head().length == 4 && !startsDifferently(peeked.head(), 0xFF, 0xFE, 0, 0);
      return new Utf8TranscodingStream(peeked, little ? UTF_32LE : UTF_32BE);
    }
    return new Utf8TranscodingStream(in, BOM_DROPPING.getOrDefault(charset, charset));
  }

  /** True once the bytes read so far already differ from {@code mark}. */
  private static boolean startsDifferently(byte[] head, int... mark) {
    for (int i = 0; i < head.length && i < mark.length; i++) {
      if ((head[i] & 0xFF) != mark[i]) {
        return true;
      }
    }
    return false;
  }
}
