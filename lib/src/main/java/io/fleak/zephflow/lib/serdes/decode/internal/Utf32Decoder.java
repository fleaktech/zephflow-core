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

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;

/**
 * Fixed-endian UTF-32. The JDK decoders drop a leading byte order mark on their own and pass
 * surrogate code points through as chars, so two of them in a row turn into one valid character.
 * This one keeps every code point and reports surrogates and values above U+10FFFF as malformed.
 */
final class Utf32Decoder extends CharsetDecoder {

  private final boolean bigEndian;

  Utf32Decoder(Charset charset, boolean bigEndian) {
    super(charset, 0.25f, 1.0f);
    this.bigEndian = bigEndian;
  }

  @Override
  protected CoderResult decodeLoop(ByteBuffer in, CharBuffer out) {
    while (in.remaining() >= 4) {
      int p = in.position();
      int cp =
          bigEndian ? codePoint(in, p, p + 1, p + 2, p + 3) : codePoint(in, p + 3, p + 2, p + 1, p);
      if (cp < 0
          || cp > Character.MAX_CODE_POINT
          || (cp >= Character.MIN_SURROGATE && cp <= Character.MAX_SURROGATE)) {
        return CoderResult.malformedForLength(4);
      }
      if (out.remaining() < Character.charCount(cp)) {
        return CoderResult.OVERFLOW;
      }
      out.put(Character.toChars(cp));
      in.position(p + 4);
    }
    return CoderResult.UNDERFLOW;
  }

  private static int codePoint(ByteBuffer in, int b0, int b1, int b2, int b3) {
    return (in.get(b0) & 0xFF) << 24
        | (in.get(b1) & 0xFF) << 16
        | (in.get(b2) & 0xFF) << 8
        | (in.get(b3) & 0xFF);
  }
}
