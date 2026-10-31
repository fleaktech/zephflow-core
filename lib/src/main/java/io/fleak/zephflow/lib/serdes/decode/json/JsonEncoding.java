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
import io.fleak.zephflow.lib.serdes.decode.internal.Transcoding;
import io.fleak.zephflow.lib.serdes.decode.internal.Utf8TranscodingStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import org.apache.commons.io.input.BoundedInputStream;
import org.apache.commons.io.input.ReaderInputStream;

/**
 * JSON may arrive as UTF-16 or UTF-32 (PowerShell 5 writes UTF-16LE by default). The encoding is
 * detected from the first bytes with the patterns of RFC 4627 §3 (plus byte order marks) and the
 * stream is re-encoded as UTF-8, so line framing, offsets and budgets always see UTF-8 bytes.
 */
public final class JsonEncoding {

  private JsonEncoding() {}

  /**
   * Limits a decompressed original to a boundary measured by {@link #strictUtf8}, while returning
   * its original bytes and byte order mark. A subsequent explicit decoder still sees its actual
   * source encoding. No bytes of the next character are admitted.
   */
  public static InputStream originalPrefix(InputStream input, long utf8Length) throws IOException {
    if (utf8Length < 0) throw new IllegalArgumentException("Negative captured prefix length");
    PeekedStream peeked = PeekedStream.peek(input, 4, JsonEncoding::decided);
    Charset charset = detect(peeked.head(), peeked.head().length);
    if (charset == null) {
      return BoundedInputStream.builder().setInputStream(peeked).setMaxCount(utf8Length).get();
    }
    return new OriginalPrefix(peeked, charset, utf8Length);
  }

  private static final class OriginalPrefix extends InputStream {
    private final InputStream input;
    private final int width;
    private final boolean littleEndian;
    private final byte[] character = new byte[4];
    private long remaining;
    private boolean first = true;
    private int position;
    private int available;

    private OriginalPrefix(InputStream input, Charset charset, long length) {
      this.input = input;
      this.width = charset.name().startsWith("UTF-32") ? 4 : 2;
      this.littleEndian = charset.name().endsWith("LE");
      this.remaining = length;
    }

    @Override
    public int read() throws IOException {
      if (position == available) {
        if (remaining == 0) return -1;
        position = 0;
        available = width;
        int codePoint = readUnit(0);
        if (width == 2 && Character.isHighSurrogate((char) codePoint)) {
          int low = readUnit(2);
          if (!Character.isLowSurrogate((char) low)) throw new IOException("Invalid UTF-16 pair");
          codePoint = Character.toCodePoint((char) codePoint, (char) low);
          available = 4;
        } else if (codePoint >= 0xD800 && codePoint <= 0xDFFF) {
          throw new IOException("Invalid Unicode surrogate");
        }
        if (codePoint < 0 || codePoint > Character.MAX_CODE_POINT)
          throw new IOException("Invalid Unicode code point");
        // The JDK UTF-32 decoder consumes the leading BOM; fixed UTF-16 decoders retain it.
        int utf8Bytes =
            first && width == 4 && codePoint == 0xFEFF
                ? 0
                : codePoint < 0x80 ? 1 : codePoint < 0x800 ? 2 : codePoint < 0x10000 ? 3 : 4;
        first = false;
        if (utf8Bytes > remaining) throw new IOException("Captured boundary splits a character");
        remaining -= utf8Bytes;
      }
      return character[position++] & 255;
    }

    private int readUnit(int offset) throws IOException {
      int value = 0;
      for (int index = 0; index < width; index++) {
        int next = input.read();
        if (next < 0) throw new EOFException("Original ends before captured boundary");
        character[offset + index] = (byte) next;
        value |= next << (8 * (littleEndian ? index : width - index - 1));
      }
      return value;
    }

    @Override
    public int read(byte[] bytes, int offset, int length) throws IOException {
      java.util.Objects.checkFromIndexSize(offset, length, bytes.length);
      int count = 0;
      while (count < length) {
        int next = read();
        if (next < 0) break;
        bytes[offset + count++] = (byte) next;
      }
      return count == 0 && length != 0 ? -1 : count;
    }

    @Override
    public void close() throws IOException {
      input.close();
    }
  }

  public static InputStream strictUtf8(InputStream in) throws IOException {
    PeekedStream peeked = PeekedStream.peek(in, 4, JsonEncoding::decided);
    Charset charset = detect(peeked.head(), peeked.head().length);
    if (charset == null) return peeked;
    var decoder =
        charset
            .newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT);
    return ReaderInputStream.builder()
        .setReader(new InputStreamReader(peeked, decoder))
        .setCharset(StandardCharsets.UTF_8)
        .get();
  }

  static InputStream toUtf8(InputStream in) throws IOException {
    PeekedStream peeked = PeekedStream.peek(in, 4, JsonEncoding::decided);
    byte[] head = peeked.head();
    Charset charset = detect(head, head.length);
    if (charset == null) {
      return peeked;
    }
    return new Utf8TranscodingStream(peeked, charset);
  }

  /** Two non-zero bytes that are not a UTF-16 byte order mark already mean UTF-8. */
  private static boolean decided(byte[] head) {
    return head.length >= 2 && head[0] != 0 && head[1] != 0 && !isUtf16Bom(head);
  }

  /** Null means UTF-8. */
  private static Charset detect(byte[] b, int n) {
    if (n >= 4) {
      if (b[0] == 0 && b[1] == 0 && b[2] == (byte) 0xFE && b[3] == (byte) 0xFF) {
        return Transcoding.UTF_32BE;
      }
      if (b[0] == (byte) 0xFF && b[1] == (byte) 0xFE && b[2] == 0 && b[3] == 0) {
        return Transcoding.UTF_32LE;
      }
      if (b[0] == 0 && b[1] == 0 && b[2] == 0 && b[3] != 0) {
        return Transcoding.UTF_32BE;
      }
      if (b[0] != 0 && b[1] == 0 && b[2] == 0 && b[3] == 0) {
        return Transcoding.UTF_32LE;
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
