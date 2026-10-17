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
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/**
 * Re-encodes a stream into UTF-8. Each read hands back what one read of the source produced, so a
 * later source failure cannot take already-decoded bytes with it. A malformed code unit becomes one
 * U+FFFD and only that unit is skipped; so does a lone surrogate, which UTF-32 decoders emit for
 * code points D800–DFFF. A leading byte order mark is dropped.
 */
public final class Utf8TranscodingStream extends InputStream {

  private static final int CHUNK = 8 * 1024;
  private static final byte[] REPLACEMENT_UTF8 = {(byte) 0xEF, (byte) 0xBF, (byte) 0xBD};

  private final InputStream in;
  private final CharsetDecoder decoder;
  private final CharsetEncoder encoder =
      StandardCharsets.UTF_8
          .newEncoder()
          .onMalformedInput(CodingErrorAction.REPLACE)
          .onUnmappableCharacter(CodingErrorAction.REPLACE)
          .replaceWith(REPLACEMENT_UTF8);
  private final int codeUnit;
  private final ByteBuffer source = ByteBuffer.allocate(CHUNK);
  private final CharBuffer chars = CharBuffer.allocate(CHUNK);
  private final ByteBuffer out = ByteBuffer.allocate(CHUNK * 3 + 16);
  private boolean sourceEnded;
  private boolean done;
  private boolean leadingCheckDone;

  /**
   * @param codeUnit bytes skipped per malformed unit: 2 for UTF-16, 4 for UTF-32
   */
  public Utf8TranscodingStream(InputStream in, Charset charset, int codeUnit) {
    this.in = in;
    this.codeUnit = codeUnit;
    this.decoder =
        charset
            .newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT);
    source.flip();
    chars.flip();
    out.flip();
  }

  @Override
  public int read() throws IOException {
    byte[] one = new byte[1];
    int n = read(one, 0, 1);
    return n < 0 ? -1 : one[0] & 0xFF;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (len == 0) {
      return 0;
    }
    while (!out.hasRemaining()) {
      if (done) {
        return -1;
      }
      convertOneRead();
    }
    int n = Math.min(len, out.remaining());
    out.get(b, off, n);
    return n;
  }

  private void convertOneRead() throws IOException {
    int sourceBefore = source.remaining();
    int charsBefore = chars.remaining();
    int read = 0;
    if (!sourceEnded) {
      source.compact();
      read = in.read(source.array(), source.position(), source.remaining());
      if (read < 0) {
        sourceEnded = true;
      } else {
        source.position(source.position() + read);
      }
      source.flip();
    }
    chars.compact();
    decode();
    boolean sourceDrained = sourceEnded && !source.hasRemaining();
    if (sourceDrained) {
      decoder.flush(chars);
    }
    chars.flip();
    dropLeadingBom();
    out.compact();
    encoder.encode(chars, out, sourceDrained);
    if (sourceDrained) {
      encoder.flush(out);
      done = !chars.hasRemaining();
    }
    out.flip();
    boolean progressed =
        read != 0
            || out.hasRemaining()
            || done
            || source.remaining() != sourceBefore
            || chars.remaining() != charsBefore;
    if (!progressed) {
      throw new IOException("transcoding to UTF-8 made no progress");
    }
  }

  private void decode() {
    while (true) {
      CoderResult result = decoder.decode(source, chars, sourceEnded);
      if (result.isError()) {
        if (!chars.hasRemaining()) {
          return;
        }
        int skip = Math.min(codeUnit, source.remaining());
        source.position(source.position() + Math.max(skip, 1));
        chars.put('\uFFFD');
        continue;
      }
      if (sourceEnded && result.isUnderflow() && source.hasRemaining() && chars.hasRemaining()) {
        source.position(source.limit());
        chars.put('\uFFFD');
      }
      return;
    }
  }

  private void dropLeadingBom() {
    if (!leadingCheckDone && chars.hasRemaining()) {
      leadingCheckDone = true;
      if (chars.get(chars.position()) == '\uFEFF') {
        chars.get();
      }
    }
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}
