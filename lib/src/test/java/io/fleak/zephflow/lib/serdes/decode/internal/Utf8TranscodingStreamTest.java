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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import org.junit.jupiter.api.Test;

class Utf8TranscodingStreamTest {

  /** A charset whose decoder never consumes anything, as a broken provider might. */
  private static final Charset STUCK =
      new Charset("x-stuck", null) {
        @Override
        public boolean contains(Charset cs) {
          return false;
        }

        @Override
        public CharsetDecoder newDecoder() {
          return new CharsetDecoder(this, 1, 1) {
            @Override
            protected CoderResult decodeLoop(ByteBuffer in, CharBuffer out) {
              return CoderResult.UNDERFLOW;
            }
          };
        }

        @Override
        public CharsetEncoder newEncoder() {
          throw new UnsupportedOperationException();
        }
      };

  @Test
  void decoderThatMakesNoProgressFailsInsteadOfSpinning() {
    InputStream stream =
        new Utf8TranscodingStream(new ByteArrayInputStream(new byte[20_000]), STUCK);
    IOException thrown = assertThrows(IOException.class, () -> stream.read(new byte[16]));
    assertEquals("transcoding to UTF-8 made no progress", thrown.getMessage());
  }
}
