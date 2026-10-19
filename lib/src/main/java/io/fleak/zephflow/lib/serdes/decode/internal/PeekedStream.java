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
import java.util.Arrays;
import java.util.function.Predicate;

/**
 * Peeks at the first bytes of a stream and then replays them. Unlike PushbackInputStream, a read
 * that returns replayed bytes never also reads the source, so a source failure right after the
 * peeked bytes cannot take them along.
 */
public final class PeekedStream extends InputStream {

  private final InputStream in;
  private final byte[] head;
  private int headPos;

  private PeekedStream(InputStream in, byte[] head) {
    this.in = in;
    this.head = head;
  }

  /**
   * Reads up to {@code max} bytes, stopping early once {@code enough} returns true for what has
   * been read so far, so no more is requested than the caller needs to decide.
   */
  public static PeekedStream peek(InputStream in, int max, Predicate<byte[]> enough)
      throws IOException {
    byte[] buf = new byte[max];
    int read = 0;
    while (read < max && !enough.test(Arrays.copyOf(buf, read))) {
      int n = in.read(buf, read, max - read);
      if (n < 0) {
        break;
      }
      read += n;
    }
    return new PeekedStream(in, Arrays.copyOf(buf, read));
  }

  public byte[] head() {
    return head.clone();
  }

  @Override
  public int read() throws IOException {
    if (headPos < head.length) {
      return head[headPos++] & 0xFF;
    }
    return in.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (len == 0) {
      return 0;
    }
    if (headPos < head.length) {
      int n = Math.min(len, head.length - headPos);
      System.arraycopy(head, headPos, b, off, n);
      headPos += n;
      return n;
    }
    return in.read(b, off, len);
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}
