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
package io.fleak.zephflow.lib.serdes.decode.compress;

import io.fleak.zephflow.lib.serdes.decode.internal.PeekedStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;

/**
 * Concatenated bzip2 streams, one library stream each. commons-compress's own concatenated mode
 * checks for the next stream inside the same read that returns the end of the previous one, so a
 * failure there drops the previous stream's last bytes; checking between streams here avoids that.
 */
final class Bzip2Streams extends InputStream {

  private static final byte[] MAGIC = {'B', 'Z', 'h'};

  private final InputStream in;
  private InputStream current;
  private int streams;
  private boolean done;

  Bzip2Streams(InputStream in) {
    this.in = in;
  }

  @Override
  public int read() throws IOException {
    byte[] one = new byte[1];
    int n = read(one, 0, 1);
    return n < 0 ? -1 : one[0] & 0xFF;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    while (!done) {
      if (current == null && !nextStream()) {
        done = true;
        break;
      }
      int n = current.read(b, off, len);
      if (n >= 0) {
        return n;
      }
      streams++;
      current = null;
    }
    return -1;
  }

  private boolean nextStream() throws IOException {
    PeekedStream peeked = PeekedStream.peek(in, MAGIC.length, Bzip2Streams::decided);
    byte[] head = peeked.head();
    if (head.length == 0 && streams > 0) {
      return false;
    }
    if (head.length > 0 && head.length < MAGIC.length && startsWithMagic(head)) {
      throw new IOException("bzip2 stream " + (streams + 1) + " is truncated");
    }
    if (head.length < MAGIC.length || !startsWithMagic(head)) {
      throw new IOException(
          streams == 0
              ? "input is not in bzip2 format"
              : "unexpected bytes after bzip2 stream " + streams);
    }
    current = new BZip2CompressorInputStream(peeked, false);
    return true;
  }

  /** Enough is read once the bytes already differ from the magic. */
  private static boolean decided(byte[] head) {
    return head.length > 0 && !startsWithMagic(head);
  }

  private static boolean startsWithMagic(byte[] head) {
    for (int i = 0; i < head.length && i < MAGIC.length; i++) {
      if (head[i] != MAGIC[i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() throws IOException {
    in.close();
  }
}
