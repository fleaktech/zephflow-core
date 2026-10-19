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

import java.util.Arrays;

/** A growable byte buffer that can also drop bytes from its front. */
public final class ByteBuilder {

  private byte[] buf = new byte[256];
  private int size;

  public void append(int b) {
    ensure(size + 1);
    buf[size++] = (byte) b;
  }

  public void append(byte[] src, int off, int len) {
    ensure(size + len);
    System.arraycopy(src, off, buf, size, len);
    size += len;
  }

  public void dropFront(int count) {
    System.arraycopy(buf, count, buf, 0, size - count);
    size -= count;
  }

  public void clear() {
    size = 0;
    if (buf.length > 64 * 1024) {
      buf = new byte[256];
    }
  }

  public int size() {
    return size;
  }

  public byte[] copy(int from, int to) {
    return Arrays.copyOfRange(buf, from, to);
  }

  public byte[] toByteArray() {
    return Arrays.copyOf(buf, size);
  }

  private void ensure(int capacity) {
    if (capacity < 0) {
      throw new OutOfMemoryError("buffer over 2 GiB");
    }
    if (capacity > buf.length) {
      buf =
          Arrays.copyOf(
              buf, Math.max(capacity, (int) Math.min(Integer.MAX_VALUE - 8, buf.length * 2L)));
    }
  }
}
