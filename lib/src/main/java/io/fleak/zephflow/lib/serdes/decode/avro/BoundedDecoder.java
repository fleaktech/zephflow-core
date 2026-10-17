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
package io.fleak.zephflow.lib.serdes.decode.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

/**
 * Avro binary decoding over one block (or one datum) in memory. Every length and item count is
 * checked against the bytes left before anything is allocated, so a wrong schema or a flipped bit
 * cannot ask for gigabytes. Avro's own limits are JVM-wide system properties, so they are not used.
 * Array, map and record counts all draw on one item budget per block: its byte length (an item
 * takes at least one byte) plus {@link #ZERO_BYTE_ITEMS} for items that encode in no bytes at all,
 * such as nulls or empty records.
 */
final class BoundedDecoder extends Decoder {

  static final long ZERO_BYTE_ITEMS = 100_000;

  private final byte[] buf;
  private int pos;
  private final int end;
  private long items;

  BoundedDecoder(byte[] buf, int off, int len) {
    this.buf = buf;
    this.pos = off;
    this.end = off + len;
    this.items = len + ZERO_BYTE_ITEMS;
  }

  /** Charges {@code count} items (records, array or map entries) against the block's budget. */
  void takeItems(long count, String what) throws MalformedAvroException {
    if (count < 0 || count > items) {
      throw new MalformedAvroException(
          what + " of " + count + " items is more than the block's " + items + " items left");
    }
    items -= count;
  }

  int remaining() {
    return end - pos;
  }

  boolean isEnd() {
    return pos == end;
  }

  private void need(long n, String what) throws MalformedAvroException {
    if (n < 0 || n > end - pos) {
      throw new MalformedAvroException(
          what + " of " + n + " bytes is longer than the " + (end - pos) + " bytes left");
    }
  }

  @Override
  public void readNull() {}

  @Override
  public boolean readBoolean() throws IOException {
    need(1, "boolean");
    int b = buf[pos++];
    if (b != 0 && b != 1) {
      throw new MalformedAvroException("invalid boolean byte " + b);
    }
    return b == 1;
  }

  @Override
  public int readInt() throws IOException {
    long value = readLong();
    if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
      throw new MalformedAvroException("int value " + value + " is out of range");
    }
    return (int) value;
  }

  @Override
  public long readLong() throws IOException {
    long raw = 0;
    for (int shift = 0; shift < 64; shift += 7) {
      need(1, "varint");
      int b = buf[pos++] & 0xFF;
      raw |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return (raw >>> 1) ^ -(raw & 1);
      }
    }
    throw new MalformedAvroException("varint is longer than 10 bytes");
  }

  @Override
  public float readFloat() throws IOException {
    need(4, "float");
    float value = ByteBuffer.wrap(buf, pos, 4).order(ByteOrder.LITTLE_ENDIAN).getFloat();
    pos += 4;
    return value;
  }

  @Override
  public double readDouble() throws IOException {
    need(8, "double");
    double value = ByteBuffer.wrap(buf, pos, 8).order(ByteOrder.LITTLE_ENDIAN).getDouble();
    pos += 8;
    return value;
  }

  @Override
  public Utf8 readString(Utf8 old) throws IOException {
    byte[] bytes = lengthPrefixed("string");
    return new Utf8(bytes);
  }

  @Override
  public String readString() throws IOException {
    return new String(lengthPrefixed("string"), StandardCharsets.UTF_8);
  }

  @Override
  public void skipString() throws IOException {
    skipLengthPrefixed("string");
  }

  @Override
  public ByteBuffer readBytes(ByteBuffer old) throws IOException {
    return ByteBuffer.wrap(lengthPrefixed("bytes"));
  }

  @Override
  public void skipBytes() throws IOException {
    skipLengthPrefixed("bytes");
  }

  @Override
  public void readFixed(byte[] bytes, int start, int length) throws IOException {
    need(length, "fixed");
    System.arraycopy(buf, pos, bytes, start, length);
    pos += length;
  }

  @Override
  public void skipFixed(int length) throws IOException {
    need(length, "fixed");
    pos += length;
  }

  @Override
  public int readEnum() throws IOException {
    return readInt();
  }

  @Override
  public long readArrayStart() throws IOException {
    return blockCount("array");
  }

  @Override
  public long arrayNext() throws IOException {
    return blockCount("array");
  }

  @Override
  public long skipArray() throws IOException {
    return skipBlocks("array");
  }

  @Override
  public long readMapStart() throws IOException {
    return blockCount("map");
  }

  @Override
  public long mapNext() throws IOException {
    return blockCount("map");
  }

  @Override
  public long skipMap() throws IOException {
    return skipBlocks("map");
  }

  @Override
  public int readIndex() throws IOException {
    return readInt();
  }

  private long blockCount(String what) throws IOException {
    long count = readLong();
    if (count < 0) {
      count = -count;
      readLong();
    }
    takeItems(count, what + " block");
    return count;
  }

  /**
   * Skips blocks that carry their byte size; returns the count of the first block that does not.
   */
  private long skipBlocks(String what) throws IOException {
    while (true) {
      long count = readLong();
      if (count >= 0) {
        takeItems(count, what + " block");
        return count;
      }
      if (count == Long.MIN_VALUE) {
        throw new MalformedAvroException(what + " block count is out of range");
      }
      long size = readLong();
      need(size, what + " block");
      pos += (int) size;
    }
  }

  private byte[] lengthPrefixed(String what) throws IOException {
    long length = readLong();
    need(length, what);
    byte[] bytes = Arrays.copyOfRange(buf, pos, pos + (int) length);
    pos += (int) length;
    return bytes;
  }

  private void skipLengthPrefixed(String what) throws IOException {
    long length = readLong();
    need(length, what);
    pos += (int) length;
  }
}
