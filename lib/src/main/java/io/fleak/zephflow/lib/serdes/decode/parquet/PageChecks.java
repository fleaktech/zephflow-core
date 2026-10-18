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
package io.fleak.zephflow.lib.serdes.decode.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader;
import org.apache.parquet.schema.PrimitiveType;

/**
 * Checks a decompressed page before parquet's decoders read it, because they allocate from counts
 * and run headers inside the page: a bit-packed run may hold at most {@value #MAX_GROUPS} groups of
 * eight, within the page's bytes (writers use at most 64; the decoder allocates the whole run); a
 * delta header no more than the page's values, in blocks of at most {@value #MAX_DELTA_BLOCK};
 * binary dictionary entries within the page; and a DELTA_BYTE_ARRAY prefix no longer than the value
 * before it.
 */
final class PageChecks {

  static final int MAX_DELTA_BLOCK = 2048;

  private static final long MAX_GROUPS = 1024;

  private PageChecks() {}

  /**
   * RLE/bit-packed hybrid runs in {@code [offset, end)} of values {@code width} bits wide, for at
   * most {@code values} values. Returns the offset after the runs that were read.
   */
  static int hybrid(byte[] page, int offset, int end, int width, long values)
      throws MalformedParquetException {
    if (width < 0 || width > 32) {
      throw new MalformedParquetException("parquet run of " + width + "-bit values");
    }
    int pos = offset;
    long seen = 0;
    while (seen < values && pos < end) {
      long header = 0;
      int shift = 0;
      while (true) {
        if (pos >= end || shift > 28) {
          throw new MalformedParquetException("parquet run header is cut off or too long");
        }
        int b = page[pos++] & 0xFF;
        header |= (long) (b & 0x7F) << shift;
        shift += 7;
        if ((b & 0x80) == 0) {
          break;
        }
      }
      long count = header >>> 1;
      if ((header & 1) == 1) {
        long bytes = count * width;
        if (count > MAX_GROUPS || bytes > end - pos) {
          throw new MalformedParquetException(
              "parquet bit-packed run of " + count * 8 + " values does not fit the page");
        }
        pos += (int) bytes;
        seen += count * 8;
      } else {
        int bytes = (width + 7) / 8;
        if (bytes > end - pos) {
          throw new MalformedParquetException(
              "parquet repeated run of " + count + " values does not fit the page");
        }
        pos += bytes;
        seen += count;
      }
    }
    return pos;
  }

  /** A four-byte little-endian length, then hybrid runs; returns the offset after it. */
  static int prefixedHybrid(byte[] page, int offset, int end, int width, long values)
      throws MalformedParquetException {
    if (end - offset < 4) {
      throw new MalformedParquetException("parquet levels are cut off before their length");
    }
    long length =
        Integer.toUnsignedLong(
            ByteBuffer.wrap(page, offset, 4).order(ByteOrder.LITTLE_ENDIAN).getInt());
    if (length > end - offset - 4) {
      throw new MalformedParquetException(
          "parquet levels of " + length + " bytes overflow the page");
    }
    int start = offset + 4;
    hybrid(page, start, start + (int) length, width, values);
    return start + (int) length;
  }

  /** The values of a page in {@code [offset, end)}, of which there are at most {@code values}. */
  static void values(byte[] page, int offset, int end, Encoding encoding, long values)
      throws IOException {
    switch (encoding) {
      case PLAIN_DICTIONARY, RLE_DICTIONARY -> {
        if (offset < end) {
          hybrid(page, offset + 1, end, page[offset] & 0xFF, values);
        }
      }
      case RLE -> prefixedHybrid(page, offset, end, 1, values);
      case DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY ->
          deltaHeader(ByteBuffer.wrap(page, offset, end - offset), values);
      case DELTA_BYTE_ARRAY -> deltaByteArray(page, offset, end, values);
      default -> {}
    }
  }

  /** Dictionary entries take at least their fixed width, or four bytes of length. */
  static void dictionary(int entries, int bytes, PrimitiveType type)
      throws MalformedParquetException {
    long width =
        switch (type.getPrimitiveTypeName()) {
          case INT32, FLOAT -> 4;
          case INT64, DOUBLE -> 8;
          case INT96 -> 12;
          case BINARY -> 4;
          case FIXED_LEN_BYTE_ARRAY -> Math.max(1, type.getTypeLength());
          case BOOLEAN -> throw new MalformedParquetException("parquet boolean dictionary");
        };
    if (entries < 0 || entries * width > bytes) {
      throw new MalformedParquetException(
          "parquet dictionary of " + entries + " entries does not fit in " + bytes + " bytes");
    }
  }

  /**
   * Binary dictionary entries, each a four-byte length and its bytes, must lie inside the page:
   * parquet's dictionary wraps each length as it stands, so a damaged one would point past it.
   */
  static void dictionaryEntries(byte[] page, int entries, PrimitiveType type)
      throws MalformedParquetException {
    dictionary(entries, page.length, type);
    if (type.getPrimitiveTypeName() != PrimitiveType.PrimitiveTypeName.BINARY) {
      return;
    }
    ByteBuffer in = ByteBuffer.wrap(page).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < entries; i++) {
      if (in.remaining() < 4) {
        throw new MalformedParquetException("parquet dictionary entry " + i + " is cut off");
      }
      int length = in.getInt();
      if (length < 0 || length > in.remaining()) {
        throw new MalformedParquetException(
            "parquet dictionary entry " + i + " of " + length + " bytes overflows the page");
      }
      in.position(in.position() + length);
    }
  }

  /** Returns the header's value count after checking the sizes parquet allocates from. */
  private static long deltaHeader(ByteBuffer in, long values) throws MalformedParquetException {
    long blockSize = varint(in);
    long miniBlocks = varint(in);
    long total = varint(in);
    if (blockSize <= 0
        || blockSize > MAX_DELTA_BLOCK
        || miniBlocks <= 0
        || blockSize % miniBlocks != 0
        || (blockSize / miniBlocks) % 8 != 0) {
      throw new MalformedParquetException(
          "parquet delta block of " + blockSize + " values in " + miniBlocks + " miniblocks");
    }
    if (total > values) {
      throw new MalformedParquetException(
          "parquet delta encoding claims " + total + " values in a page of " + values);
    }
    return total;
  }

  /**
   * Prefix lengths, then suffixes as DELTA_LENGTH_BYTE_ARRAY. Each value is allocated at its prefix
   * plus suffix length, so a prefix may not be longer than the value before it.
   */
  private static void deltaByteArray(byte[] page, int offset, int end, long values)
      throws IOException {
    ByteBuffer in = ByteBuffer.wrap(page, offset, end - offset);
    int[] prefix = deltaValues(in, values);
    int[] suffix = deltaValues(in, values);
    if (prefix.length != suffix.length) {
      throw new MalformedParquetException(
          "parquet DELTA_BYTE_ARRAY has "
              + prefix.length
              + " prefixes and "
              + suffix.length
              + " suffixes");
    }
    long dataLeft = in.remaining();
    long previous = 0;
    for (int i = 0; i < prefix.length; i++) {
      if (prefix[i] < 0 || prefix[i] > previous || suffix[i] < 0 || suffix[i] > dataLeft) {
        throw new MalformedParquetException("parquet DELTA_BYTE_ARRAY value " + i + " is invalid");
      }
      dataLeft -= suffix[i];
      previous = (long) prefix[i] + suffix[i];
    }
  }

  /** Decodes one DELTA_BINARY_PACKED stream at {@code in}'s position and moves past it. */
  private static int[] deltaValues(ByteBuffer in, long values) throws IOException {
    int total = (int) deltaHeader(in.duplicate(), values);
    ByteBufferInputStream stream = ByteBufferInputStream.wrap(in.slice());
    int[] decoded = new int[total];
    try {
      DeltaBinaryPackingValuesReader reader = new DeltaBinaryPackingValuesReader();
      reader.initFromPage(total, stream);
      for (int i = 0; i < total; i++) {
        decoded[i] = reader.readInteger();
      }
    } catch (RuntimeException e) {
      throw new MalformedParquetException(
          "parquet delta values do not decode: " + e.getMessage(), e);
    }
    in.position(in.position() + (int) stream.position());
    return decoded;
  }

  private static long varint(ByteBuffer in) throws MalformedParquetException {
    long value = 0;
    for (int shift = 0; shift < 35; shift += 7) {
      if (!in.hasRemaining()) {
        throw new MalformedParquetException("parquet delta header is cut off");
      }
      int b = in.get() & 0xFF;
      value |= (long) (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        return value;
      }
    }
    throw new MalformedParquetException("parquet delta header varint is too long");
  }
}
