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

import com.github.luben.zstd.Zstd;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4SafeDecompressor;
import org.apache.parquet.format.CompressionCodec;
import org.xerial.snappy.Snappy;

/**
 * Decompresses one page into exactly the size its header claims, which has already been checked
 * against the row group's budget; more or less output is malformed. LZ4 is Hadoop's framing, as
 * parquet-mr writes it.
 */
final class PageCodecs {

  private static final LZ4SafeDecompressor LZ4 = LZ4Factory.fastestInstance().safeDecompressor();

  private PageCodecs() {}

  static byte[] decompress(CompressionCodec codec, byte[] src, int offset, int length, int size)
      throws MalformedParquetException {
    if (codec == CompressionCodec.UNCOMPRESSED) {
      if (length != size) {
        throw new MalformedParquetException(
            "uncompressed page of " + length + " bytes claims " + size);
      }
      return Arrays.copyOfRange(src, offset, offset + length);
    }
    byte[] out = new byte[size];
    int produced;
    try {
      produced =
          switch (codec) {
            case SNAPPY -> snappy(src, offset, length, out);
            case GZIP -> gzip(src, offset, length, out);
            case ZSTD -> zstd(src, offset, length, out);
            case LZ4 -> hadoopLz4(src, offset, length, out);
            case LZ4_RAW -> LZ4.decompress(src, offset, length, out, 0, size);
            default ->
                throw new MalformedParquetException("parquet codec " + codec + " is not supported");
          };
    } catch (MalformedParquetException e) {
      throw e;
    } catch (IOException | RuntimeException e) {
      throw new MalformedParquetException(
          "parquet " + codec + " page does not decompress: " + e.getMessage(), e);
    }
    if (produced != size) {
      throw new MalformedParquetException(
          "parquet " + codec + " page decompressed to " + produced + " bytes, not " + size);
    }
    return out;
  }

  private static int snappy(byte[] src, int offset, int length, byte[] out) throws IOException {
    int claimed = Snappy.uncompressedLength(src, offset, length);
    if (claimed != out.length) {
      throw new MalformedParquetException(
          "snappy page holds " + claimed + " bytes, not " + out.length);
    }
    return Snappy.uncompress(src, offset, length, out, 0);
  }

  private static int gzip(byte[] src, int offset, int length, byte[] out) throws IOException {
    try (InputStream in = new GZIPInputStream(new ByteArrayInputStream(src, offset, length))) {
      int produced = in.readNBytes(out, 0, out.length);
      if (in.read() >= 0) {
        throw new MalformedParquetException("gzip page holds more than " + out.length + " bytes");
      }
      return produced;
    }
  }

  private static int zstd(byte[] src, int offset, int length, byte[] out)
      throws MalformedParquetException {
    long result = Zstd.decompressByteArray(out, 0, out.length, src, offset, length);
    if (Zstd.isError(result)) {
      throw new MalformedParquetException("zstd page: " + Zstd.getErrorName(result));
    }
    return (int) result;
  }

  /** Blocks of a big-endian raw length followed by chunks of a big-endian length and lz4 data. */
  private static int hadoopLz4(byte[] src, int offset, int length, byte[] out)
      throws MalformedParquetException {
    ByteBuffer in = ByteBuffer.wrap(src, offset, length);
    int produced = 0;
    while (in.hasRemaining()) {
      long raw = Integer.toUnsignedLong(int32(in));
      if (raw > out.length - produced) {
        throw new MalformedParquetException("lz4 block of " + raw + " bytes overflows the page");
      }
      long blockEnd = produced + raw;
      while (produced < blockEnd) {
        long compressed = Integer.toUnsignedLong(int32(in));
        if (compressed > in.remaining()) {
          throw new MalformedParquetException("lz4 chunk of " + compressed + " bytes is cut off");
        }
        produced +=
            LZ4.decompress(
                src, in.position(), (int) compressed, out, produced, (int) (blockEnd - produced));
        in.position(in.position() + (int) compressed);
      }
    }
    return produced;
  }

  private static int int32(ByteBuffer in) throws MalformedParquetException {
    if (in.remaining() < 4) {
      throw new MalformedParquetException("lz4 page is cut off inside a length");
    }
    return in.getInt();
  }
}
