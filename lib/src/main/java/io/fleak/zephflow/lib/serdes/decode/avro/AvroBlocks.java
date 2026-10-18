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

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdIOException;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import io.fleak.zephflow.lib.serdes.decode.internal.ByteBuilder;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.snappy.SnappyCompressorInputStream;
import org.tukaani.xz.MemoryLimitException;
import org.tukaani.xz.XZInputStream;

/** Decompresses one avro block, stopping once the output would exceed the limit. */
final class AvroBlocks {

  /** The block decompresses to more than the limit. */
  static final class TooLarge extends Exception {
    final long size;

    TooLarge(long size) {
      super(null, null, false, false);
      this.size = size;
    }
  }

  /** The block's compressed data is damaged. */
  static final class CorruptBlockException extends MalformedAvroException {
    CorruptBlockException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  private AvroBlocks() {}

  /** The largest block a byte array can hold. */
  static final long MAX_BLOCK = Integer.MAX_VALUE - 8;

  /**
   * {@code limit} caps the decompressed size; 0 means no cap beyond what a byte array can hold. The
   * xz decoder's memory is a separate matter: it follows the codec default, not this limit.
   */
  static byte[] decompress(String codec, byte[] block, long limit)
      throws TooLarge, CorruptBlockException {
    long cap = limit > 0 ? limit : MAX_BLOCK;
    try {
      return switch (codec) {
        case "null" -> block;
        case "deflate" -> inflate(block, cap);
        case "snappy" -> snappy(block, cap);
        case "zstandard" ->
            drain(new ZstdInputStreamNoFinalizer(new ByteArrayInputStream(block)), cap);
        case "bzip2" -> drain(new BZip2CompressorInputStream(new ByteArrayInputStream(block)), cap);
        case "xz" -> drain(new XZInputStream(new ByteArrayInputStream(block), xzMemoryKiB()), cap);
        default ->
            throw new IllegalStateException("codec checked when the header was read: " + codec);
      };
    } catch (MemoryLimitException e) {
      throw new TooLarge(e.getMemoryNeeded() * 1024L);
    } catch (ZstdIOException e) {
      if (e.getErrorCode() == Zstd.errFrameParameterWindowTooLarge()) {
        throw new TooLarge(0);
      }
      throw new CorruptBlockException("avro zstandard block is corrupt: " + e.getMessage(), e);
    } catch (CorruptBlockException e) {
      throw e;
    } catch (IOException | RuntimeException e) {
      throw new CorruptBlockException("avro " + codec + " block is corrupt: " + e.getMessage(), e);
    }
  }

  private static int xzMemoryKiB() {
    long bytes = Math.min(128L * 1024 * 1024, Runtime.getRuntime().maxMemory() / 2);
    return (int) ((bytes + 1023) / 1024);
  }

  /** Avro's deflate codec is raw deflate (RFC 1951), without a zlib header. */
  private static byte[] inflate(byte[] block, long limit) throws TooLarge, CorruptBlockException {
    Inflater inflater = new Inflater(true);
    try {
      inflater.setInput(block);
      ByteBuilder out = new ByteBuilder();
      byte[] buf = new byte[8 * 1024];
      while (!inflater.finished()) {
        int n = inflater.inflate(buf);
        if (n == 0 && !inflater.finished()) {
          throw new CorruptBlockException(
              inflater.needsInput()
                  ? "avro deflate block is truncated"
                  : "avro deflate block is corrupt",
              null);
        }
        append(out, buf, n, limit);
      }
      if (inflater.getRemaining() > 0) {
        throw new CorruptBlockException("avro deflate block has bytes after its data", null);
      }
      return out.toByteArray();
    } catch (DataFormatException e) {
      throw new CorruptBlockException("avro deflate block is corrupt: " + e.getMessage(), e);
    } finally {
      inflater.end();
    }
  }

  /**
   * Avro's snappy codec is raw snappy followed by the big-endian CRC32 of the uncompressed data.
   */
  private static byte[] snappy(byte[] block, long limit)
      throws IOException, TooLarge, CorruptBlockException {
    if (block.length < 4) {
      throw new CorruptBlockException("avro snappy block is too short", null);
    }
    byte[] data =
        drain(
            new SnappyCompressorInputStream(new ByteArrayInputStream(block, 0, block.length - 4)),
            limit);
    CRC32 crc = new CRC32();
    crc.update(data);
    int expected =
        (block[block.length - 4] & 0xFF) << 24
            | (block[block.length - 3] & 0xFF) << 16
            | (block[block.length - 2] & 0xFF) << 8
            | (block[block.length - 1] & 0xFF);
    if ((int) crc.getValue() != expected) {
      throw new CorruptBlockException("avro snappy block checksum mismatch", null);
    }
    return data;
  }

  private static byte[] drain(InputStream in, long limit) throws IOException, TooLarge {
    try (in) {
      ByteBuilder out = new ByteBuilder();
      byte[] buf = new byte[8 * 1024];
      int n;
      while ((n = in.read(buf)) >= 0) {
        append(out, buf, n, limit);
      }
      return out.toByteArray();
    }
  }

  private static void append(ByteBuilder out, byte[] buf, int n, long limit) throws TooLarge {
    if (out.size() + (long) n > limit) {
      throw new TooLarge(out.size() + (long) n);
    }
    out.append(buf, 0, n);
  }
}
