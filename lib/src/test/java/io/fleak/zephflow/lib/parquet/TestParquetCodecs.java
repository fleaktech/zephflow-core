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
package io.fleak.zephflow.lib.parquet;

import com.github.luben.zstd.Zstd;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.zip.GZIPOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.xerial.snappy.Snappy;

/** Page compressors for test fixtures; parquet's own need Hadoop, which is not on the classpath. */
public enum TestParquetCodecs implements CompressionCodecFactory {
  INSTANCE;

  private static final LZ4Compressor LZ4 = LZ4Factory.fastestInstance().fastCompressor();

  @Override
  public BytesInputCompressor getCompressor(CompressionCodecName codec) {
    return new BytesInputCompressor() {
      @Override
      public BytesInput compress(BytesInput bytes) throws IOException {
        return BytesInput.from(TestParquetCodecs.compress(codec, bytes.toByteArray()));
      }

      @Override
      public CompressionCodecName getCodecName() {
        return codec;
      }

      @Override
      public void release() {}
    };
  }

  @Override
  public BytesInputDecompressor getDecompressor(CompressionCodecName codec) {
    throw new UnsupportedOperationException("fixtures are only written");
  }

  @Override
  public void release() {}

  private static byte[] compress(CompressionCodecName codec, byte[] raw) throws IOException {
    return switch (codec) {
      case UNCOMPRESSED -> raw;
      case SNAPPY -> Snappy.compress(raw);
      case ZSTD -> Zstd.compress(raw);
      case GZIP -> gzip(raw);
      case LZ4 -> hadoopLz4(raw);
      default -> throw new UnsupportedOperationException("no test compressor for " + codec);
    };
  }

  private static byte[] gzip(byte[] raw) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
      gzip.write(raw);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return out.toByteArray();
  }

  /** Hadoop's block framing: raw length, then one chunk of compressed length and bytes. */
  private static byte[] hadoopLz4(byte[] raw) {
    byte[] chunk = LZ4.compress(raw);
    return ByteBuffer.allocate(8 + chunk.length)
        .putInt(raw.length)
        .putInt(chunk.length)
        .put(chunk)
        .array();
  }
}
