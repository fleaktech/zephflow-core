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

import io.fleak.zephflow.lib.serdes.decode.DecodeLimits;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.internal.DecodeFlags;
import io.fleak.zephflow.lib.serdes.decode.internal.PeekedStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorInputStream;
import org.tukaani.xz.MemoryLimitException;
import org.tukaani.xz.XZInputStream;

public final class Decompressors {

  private Decompressors() {}

  /** Applies the codecs in order: the first entry undoes the outermost compression. */
  public static InputStream wrap(
      List<DecompressionCodec> codecs, InputStream in, DecodeFlags flags, DecodeLimits limits)
      throws IOException {
    InputStream current = in;
    for (DecompressionCodec codec : codecs) {
      current =
          switch (codec) {
            case GZIP -> new GzipMemberStream(current, flags);
            case AUTO -> autoDetect(current, flags);
            case ZSTD -> new ZstdFrameStream(current, flags);
            case DEFLATE -> new ZlibStream(current, flags);
            case BZIP2 -> library("bzip2", current, Bzip2Streams::new, flags);
            case LZ4 ->
                library("lz4", current, s -> new FramedLZ4CompressorInputStream(s, true), flags);
            case SNAPPY ->
                library("snappy", current, FramedSnappyCompressorInputStream::new, flags);
            case XZ -> xz(current, flags, limits.maxDocumentBytes());
          };
    }
    return current;
  }

  private static InputStream library(
      String name, InputStream in, LibraryCodecStream.Opener opener, DecodeFlags flags) {
    return new LibraryCodecStream(name, in, opener, e -> -1, flags);
  }

  /** The decoder's memory limit is {@code maxDocumentBytes}; without one, xz has none either. */
  private static InputStream xz(InputStream in, DecodeFlags flags, long maxDocumentBytes) {
    int limitKiB =
        maxDocumentBytes > 0
            ? (int) Math.min(Integer.MAX_VALUE, (maxDocumentBytes + 1023) / 1024)
            : -1;
    return new LibraryCodecStream(
        "xz",
        in,
        s -> new XZInputStream(s, limitKiB),
        e -> e instanceof MemoryLimitException m ? m.getMemoryNeeded() * 1024L : -1,
        flags);
  }

  private static InputStream autoDetect(InputStream in, DecodeFlags flags) throws IOException {
    PeekedStream peeked =
        PeekedStream.peek(in, 2, head -> head.length >= 1 && (head[0] & 0xFF) != 0x1f);
    byte[] magic = peeked.head();
    boolean gzip = magic.length == 2 && (magic[0] & 0xFF) == 0x1f && (magic[1] & 0xFF) == 0x8b;
    return gzip ? new GzipMemberStream(peeked, flags) : peeked;
  }
}
