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
package io.fleak.zephflow.lib.serdes.decode;

import static io.fleak.zephflow.lib.serdes.decode.DecodeTestSupport.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.luben.zstd.Zstd;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZOutputStream;

class CodecDecodeTest {

  @FunctionalInterface
  interface Compressor {
    OutputStream wrap(OutputStream out) throws IOException;
  }

  /** Expected messages when a stream is followed by garbage, cut after 2 bytes, or absent. */
  record Codec(
      String name, Compressor compressor, String garbage, String cutHeader, String notCompressed) {

    byte[] compress(String text) {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (OutputStream out = compressor.wrap(bytes)) {
        out.write(utf8(text));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return bytes.toByteArray();
    }

    @Override
    public String toString() {
      return name;
    }
  }

  static Stream<Codec> codecs() {
    return Stream.of(
        new Codec(
            "zstd",
            out -> new ZstdBytesOutput(out),
            "unexpected bytes after zstd frame 1",
            "zstd frame 2 is truncated",
            "input is not in zstd format"),
        new Codec(
            "snappy",
            FramedSnappyCompressorOutputStream::new,
            "snappy data is corrupt: Unskippable chunk with type 103 (hex 67) detected.",
            "snappy data is corrupt: Not a framed Snappy stream",
            "snappy data is corrupt: Not a framed Snappy stream"),
        new Codec(
            "lz4",
            FramedLZ4CompressorOutputStream::new,
            "lz4 data is corrupt: LZ4 frame stream followed by garbage",
            "lz4 data is corrupt: LZ4 frame stream followed by garbage",
            "lz4 data is corrupt: Not a LZ4 frame stream"),
        new Codec(
            "bzip2",
            BZip2CompressorOutputStream::new,
            "bzip2 data is corrupt: unexpected bytes after bzip2 stream 1",
            "bzip2 data is corrupt: bzip2 stream 2 is truncated",
            "bzip2 data is corrupt: input is not in bzip2 format"),
        new Codec(
            "xz",
            out -> new XZOutputStream(out, new LZMA2Options(6)),
            "xz data is corrupt: Garbage after a valid XZ Stream",
            "xz data is corrupt: unexpected end of data",
            "xz data is corrupt: unexpected end of data"),
        new Codec(
            "deflate",
            DeflaterOutputStream::new,
            "unexpected bytes after zlib stream 1",
            "zlib stream 2 is truncated",
            "input is not in zlib format"));
  }

  /** zstd-jni's output stream flushes frames oddly for tiny inputs; one-shot frames are plain. */
  private static final class ZstdBytesOutput extends ByteArrayOutputStream {
    private final OutputStream out;

    ZstdBytesOutput(OutputStream out) {
      this.out = out;
    }

    @Override
    public void close() throws IOException {
      out.write(Zstd.compress(toByteArray()));
      out.close();
    }
  }

  private static String lines(Codec codec) {
    return "{\"compression\":\""
        + codec.name()
        + "\",\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"line\"}}}";
  }

  private static final String FIRST = "line-0\nline-1\n";
  private static final String SECOND = "line-2\nline-3\n";

  private static Ok line(int i) {
    return new Ok(raw("line-" + i), i + 1, new RawSpan(7L * i, 6));
  }

  private static Bad corrupt(long index, long offset, String message) {
    return new Bad(
        ErrorKind.CORRUPT_COMPRESSION,
        "",
        index,
        RawSpan.toEnd(offset),
        "CorruptCompressionException: " + message,
        true);
  }

  @ParameterizedTest
  @MethodSource("codecs")
  void concatenatedStreamsAreAllRead(Codec codec) {
    assertEquals(
        List.of(line(0), line(1), line(2), line(3)),
        decode(lines(codec), concat(codec.compress(FIRST), codec.compress(SECOND))));
  }

  @ParameterizedTest
  @MethodSource("codecs")
  void garbageAfterTheLastStreamIsCorruptAfterEverythingBeforeIt(Codec codec) {
    assertEquals(
        List.of(line(0), line(1), corrupt(3, 14, codec.garbage())),
        decode(lines(codec), concat(codec.compress(FIRST), utf8("garbage!garbage!garbage!"))));
  }

  @ParameterizedTest
  @MethodSource("codecs")
  void streamCutShortIsCorruptAfterEverythingBeforeIt(Codec codec) {
    byte[] second = codec.compress(SECOND);
    assertEquals(
        List.of(line(0), line(1), corrupt(3, 14, codec.cutHeader())),
        decode(lines(codec), concat(codec.compress(FIRST), Arrays.copyOf(second, 2))));
  }

  @ParameterizedTest
  @MethodSource("codecs")
  void inputThatIsNotCompressedIsCorruptAndEmptyInputIsEmpty(Codec codec) {
    assertEquals(
        List.of(corrupt(1, 0, codec.notCompressed())), decode(lines(codec), utf8("plain text\n")));
    assertEquals(List.of(), decode(lines(codec), new byte[0]));
  }

  @ParameterizedTest
  @MethodSource("codecs")
  void readFailureIsNotTakenForCorruptData(Codec codec) {
    byte[] first = codec.compress(FIRST);
    byte[] input = concat(first, codec.compress(SECOND));
    assertEquals(
        List.of(line(0), line(1), new Threw("SourceReadException")),
        decode(decoder(lines(codec)), failingAt(input, first.length + 1), DecodeLimits.UNLIMITED));
  }

  @Test
  void zstdLastFrameCutInTheMiddleIsTruncatedNotACleanEnd() {
    Codec zstd = codecs().findFirst().orElseThrow();
    byte[] second = zstd.compress(SECOND.repeat(100));
    assertEquals(
        List.of(line(0), line(1), corrupt(3, 14, "zstd frame 2 is truncated")),
        decode(
            lines(zstd), concat(zstd.compress(FIRST), Arrays.copyOf(second, second.length - 3))));
  }

  @Test
  void skippableFramesAndChunksAreSkipped() {
    byte[] zstdSkippable = {0x50, 0x2A, 0x4D, 0x18, 3, 0, 0, 0, 'a', 'b', 'c'};
    List<Codec> all = codecs().toList();
    assertEquals(
        List.of(line(0), line(1), line(2), line(3)),
        decode(
            lines(all.get(0)),
            concat(all.get(0).compress(FIRST), zstdSkippable, all.get(0).compress(SECOND))));
    assertEquals(
        List.of(line(0), line(1), line(2), line(3)),
        decode(
            lines(all.get(2)),
            concat(all.get(2).compress(FIRST), zstdSkippable, all.get(2).compress(SECOND))));
    byte[] snappy = all.get(1).compress(FIRST + SECOND);
    byte[] skippable = {(byte) 0x80, 2, 0, 0, 'x', 'y'};
    byte[] withSkippable =
        concat(Arrays.copyOf(snappy, 10), skippable, Arrays.copyOfRange(snappy, 10, snappy.length));
    assertEquals(
        List.of(line(0), line(1), line(2), line(3)), decode(lines(all.get(1)), withSkippable));
  }

  @Test
  void zlibStreamThatNeedsAPresetDictionaryIsCorrupt() {
    Deflater deflater = new Deflater();
    deflater.setDictionary(utf8("line-"));
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DeflaterOutputStream out = new DeflaterOutputStream(bytes, deflater)) {
      out.write(utf8(FIRST));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    assertEquals(
        List.of(corrupt(1, 0, "zlib stream 1 needs a preset dictionary")),
        decode(lines(codecs().toList().get(5)), bytes.toByteArray()));
  }

  @Test
  void codecsOverTheMemoryLimitAreOversized() throws Exception {
    Codec xz = codecs().toList().get(4);
    long needed = new LZMA2Options(6).getDecoderMemoryUsage() * 1024L;
    assertEquals(
        List.of(new Big(1, null, needed, "", true)),
        decode(
            decoder(lines(xz)),
            ByteInput.ofBytes(xz.compress(FIRST)),
            new DecodeLimits(0, 4 << 20)));
    byte[] hugeWindow = {0x28, (byte) 0xB5, 0x2F, (byte) 0xFD, 0x00, (byte) 0xA0, 1, 0, 0};
    assertEquals(
        List.of(new Big(1, null, 0, "", true)),
        decode(lines(codecs().findFirst().orElseThrow()), hugeWindow));
  }

  @Test
  void codecsChainFromTheOutermost() {
    Codec zstd = codecs().findFirst().orElseThrow();
    assertEquals(
        List.of(line(0), line(1)),
        decode(
            "{\"compression\":[\"zstd\",\"gzip\"],\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"line\"}}}",
            Zstd.compress(gzipOne(utf8(FIRST)))));
  }
}
