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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.luben.zstd.Zstd;
import com.github.luben.zstd.ZstdCompressCtx;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream.BlockSize;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream.Parameters;
import org.apache.commons.compress.compressors.lz4.XXHash32;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
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
      return compressBytes(utf8(text));
    }

    byte[] compressBytes(byte[] data) {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (OutputStream out = compressor.wrap(bytes)) {
        out.write(data);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      return bytes.toByteArray();
    }

    String config() {
      return "{\"compression\":\""
          + name
          + "\",\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"line\"}}}";
    }

    @Override
    public String toString() {
      return name;
    }
  }

  private static final Codec ZSTD =
      new Codec(
          "zstd",
          OneShotZstd::new,
          "unexpected bytes after zstd frame 1",
          "zstd frame 2 is truncated",
          "input is not in zstd format");
  private static final Codec SNAPPY =
      new Codec(
          "snappy",
          FramedSnappyCompressorOutputStream::new,
          "snappy data is corrupt: Unskippable chunk with type 103 (hex 67) detected.",
          "snappy data is corrupt: Not a framed Snappy stream",
          "snappy data is corrupt: Not a framed Snappy stream");
  private static final Codec LZ4 =
      new Codec(
          "lz4",
          FramedLZ4CompressorOutputStream::new,
          "unexpected bytes after lz4 frame 1",
          "lz4 frame 2 is truncated",
          "input is not in lz4 frame format");
  private static final Codec BZIP2 =
      new Codec(
          "bzip2",
          BZip2CompressorOutputStream::new,
          "bzip2 data is corrupt: unexpected bytes after bzip2 stream 1",
          "bzip2 data is corrupt: bzip2 stream 2 is truncated",
          "bzip2 data is corrupt: input is not in bzip2 format");
  private static final Codec XZ =
      new Codec(
          "xz",
          out -> new XZOutputStream(out, new LZMA2Options(6)),
          "xz data is corrupt: Garbage after a valid XZ Stream",
          "xz data is corrupt: unexpected end of data",
          "xz data is corrupt: unexpected end of data");
  private static final Codec DEFLATE =
      new Codec(
          "deflate",
          DeflaterOutputStream::new,
          "unexpected bytes after zlib stream 1",
          "zlib stream 2 is truncated",
          "input is not in zlib format");

  static Stream<Codec> codecs() {
    return Stream.of(ZSTD, SNAPPY, LZ4, BZIP2, XZ, DEFLATE);
  }

  /** One-shot zstd frames, as most producers write them. */
  private static final class OneShotZstd extends ByteArrayOutputStream {
    private final OutputStream out;

    OneShotZstd(OutputStream out) {
      this.out = out;
    }

    @Override
    public void close() throws IOException {
      out.write(Zstd.compress(toByteArray()));
      out.close();
    }
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

  private static String numbered(int count) {
    StringBuilder text = new StringBuilder();
    for (int i = 0; i < count; i++) {
      text.append("line-").append(i).append('\n');
    }
    return text.toString();
  }

  /** Expected outcomes for the complete lines of {@code text}, as numbered() writes them. */
  private static List<Object> completeLines(String text) {
    List<Object> expected = new ArrayList<>();
    long offset = 0;
    int newline;
    while ((newline = text.indexOf('\n', (int) offset)) >= 0) {
      String line = text.substring((int) offset, newline);
      expected.add(new Ok(raw(line), expected.size() + 1, new RawSpan(offset, line.length())));
      offset = newline + 1;
    }
    return expected;
  }

  @ParameterizedTest
  @MethodSource("codecs")
  void concatenatedStreamsAreAllRead(Codec codec) {
    assertEquals(
        List.of(line(0), line(1), line(2), line(3)),
        decode(codec.config(), concat(codec.compress(FIRST), codec.compress(SECOND))));
  }

  @ParameterizedTest
  @MethodSource("codecs")
  void garbageAfterTheLastStreamIsCorruptAfterEverythingBeforeIt(Codec codec) {
    assertEquals(
        List.of(line(0), line(1), corrupt(3, 14, codec.garbage())),
        decode(codec.config(), concat(codec.compress(FIRST), utf8("garbage!garbage!garbage!"))));
  }

  @ParameterizedTest
  @MethodSource("codecs")
  void streamCutShortIsCorruptAfterEverythingBeforeIt(Codec codec) {
    byte[] second = codec.compress(SECOND);
    assertEquals(
        List.of(line(0), line(1), corrupt(3, 14, codec.cutHeader())),
        decode(codec.config(), concat(codec.compress(FIRST), Arrays.copyOf(second, 2))));
  }

  @ParameterizedTest
  @MethodSource("codecs")
  void inputThatIsNotCompressedIsCorruptAndEmptyInputIsEmpty(Codec codec) {
    assertEquals(
        List.of(corrupt(1, 0, codec.notCompressed())),
        decode(codec.config(), utf8("plain text\n")));
    assertEquals(List.of(), decode(codec.config(), new byte[0]));
  }

  @ParameterizedTest
  @MethodSource("codecs")
  void readFailureIsNotTakenForCorruptData(Codec codec) {
    byte[] first = codec.compress(FIRST);
    byte[] input = concat(first, codec.compress(SECOND));
    assertEquals(
        List.of(line(0), line(1), new Threw("SourceReadException")),
        decode(
            decoder(codec.config()), failingAt(input, first.length + 1), DecodeLimits.UNLIMITED));
  }

  @Test
  void zstdLastFrameCutInTheMiddleIsTruncatedNotACleanEnd() {
    byte[] second = ZSTD.compress(SECOND.repeat(100));
    assertEquals(
        List.of(line(0), line(1), corrupt(3, 14, "zstd frame 2 is truncated")),
        decode(
            ZSTD.config(), concat(ZSTD.compress(FIRST), Arrays.copyOf(second, second.length - 3))));
  }

  @Test
  void lz4FrameCutAfterAnUncompressedBlockSizeIsTruncated() {
    byte[] random = new byte[100];
    new Random(1).nextBytes(random);
    byte[] second = LZ4.compressBytes(concat(random, utf8("\n")));
    assertTrue(second[10] < 0, "the block must be stored uncompressed");
    assertEquals(
        List.of(line(0), line(1), corrupt(3, 14, "lz4 frame 2 is truncated")),
        decode(LZ4.config(), concat(LZ4.compress(FIRST), Arrays.copyOf(second, 11))));
  }

  @Test
  void lz4LinkedBlocksAndChecksumsDecode() throws IOException {
    String text = numbered(50_000);
    Parameters parameters = new Parameters(BlockSize.K64, true, true, true);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (OutputStream out = new FramedLZ4CompressorOutputStream(bytes, parameters)) {
      out.write(utf8(text));
    }
    assertEquals(completeLines(text), decode(LZ4.config(), bytes.toByteArray()));
    byte[] damaged = bytes.toByteArray();
    damaged[damaged.length - 1] ^= 1;
    List<Object> expected = new ArrayList<>(completeLines(text));
    expected.add(corrupt(50_001, text.length(), "lz4 frame 1 content checksum mismatch"));
    assertEquals(expected, decode(LZ4.config(), damaged));
  }

  @Test
  void lz4BlockChecksumMismatchHandsOnNothingFromThatBlock() throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (OutputStream out =
        new FramedLZ4CompressorOutputStream(
            bytes, new Parameters(BlockSize.K64, false, true, false))) {
      out.write(utf8(FIRST));
    }
    byte[] damaged = bytes.toByteArray();
    damaged[12] ^= 1;
    assertEquals(
        List.of(corrupt(1, 0, "lz4 block checksum mismatch in frame 1")),
        decode(LZ4.config(), damaged));
  }

  @Test
  void skippableFramesAndChunksAreSkipped() {
    byte[] skippableFrame = {0x50, 0x2A, 0x4D, 0x18, 3, 0, 0, 0, 'a', 'b', 'c'};
    List<Object> all = List.of(line(0), line(1), line(2), line(3));
    assertEquals(
        all,
        decode(ZSTD.config(), concat(ZSTD.compress(FIRST), skippableFrame, ZSTD.compress(SECOND))));
    assertEquals(
        all,
        decode(LZ4.config(), concat(LZ4.compress(FIRST), skippableFrame, LZ4.compress(SECOND))));
    byte[] snappy = SNAPPY.compress(FIRST + SECOND);
    byte[] skippableChunk = {(byte) 0x80, 2, 0, 0, 'x', 'y'};
    assertEquals(
        all,
        decode(
            SNAPPY.config(),
            concat(
                Arrays.copyOf(snappy, 10),
                skippableChunk,
                Arrays.copyOfRange(snappy, 10, snappy.length))));
  }

  @Test
  void snappyCutAtAChunkBoundaryEndsCleanlyBecauseTheFormatHasNoEndMark() {
    byte[] second = SNAPPY.compress(SECOND);
    assertEquals(
        List.of(line(0), line(1)),
        decode(SNAPPY.config(), concat(SNAPPY.compress(FIRST), Arrays.copyOf(second, 10))));
  }

  @Test
  void xzStreamPaddingIsAllowed() {
    assertEquals(
        List.of(line(0), line(1), line(2), line(3)),
        decode(XZ.config(), concat(XZ.compress(FIRST), new byte[4], XZ.compress(SECOND))));
  }

  @Test
  void xzMemoryLimitIsMaxDocumentBytesInWholeKibibytes() throws Exception {
    long needed = new LZMA2Options(6).getDecoderMemoryUsage() * 1024L;
    byte[] input = XZ.compress(FIRST);
    assertEquals(
        List.of(line(0), line(1)),
        decode(decoder(XZ.config()), ByteInput.ofBytes(input), new DecodeLimits(0, needed)));
    assertEquals(
        List.of(new Big(1, null, needed, "", true)),
        decode(decoder(XZ.config()), ByteInput.ofBytes(input), new DecodeLimits(0, needed - 1024)));
  }

  @Test
  void zstdWindowOverItsLimitIsOversized() {
    byte[] hugeWindow = {0x28, (byte) 0xB5, 0x2F, (byte) 0xFD, 0x00, (byte) 0xA0, 1, 0, 0};
    assertEquals(List.of(new Big(1, null, 0, "", true)), decode(ZSTD.config(), hugeWindow));
  }

  @Test
  void anOuterCodecsLimitStaysOversizedThroughInnerCodecs() throws Exception {
    long needed = new LZMA2Options(6).getDecoderMemoryUsage() * 1024L;
    assertEquals(
        List.of(new Big(1, null, needed, "", true)),
        decode(
            decoder(
                "{\"compression\":[\"xz\",\"bzip2\"],\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"line\"}}}"),
            ByteInput.ofBytes(XZ.compressBytes(BZIP2.compress(FIRST))),
            new DecodeLimits(0, 1 << 20)));
    byte[] hugeWindow = {0x28, (byte) 0xB5, 0x2F, (byte) 0xFD, 0x00, (byte) 0xA0, 1, 0, 0};
    assertEquals(
        List.of(new Big(1, null, 0, "", true)),
        decode(
            "{\"compression\":[\"zstd\",\"snappy\"],\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"line\"}}}",
            hugeWindow));
  }

  @Test
  void checksumMismatchesAreCorrupt() {
    byte[] zstd = new ZstdCompressCtx().setChecksum(true).compress(utf8(FIRST));
    zstd[zstd.length - 1] ^= 1;
    assertEquals(
        List.of(corrupt(1, 0, "zstd frame 1 checksum mismatch")), decode(ZSTD.config(), zstd));
    byte[] zlib = DEFLATE.compress(FIRST);
    zlib[zlib.length - 1] ^= 1;
    assertEquals(
        List.of(line(0), line(1), corrupt(3, 14, "zlib data is corrupt: incorrect data check")),
        decode(DEFLATE.config(), zlib));
  }

  @Test
  void zlibHandsOnEverythingInflatedBeforeDamagedData() {
    String text = numbered(50_000);
    byte[] damaged = DEFLATE.compress(text);
    damaged[damaged.length / 2] ^= (byte) 0xFF;
    Inflater reference = new Inflater();
    reference.setInput(damaged);
    ByteArrayOutputStream released = new ByteArrayOutputStream();
    byte[] one = new byte[1];
    String message = null;
    try {
      while (!reference.finished() && reference.inflate(one) == 1) {
        released.write(one[0]);
      }
    } catch (DataFormatException e) {
      message = e.getMessage();
    } finally {
      reference.end();
    }
    String prefix = released.toString(StandardCharsets.UTF_8);
    List<Object> expected = new ArrayList<>(completeLines(prefix));
    long offset = prefix.lastIndexOf('\n') + 1;
    expected.add(corrupt(expected.size() + 1, offset, "zlib data is corrupt: " + message));
    assertEquals(expected, decode(DEFLATE.config(), damaged));
  }

  /** What a library stream releases before failing when read the way the decoder reads it. */
  private static List<Object> releasedInPieces(InputStream library, String messagePrefix) {
    ByteArrayOutputStream released = new ByteArrayOutputStream();
    String message = null;
    byte[] piece = new byte[256];
    try (library) {
      int n;
      while ((n = library.read(piece, 0, piece.length)) >= 0) {
        released.write(piece, 0, n);
      }
    } catch (IOException e) {
      message = e.getMessage();
    }
    String prefix = released.toString(StandardCharsets.UTF_8);
    List<Object> expected = new ArrayList<>(completeLines(prefix));
    expected.add(
        corrupt(expected.size() + 1, prefix.lastIndexOf('\n') + 1, messagePrefix + message));
    return expected;
  }

  @Test
  void libraryCodecsHandOnWhatTheyReleaseInPiecesBeforeDamagedData() throws IOException {
    byte[] damaged = BZIP2.compress(numbered(200_000));
    damaged[damaged.length / 2] ^= (byte) 0xFF;
    assertEquals(
        releasedInPieces(
            new BZip2CompressorInputStream(new ByteArrayInputStream(damaged), false),
            "bzip2 data is corrupt: "),
        decode(BZIP2.config(), damaged));
  }

  @Test
  void lz4BlockWithoutChecksumIsHandedOnAsItDecodes() throws IOException {
    String text = numbered(200_000);
    byte[] whole = LZ4.compress(text);
    byte[] cut = Arrays.copyOf(whole, whole.length / 2);
    ByteArrayOutputStream released = new ByteArrayOutputStream();
    try (InputStream reference =
        new FramedLZ4CompressorInputStream(new ByteArrayInputStream(cut))) {
      int b;
      while ((b = reference.read()) >= 0) {
        released.write(b);
      }
    } catch (IOException expected) {
      // The cut.
    }
    List<Object> actual = decode(LZ4.config(), cut);
    long delivered =
        actual.stream()
            .filter(Ok.class::isInstance)
            .mapToLong(o -> ((Ok) o).span().length() + 1)
            .sum();
    // Piece boundaries inside the block decoder differ from a byte-at-a-time read, so the
    // documented bound is checked rather than an exact list.
    assertTrue(
        delivered > released.size() - 256 - 16 && delivered <= released.size(),
        "delivered " + delivered + " of " + released.size());
    assertEquals(
        corrupt(actual.size(), delivered, "lz4 frame 1 is truncated"),
        actual.get(actual.size() - 1));
  }

  @Test
  void lz4SkippableFrameCutShortIsCorruptEvenFromAFile(@TempDir Path dir) throws IOException {
    byte[] skippable = {
      0x50, 0x2A, 0x4D, 0x18, (byte) 0xE8, 3, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    };
    Path file = dir.resolve("cut.lz4");
    Files.write(file, concat(LZ4.compress(FIRST), skippable));
    assertEquals(
        List.of(line(0), line(1), corrupt(3, 14, "lz4 skippable frame is truncated")),
        decode(
            decoder(LZ4.config()),
            () -> new FileInputStream(file.toFile()),
            DecodeLimits.UNLIMITED));
  }

  /** A frame of {@code blocks} with the given FLG and BD bytes and a correct header checksum. */
  private static byte[] lz4Frame(int flg, int bd, byte[] contentSize, byte[]... blocks) {
    ByteArrayOutputStream descriptor = new ByteArrayOutputStream();
    descriptor.write(flg);
    descriptor.write(bd);
    descriptor.writeBytes(contentSize);
    XXHash32 hash = new XXHash32();
    hash.update(descriptor.toByteArray(), 0, descriptor.size());
    ByteArrayOutputStream frame = new ByteArrayOutputStream();
    frame.writeBytes(new byte[] {0x04, 0x22, 0x4D, 0x18});
    frame.writeBytes(descriptor.toByteArray());
    frame.write((int) (hash.getValue() >> 8) & 0xFF);
    for (byte[] block : blocks) {
      frame.writeBytes(block);
    }
    frame.writeBytes(new byte[4]);
    return frame.toByteArray();
  }

  private static byte[] storedBlock(byte[] data, int declaredSize) {
    return concat(
        new byte[] {
          (byte) declaredSize,
          (byte) (declaredSize >> 8),
          (byte) (declaredSize >> 16),
          (byte) ((declaredSize >> 24) | 0x80)
        },
        data);
  }

  @Test
  void lz4FrameHeadersAreChecked() {
    byte[] data = utf8(FIRST);
    byte[] good = lz4Frame(0x60, 0x40, new byte[0], storedBlock(data, data.length));
    assertEquals(List.of(line(0), line(1)), decode(LZ4.config(), good));
    byte[] badChecksum = good.clone();
    badChecksum[6] ^= 1;
    byte[] sizeEight = {(byte) 99, 0, 0, 0, 0, 0, 0, 0};
    List<Object[]> cases =
        List.of(
            new Object[] {badChecksum, "lz4 frame 1 header checksum mismatch"},
            new Object[] {
              lz4Frame(0x61, 0x40, new byte[0]), "lz4 frames with a dictionary are not supported"
            },
            new Object[] {
              lz4Frame(0x20, 0x40, new byte[0]), "lz4 frame 1 has an unsupported header"
            },
            new Object[] {
              lz4Frame(0x60, 0x30, new byte[0]), "lz4 frame 1 has an invalid block size"
            },
            new Object[] {
              lz4Frame(0x60, 0x40, new byte[0], storedBlock(data, 70_000)),
              "lz4 block of 70000 bytes exceeds the frame's block size"
            },
            new Object[] {
              lz4Frame(0x68, 0x40, sizeEight, storedBlock(data, data.length)),
              "lz4 frame 1 content size mismatch"
            });
    for (Object[] c : cases) {
      List<Object> actual = decode(LZ4.config(), (byte[]) c[0]);
      Bad last = (Bad) actual.get(actual.size() - 1);
      assertEquals(
          corrupt(last.recordIndex(), last.span().offset(), (String) c[1]), last, (String) c[1]);
    }
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
        decode(DEFLATE.config(), bytes.toByteArray()));
  }

  @Test
  void codecsChainFromTheOutermost() {
    assertEquals(
        List.of(line(0), line(1)),
        decode(
            "{\"compression\":[\"zstd\",\"gzip\"],\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"line\"}}}",
            Zstd.compress(gzipOne(utf8(FIRST)))));
  }
}
