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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.github.luben.zstd.Zstd;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Stream;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream;
import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.tukaani.xz.LZMA2Options;
import org.tukaani.xz.XZOutputStream;

class UndeclaredCompressionTest {

  private static final String TEXT_LINE = "{\"type\":\"text\",\"framing\":{\"type\":\"line\"}}";
  private static final String TEXT_WHOLE = "{\"type\":\"text\",\"framing\":{\"type\":\"whole\"}}";
  private static final String TEXT_MULTILINE =
      "{\"type\":\"text\",\"framing\":{\"type\":\"multiline\",\"startPattern\":\"^a\"}}";
  private static final String CSV = "{\"type\":\"csv\"}";
  private static final byte[] SKIPPABLE_FRAME = {0x50, 0x2a, 0x4d, 0x18, 1, 0, 0, 0, 0};

  @FunctionalInterface
  private interface Compressor {
    OutputStream wrap(OutputStream out) throws IOException;
  }

  private static byte[] compress(Compressor compressor) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (OutputStream out = compressor.wrap(bytes)) {
      out.write(utf8("a,b\n1,2\n"));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return bytes.toByteArray();
  }

  private static String config(String compression, String format) {
    return "{\"compression\":" + compression + ",\"format\":" + format + "}";
  }

  static Stream<Arguments> compressed() {
    return Stream.of(
            Arguments.of("gzip", gzip("a,b\n1,2\n")),
            Arguments.of("zstd", Zstd.compress(utf8("a,b\n1,2\n"))),
            Arguments.of("xz", compress(out -> new XZOutputStream(out, new LZMA2Options(6)))),
            Arguments.of("bzip2", compress(BZip2CompressorOutputStream::new)),
            Arguments.of("lz4", compress(FramedLZ4CompressorOutputStream::new)),
            Arguments.of("snappy", compress(FramedSnappyCompressorOutputStream::new)),
            Arguments.of("deflate", compress(DeflaterOutputStream::new)),
            Arguments.of(
                "deflate",
                compress(
                    out -> new DeflaterOutputStream(out, new Deflater(Deflater.BEST_COMPRESSION)))),
            Arguments.of(
                "bzip2",
                new byte[] {'B', 'Z', 'h', '9', 0x17, 0x72, 0x45, 0x38, 0x50, (byte) 0x90}),
            Arguments.of("zstd or lz4", concat(SKIPPABLE_FRAME, Zstd.compress(utf8("a,b\n1,2\n")))),
            Arguments.of(
                "zstd or lz4",
                concat(SKIPPABLE_FRAME, compress(FramedLZ4CompressorOutputStream::new))))
        .flatMap(
            codec ->
                Stream.of(TEXT_LINE, TEXT_WHOLE, TEXT_MULTILINE, CSV)
                    .map(format -> Arguments.of(codec.get()[0], format, codec.get()[1])));
  }

  @ParameterizedTest(name = "{0} {1}")
  @MethodSource("compressed")
  void refusesCompressedDataWithNoCompressionConfigured(String codec, String format, byte[] bytes) {
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                "",
                1,
                RawSpan.toEnd(0),
                "UndeclaredCompressionException: The data starts with a "
                    + codec
                    + " signature, but no compression is configured. Set the compression the data"
                    + " uses.",
                true)),
        decode(config("[]", format), bytes));
  }

  @Test
  void noCompressionKeyMeansNoCompression() {
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                "",
                1,
                RawSpan.toEnd(0),
                "UndeclaredCompressionException: The data starts with a gzip signature, but no"
                    + " compression is configured. Set the compression the data uses.",
                true)),
        decode("{\"format\":" + TEXT_LINE + "}", gzip("x\n")));
  }

  @Test
  void configuredCompressionStillReads() {
    assertEquals(
        List.of(new Ok(raw("x"), 1, new RawSpan(0, 1))),
        decode(config("[\"gzip\"]", TEXT_LINE), gzip("x\n")));
  }

  @Test
  void autoCompressionStillReads() {
    assertEquals(
        List.of(new Ok(raw("x"), 1, new RawSpan(0, 1))),
        decode(config("[\"auto\"]", TEXT_LINE), gzip("x\n")));
  }

  @Test
  void textThatOnlyResemblesASignatureIsRead() {
    assertEquals(
        List.of(new Ok(raw("BZh9 report"), 1, new RawSpan(0, 11))),
        decode(config("[]", TEXT_LINE), utf8("BZh9 report\n")));
  }

  @Test
  void textThatStartsLikeAWeakZlibHeaderIsRead() {
    assertEquals(
        List.of(new Ok(raw("x^2 + 1"), 1, new RawSpan(0, 7))),
        decode(config("[]", TEXT_LINE), utf8("x^2 + 1\n")));
  }

  @Test
  void aLineThatStartsLikeBzip2IsNotHeldBack() throws SourceReadException {
    try (RecordCursor cursor =
        decoder(config("[]", TEXT_LINE))
            .open(() -> oneChunkThenBlocks("BZh\n"), DecodeLimits.UNLIMITED)) {
      assertEquals(
          new Ok(raw("BZh"), 1, new RawSpan(0, 3)),
          view(assertInstanceOf(DecodeOutcome.Decoded.class, cursor.next())));
    }
  }

  @Test
  void inputShorterThanASignatureIsRead() {
    assertEquals(
        List.of(new Ok(raw("BZ"), 1, new RawSpan(0, 2))),
        decode(config("[]", TEXT_LINE), utf8("BZ")));
  }

  @Test
  void emptyInputGivesNothing() {
    assertEquals(List.of(), decode(config("[]", TEXT_LINE), new byte[0]));
  }

  @Test
  void readsNoMoreThanItNeedsToDecide() throws SourceReadException {
    try (RecordCursor cursor =
        decoder(config("[]", TEXT_LINE))
            .open(() -> oneChunkThenBlocks("a\n"), DecodeLimits.UNLIMITED)) {
      assertEquals(
          new Ok(raw("a"), 1, new RawSpan(0, 1)),
          view(assertInstanceOf(DecodeOutcome.Decoded.class, cursor.next())));
    }
  }

  private static InputStream oneChunkThenBlocks(String chunk) {
    byte[] bytes = utf8(chunk);
    return new InputStream() {
      private boolean served;

      @Override
      public int read() {
        throw new AssertionError("read past the first chunk");
      }

      @Override
      public int read(byte[] b, int off, int len) {
        if (served) {
          throw new AssertionError("read past the first chunk");
        }
        served = true;
        System.arraycopy(bytes, 0, b, off, bytes.length);
        return bytes.length;
      }
    };
  }

  @Test
  void aReadFailureWhileCheckingIsAReadFailure() {
    InputStream failing =
        new InputStream() {
          @Override
          public int read() throws IOException {
            throw new IOException("connection lost");
          }
        };

    assertEquals(
        List.of(new Threw("SourceReadException")),
        decode(decoder(config("[]", TEXT_LINE)), () -> failing, DecodeLimits.UNLIMITED));
  }
}
