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
package io.fleak.zephflow.lib.commands.fssource;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.commands.fssource.api.FileKey;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;

class FsPayloadReaderTest {

  private static final FileKey KEY = new FileKey("file", "file:///tmp/x");

  private static FileReader readerOf(byte[] bytes) {
    return (key, offset) -> new ByteArrayInputStream(bytes);
  }

  private static byte[] gzip(String text) throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(out)) {
      gzipOutputStream.write(text.getBytes(StandardCharsets.UTF_8));
    }
    return out.toByteArray();
  }

  @Test
  void readWholeReturnsPlainBytesUnchanged() throws Exception {
    FsPayloadReader payloadReader =
        new FsPayloadReader(readerOf("hello".getBytes(StandardCharsets.UTF_8)), 1024, 8);

    assertEquals("hello", new String(payloadReader.readWhole(KEY), StandardCharsets.UTF_8));
  }

  @Test
  void readWholeTransparentlyGunzips() throws Exception {
    FsPayloadReader payloadReader = new FsPayloadReader(readerOf(gzip("hello")), 1024, 8);

    assertEquals("hello", new String(payloadReader.readWhole(KEY), StandardCharsets.UTF_8));
  }

  @Test
  void readWholeRejectsAPayloadOverTheCap() {
    FsPayloadReader payloadReader =
        new FsPayloadReader(readerOf("0123456789".getBytes(StandardCharsets.UTF_8)), 4, 8);

    assertThrows(
        FsPayloadReader.PayloadTooLargeException.class, () -> payloadReader.readWhole(KEY));
  }

  @Test
  void readWholeCountsDecompressedBytesAgainstTheCap() throws Exception {
    // 10 KiB of 'a' compresses to well under 100 bytes; the cap must still catch it.
    FsPayloadReader payloadReader = new FsPayloadReader(readerOf(gzip("a".repeat(10240))), 1024, 8);

    assertThrows(
        FsPayloadReader.PayloadTooLargeException.class, () -> payloadReader.readWhole(KEY));
  }

  @Test
  void forEachChunkSplitsOnlyAtNewlines() throws Exception {
    String payload = "aaaa\nbbbb\ncccc\ndddd\n";
    FsPayloadReader payloadReader =
        new FsPayloadReader(readerOf(payload.getBytes(StandardCharsets.UTF_8)), 1024, 6);

    List<String> chunks = new ArrayList<>();
    payloadReader.forEachChunk(KEY, bytes -> chunks.add(new String(bytes, StandardCharsets.UTF_8)));

    assertTrue(chunks.size() > 1, "a 20-byte payload at a 6-byte chunk size must split");
    assertEquals(payload, String.join("", chunks), "no bytes may be lost or duplicated");
    chunks.forEach(
        chunk -> assertTrue(chunk.endsWith("\n"), "every chunk ends on a line boundary: " + chunk));
  }

  @Test
  void forEachChunkEmitsATrailingLineWithoutANewline() throws Exception {
    FsPayloadReader payloadReader =
        new FsPayloadReader(readerOf("aaaa\nbbbb".getBytes(StandardCharsets.UTF_8)), 1024, 6);

    List<String> chunks = new ArrayList<>();
    payloadReader.forEachChunk(KEY, bytes -> chunks.add(new String(bytes, StandardCharsets.UTF_8)));

    assertEquals("aaaa\nbbbb", String.join("", chunks));
  }

  @Test
  void forEachChunkStreamsFarBeyondTheWholeFileCap() throws Exception {
    // 2000 lines, cap of 64 bytes: unbounded in total size, bounded per chunk.
    String payload = "line\n".repeat(2000);
    FsPayloadReader payloadReader =
        new FsPayloadReader(readerOf(payload.getBytes(StandardCharsets.UTF_8)), 64, 16);

    List<String> chunks = new ArrayList<>();
    payloadReader.forEachChunk(KEY, bytes -> chunks.add(new String(bytes, StandardCharsets.UTF_8)));

    assertEquals(payload, String.join("", chunks));
  }

  @Test
  void forEachChunkRejectsASingleLineOverTheCap() {
    FsPayloadReader payloadReader =
        new FsPayloadReader(readerOf("0123456789".getBytes(StandardCharsets.UTF_8)), 4, 2);

    assertThrows(
        FsPayloadReader.PayloadTooLargeException.class,
        () -> payloadReader.forEachChunk(KEY, bytes -> {}),
        "a payload with no newline cannot be chunked and must not buffer without bound");
  }

  @Test
  void forEachChunkStaysLinearForALongNewlineFreeLine() throws Exception {
    // A single line spanning many 8 KiB reads is what exposes the quadratic re-copy: with no
    // newline ever found, the old code redid a full copy-and-rescan of everything buffered so far
    // on EVERY read. 64 MiB of one line at a 1 MiB chunk size makes the blowup unmistakable:
    // measured empirically at ~37s unfixed (2.5x over this 15s budget) vs well under 1s fixed. A
    // smaller 32 MiB payload measured ~8.7s unfixed, too close to the budget to be unambiguous, so
    // this was sized up until RED was clear - see the fix report for the calibration numbers.
    int payloadSize = 64 * 1024 * 1024;
    byte[] payload = new byte[payloadSize];
    Arrays.fill(payload, (byte) 'a');
    int chunkSizeBytes = 1024 * 1024;
    FsPayloadReader payloadReader =
        new FsPayloadReader(readerOf(payload), payloadSize + 1024L, chunkSizeBytes);

    List<byte[]> chunks = new ArrayList<>();
    assertTimeoutPreemptively(
        Duration.ofSeconds(15),
        () -> payloadReader.forEachChunk(KEY, chunks::add),
        "forEachChunk must do linear, not quadratic, work in payload size");

    assertEquals(1, chunks.size(), "a single newline-free line arrives as one trailing chunk");
    assertArrayEquals(payload, chunks.get(0));
  }

  @Test
  void openClosesTheUnderlyingStreamWhenMagicByteDetectionFails() {
    TrackingFailingStream failingStream = new TrackingFailingStream();
    FileReader reader = (key, offset) -> failingStream;
    FsPayloadReader payloadReader = new FsPayloadReader(reader, 1024, 8);

    // readWhole and forEachChunk both open() the same way; readWhole is enough to exercise it.
    assertThrows(IOException.class, () -> payloadReader.readWhole(KEY));

    assertTrue(
        failingStream.closed,
        "the underlying stream must be closed when detection fails, before the caller's"
            + " try-with-resources takes ownership of it");
  }

  /** A stream that fails every read, to drive the magic-byte detection failure path. */
  private static final class TrackingFailingStream extends InputStream {
    boolean closed = false;

    @Override
    public int read() throws IOException {
      throw new IOException("boom");
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  @Test
  void openClosesTheUnderlyingStreamWhenGzipConstructionFails() {
    // Valid gzip magic bytes followed by a body too short to be a real gzip header: the
    // GZIPInputStream constructor reads past the magic bytes to parse the rest of the header and
    // throws once the stream runs out, which is what a corrupt gzip body looks like.
    byte[] payload = {(byte) 0x1f, (byte) 0x8b, 0x00, 0x00};
    TrackingStream trackingStream = new TrackingStream(payload);
    FileReader reader = (key, offset) -> trackingStream;
    FsPayloadReader payloadReader = new FsPayloadReader(reader, 1024, 8);

    assertThrows(IOException.class, () -> payloadReader.readWhole(KEY));

    assertTrue(
        trackingStream.closed,
        "the underlying stream must be closed when GZIPInputStream construction fails, or a"
            + " pooled connection (S3, for one) leaks on every retry of a corrupt file");
  }

  /** A stream that tracks whether {@code close()} was called, to drive the gzip failure path. */
  private static final class TrackingStream extends ByteArrayInputStream {
    boolean closed = false;

    TrackingStream(byte[] bytes) {
      super(bytes);
    }

    @Override
    public void close() throws IOException {
      closed = true;
      super.close();
    }
  }
}
