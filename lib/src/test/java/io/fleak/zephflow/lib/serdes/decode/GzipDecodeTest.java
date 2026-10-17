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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import org.junit.jupiter.api.Test;

class GzipDecodeTest {

  private static final String GZIP_LINES =
      "{\"compression\":[\"gzip\"],\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"line\"}}}";
  private static final String AUTO_LINES =
      "{\"compression\":\"auto\",\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"line\"}}}";

  /** Expected outcomes for lines "line-0".."line-(n-1)", each followed by a newline. */
  private static List<Object> numberedLines(int n) {
    List<Object> expected = new ArrayList<>();
    long offset = 0;
    for (int i = 0; i < n; i++) {
      String text = "line-" + i;
      expected.add(new Ok(raw(text), i + 1, new RawSpan(offset, text.length())));
      offset += text.length() + 1;
    }
    return expected;
  }

  private static String numberedText(int from, int to) {
    StringBuilder text = new StringBuilder();
    for (int i = from; i < to; i++) {
      text.append("line-").append(i).append('\n');
    }
    return text.toString();
  }

  private static long offsetOfLine(int n) {
    return numberedText(0, n).length();
  }

  private static Bad corrupt(long recordIndex, long offset, String message) {
    return new Bad(
        ErrorKind.CORRUPT_COMPRESSION,
        "",
        recordIndex,
        RawSpan.toEnd(offset),
        "CorruptCompressionException: " + message,
        true);
  }

  @Test
  void readsEveryMemberEvenWhenTheStreamReportsNothingAvailable() {
    byte[] input = gzip(numberedText(0, 2), numberedText(2, 3), numberedText(3, 4));
    assertEquals(
        numberedLines(4), decode(decoder(GZIP_LINES), trickle(input, 3), DecodeLimits.UNLIMITED));
  }

  @Test
  void autoDetectsGzipAndPassesPlainInputThrough() {
    assertEquals(numberedLines(2), decode(AUTO_LINES, gzip(numberedText(0, 2))));
    assertEquals(numberedLines(2), decode(AUTO_LINES, utf8(numberedText(0, 2))));
    assertEquals(List.of(), decode(AUTO_LINES, new byte[0]));
  }

  @Test
  void emptyInputWithGzipConfiguredHasNoOutcomes() {
    assertEquals(List.of(), decode(GZIP_LINES, new byte[0]));
  }

  @Test
  void plainInputWithGzipConfiguredIsCorrupt() {
    assertEquals(
        List.of(corrupt(1, 0, "input is not in gzip format")), decode(GZIP_LINES, utf8("a\nb\n")));
  }

  @Test
  void garbageAfterTheLastMemberKeepsEveryEarlierRecord() {
    byte[] input = concat(gzip(numberedText(0, 2), numberedText(2, 3)), utf8("GARBAGE"));
    List<Object> expected = new ArrayList<>(numberedLines(3));
    expected.add(corrupt(4, offsetOfLine(3), "unexpected bytes after gzip member 2"));
    assertEquals(expected, decode(GZIP_LINES, input));
    assertEquals(expected, decode(decoder(GZIP_LINES), trickle(input, 2), DecodeLimits.UNLIMITED));
  }

  @Test
  void garbageBetweenMembersStopsThereAndKeepsEarlierRecords() {
    byte[] input = concat(gzip(numberedText(0, 2)), utf8("XX"), gzip(numberedText(2, 4)));
    List<Object> expected = new ArrayList<>(numberedLines(2));
    expected.add(corrupt(3, offsetOfLine(2), "unexpected bytes after gzip member 1"));
    assertEquals(expected, decode(GZIP_LINES, input));
  }

  @Test
  void truncatedTrailerKeepsEveryCompleteLine() {
    byte[] whole = gzip(numberedText(0, 3));
    byte[] input = Arrays.copyOf(whole, whole.length - 3);
    List<Object> expected = new ArrayList<>(numberedLines(3));
    expected.add(corrupt(4, offsetOfLine(3), "gzip member 1 is truncated"));
    assertEquals(expected, decode(GZIP_LINES, input));
  }

  @Test
  void truncatedBodyKeepsEveryLineTheInflaterReleased() {
    byte[] whole = gzip(numberedText(0, 2000));
    byte[] cut = Arrays.copyOf(whole, whole.length / 2);
    int released = completeLines(recoverablePrefix(cut));
    List<Object> expected = new ArrayList<>(numberedLines(released));
    expected.add(corrupt(released + 1, offsetOfLine(released), "gzip member 1 is truncated"));
    assertEquals(expected, decode(GZIP_LINES, cut));
    assertTrue(released > 500, "most lines before the cut must come out, got " + released);
  }

  @Test
  void corruptDeflateDataMidMemberKeepsEverythingDecodedBeforeTheDamage() {
    byte[] input = gzip(numberedText(0, 50_000));
    for (int flipAt : new int[] {207, input.length / 2}) {
      byte[] damaged = input.clone();
      damaged[flipAt] ^= (byte) 0xFF;
      // Damage may decode into wrong-but-valid bytes before zlib notices, so the expected lines
      // are whatever a byte-at-a-time inflater releases, not the original text.
      String released = new String(recoverablePrefix(damaged), StandardCharsets.UTF_8);
      List<Object> expected = new ArrayList<>();
      long offset = 0;
      int lineNumber = 0;
      int newline;
      while ((newline = released.indexOf('\n', (int) offset)) >= 0) {
        String line = released.substring((int) offset, newline);
        expected.add(new Ok(raw(line), ++lineNumber, new RawSpan(offset, line.length())));
        offset = newline + 1;
      }
      List<Object> outcomes = decode(GZIP_LINES, damaged);
      Bad failure = (Bad) outcomes.get(outcomes.size() - 1);
      expected.add(
          corrupt(
              lineNumber + 1,
              offset,
              failure.error().substring("CorruptCompressionException: ".length())));
      assertEquals(expected, outcomes, "flip at " + flipAt);

      assertTrue(lineNumber > 40, "lines before the damage must come out, got " + lineNumber);
    }
  }

  /** Inflates one byte at a time to find exactly how much a perfect reader could recover. */
  private static byte[] recoverablePrefix(byte[] gzip) {
    if (gzip.length <= 10) {
      return new byte[0];
    }
    Inflater inflater = new Inflater(true);
    inflater.setInput(gzip, 10, gzip.length - 10);
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    byte[] one = new byte[1];
    try {
      while (!inflater.finished() && inflater.inflate(one) == 1) {
        out.write(one[0]);
      }
    } catch (DataFormatException expected) {
      // The damaged spot.
    } finally {
      inflater.end();
    }
    return out.toByteArray();
  }

  @Test
  void headerWithEveryOptionalFieldDecodes() throws Exception {
    ByteArrayOutputStream member = new ByteArrayOutputStream();
    member.write(new byte[] {0x1f, (byte) 0x8b, 8, 0x02 | 0x04 | 0x08 | 0x10, 0, 0, 0, 0, 0, 3});
    byte[] extra = new byte[65_535];
    member.write(new byte[] {(byte) 0xFF, (byte) 0xFF});
    member.write(extra);
    member.write(utf8("name.txt\0comment\0"));
    member.write(new byte[] {0x12, 0x34});
    byte[] text = utf8("a\nb\n");
    Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION, true);
    deflater.setInput(text);
    deflater.finish();
    byte[] body = new byte[256];
    member.write(body, 0, deflater.deflate(body));
    deflater.end();
    CRC32 crc = new CRC32();
    crc.update(text);
    ByteBuffer trailer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
    trailer.putInt((int) crc.getValue()).putInt(text.length);
    member.write(trailer.array());
    assertEquals(
        List.of(new Ok(raw("a"), 1, new RawSpan(0, 1)), new Ok(raw("b"), 2, new RawSpan(2, 1))),
        decode(GZIP_LINES, member.toByteArray()));
  }

  @Test
  void corruptCrcIsReportedAfterTheMembersLines() {
    byte[] input = gzip("a\n");
    input[input.length - 8] ^= 1;
    assertEquals(
        List.of(
            new Ok(raw("a"), 1, new RawSpan(0, 1)), corrupt(2, 2, "gzip member 1 CRC mismatch")),
        decode(GZIP_LINES, input));
  }

  @Test
  void unterminatedFileNameStopsReadingAtTheHeaderLimit() {
    byte[] header = {0x1f, (byte) 0x8b, 8, 0x08, 0, 0, 0, 0, 0, 0};
    byte[] name = new byte[4_000_000];
    Arrays.fill(name, (byte) 'n');
    AtomicLong read = new AtomicLong();
    byte[] input = concat(header, name);
    ByteInput counting =
        () ->
            new InputStream() {
              private final ByteArrayInputStream in = new ByteArrayInputStream(input);

              @Override
              public int read() {
                int b = in.read();
                if (b >= 0) {
                  read.incrementAndGet();
                }
                return b;
              }

              @Override
              public int read(byte[] b, int off, int len) {
                int n = in.read(b, off, len);
                if (n > 0) {
                  read.addAndGet(n);
                }
                return n;
              }
            };
    assertEquals(
        List.of(corrupt(1, 0, "gzip header is longer than 1048576 bytes")),
        decode(decoder(GZIP_LINES), counting, DecodeLimits.UNLIMITED));
    assertTrue(read.get() < 1_200_000, "read " + read.get() + " bytes of a 4 MB header");
  }

  @Test
  void networkFailureAnywhereIsAReadFailureAfterTheRecordsBeforeIt() {
    byte[] first = gzip(numberedText(0, 3000));
    byte[] input = concat(first, gzip(numberedText(3000, 3002)));
    for (int cut : new int[] {5, first.length / 2, first.length - 2}) {
      int released = completeLines(recoverablePrefix(Arrays.copyOf(input, cut)));
      assertReadFailureAfter(released, input, cut);
    }
    assertReadFailureAfter(3000, input, first.length);
    assertReadFailureAfter(3000, input, first.length + 4);
    assertReadFailureAfter(3002, input, input.length - 1);
  }

  private static void assertReadFailureAfter(int lines, byte[] input, int cut) {
    List<Object> expected = new ArrayList<>(numberedLines(lines));
    expected.add(new Threw("SourceReadException"));
    assertEquals(
        expected,
        decode(decoder(GZIP_LINES), failingAt(input, cut), DecodeLimits.UNLIMITED),
        "cut at " + cut);
  }

  private static int completeLines(byte[] text) {
    int lines = 0;
    for (byte b : text) {
      if (b == '\n') {
        lines++;
      }
    }
    return lines;
  }
}
