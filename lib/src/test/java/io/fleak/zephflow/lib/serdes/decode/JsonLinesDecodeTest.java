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
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;

import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData.NumberType;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class JsonLinesDecodeTest {

  private static final String LINES = "{\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}";

  @Test
  void decodesEachLineAndReportsBadLinesWithoutStopping() {
    String input =
        "{\"a\":1}\n"
            + "\n"
            + "not json\r\n"
            + "[1,2]\r"
            + "{\"b\":2} trailing\n"
            + "{\"c\":3}{\"d\":4}\n"
            + "  \t\n"
            + "{\"e\":5}";
    assertEquals(
        List.of(
            new Ok(json("{\"a\":1}"), 1, new RawSpan(0, 7)),
            new Bad(
                ErrorKind.MALFORMED,
                "not json",
                3,
                new RawSpan(9, 8),
                "JsonParseException: Unrecognized token 'not': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')",
                false),
            new Bad(
                ErrorKind.MALFORMED,
                "[1,2]",
                4,
                new RawSpan(19, 5),
                "IllegalArgumentException: expected a JSON object, got a JSON array",
                false),
            new Bad(
                ErrorKind.MALFORMED,
                "{\"b\":2} trailing",
                5,
                new RawSpan(25, 16),
                "JsonParseException: Unrecognized token 'trailing': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')",
                false),
            new Bad(
                ErrorKind.MALFORMED,
                "{\"c\":3}{\"d\":4}",
                6,
                new RawSpan(42, 14),
                "IllegalArgumentException: unexpected content after the JSON value",
                false),
            new Ok(json("{\"e\":5}"), 8, new RawSpan(61, 7))),
        decode(LINES, utf8(input)));
  }

  @Test
  void emptyInputHasNoOutcomes() {
    assertEquals(List.of(), decode(LINES, new byte[0]));
  }

  @Test
  void bomIsSkippedButCountedInOffsets() {
    byte[] input = concat(new byte[] {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF}, utf8("{\"a\":1}\n"));
    assertEquals(List.of(new Ok(json("{\"a\":1}"), 1, new RawSpan(3, 7))), decode(LINES, input));
  }

  @Test
  void utf16InputIsTranscodedBeforeFraming() {
    byte[] input = "{\"a\":1}\n{\"b\":\"é\"}\n".getBytes(StandardCharsets.UTF_16LE);
    assertEquals(
        List.of(
            new Ok(json("{\"a\":1}"), 1, new RawSpan(0, 7)),
            new Ok(json("{\"b\":\"é\"}"), 2, new RawSpan(8, 10))),
        decode(LINES, input));
  }

  private static String numberedJsonLines(int n) {
    StringBuilder text = new StringBuilder();
    for (int i = 0; i < n; i++) {
      text.append("{\"i\":").append(i).append("}\n");
    }
    return text.toString();
  }

  private static List<Object> numberedJsonOutcomes(int n) {
    List<Object> expected = new ArrayList<>();
    long offset = 0;
    for (int i = 0; i < n; i++) {
      String line = "{\"i\":" + i + "}";
      expected.add(new Ok(json(line), i + 1, new RawSpan(offset, line.length())));
      offset += line.length() + 1;
    }
    return expected;
  }

  @Test
  void utf16ReadFailureKeepsEveryLineBeforeTheCut() {
    String text = numberedJsonLines(20_000);
    byte[] input = text.getBytes(StandardCharsets.UTF_16LE);
    int cut = input.length / 2;
    int before = (int) text.substring(0, cut / 2).chars().filter(c -> c == '\n').count();
    List<Object> expected = new ArrayList<>(numberedJsonOutcomes(before));
    expected.add(new Threw("SourceReadException"));
    assertEquals(expected, decode(decoder(LINES), failingAt(input, cut), DecodeLimits.UNLIMITED));
  }

  @Test
  void utf16GzipWithACutTrailerKeepsEveryLine() {
    String text = numberedJsonLines(20_000);
    byte[] whole = gzipOne(text.getBytes(StandardCharsets.UTF_16LE));
    byte[] cut = Arrays.copyOf(whole, whole.length - 8);
    List<Object> expected = new ArrayList<>(numberedJsonOutcomes(20_000));
    expected.add(
        new Bad(
            ErrorKind.CORRUPT_COMPRESSION,
            "",
            20_001,
            RawSpan.toEnd(text.length()),
            "CorruptCompressionException: gzip member 1 is truncated",
            true));
    assertEquals(
        expected,
        decode(
            "{\"compression\":\"gzip\",\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}", cut));
  }

  @Test
  void utf16LittleEndianWithBomStartsOffsetsAtZero() {
    byte[] input =
        concat(
            new byte[] {(byte) 0xFF, (byte) 0xFE},
            "{\"a\":1}\n{\"b\":2}\n".getBytes(StandardCharsets.UTF_16LE));
    assertEquals(
        List.of(
            new Ok(json("{\"a\":1}"), 1, new RawSpan(0, 7)),
            new Ok(json("{\"b\":2}"), 2, new RawSpan(8, 7))),
        decode(LINES, input));
  }

  @Test
  void loneUtf16SurrogateReplacesOnlyItsOwnUnit() {
    byte[] input =
        concat(
            "{\"s\":\"".getBytes(StandardCharsets.UTF_16LE),
            new byte[] {0x00, (byte) 0xD8},
            "x\"}\n{\"t\":1}\n".getBytes(StandardCharsets.UTF_16LE));
    assertEquals(
        List.of(
            new Ok(json("{\"s\":\"\uFFFDx\"}"), 1, new RawSpan(0, 12)),
            new Ok(json("{\"t\":1}"), 2, new RawSpan(13, 7))),
        decode(LINES, input));
  }

  @Test
  void invalidUtf32CodePointsBecomeOneReplacementEachAndLaterLinesSurvive() {
    Charset utf32 = Charset.forName("UTF-32BE");
    for (int bad : new int[] {0xD800, 0xDC00, 0x110000}) {
      StringBuilder tail = new StringBuilder();
      for (int i = 0; i < 5000; i++) {
        tail.append("{\"x\":").append(i).append("}\n");
      }
      byte[] input =
          concat(
              "{\"s\":\"a".getBytes(utf32),
              new byte[] {(byte) (bad >>> 24), (byte) (bad >>> 16), (byte) (bad >>> 8), (byte) bad},
              ("b\"}\n" + tail).getBytes(utf32));
      List<Object> expected = new ArrayList<>();
      expected.add(new Ok(json("{\"s\":\"a\uFFFDb\"}"), 1, new RawSpan(0, 13)));
      long offset = 14;
      for (int i = 0; i < 5000; i++) {
        String line = "{\"x\":" + i + "}";
        expected.add(new Ok(json(line), i + 2, new RawSpan(offset, line.length())));
        offset += line.length() + 1;
      }
      List<Object> actual =
          assertTimeoutPreemptively(Duration.ofSeconds(20), () -> decode(LINES, input));
      assertEquals(expected, actual, "bad code point " + Integer.toHexString(bad));
    }
  }

  @Test
  void utf32SurrogatePairWrittenAsTwoCodePointsIsTwoReplacements() {
    Charset utf32 = Charset.forName("UTF-32BE");
    byte[] input =
        concat(
            "{\"s\":\"a".getBytes(utf32),
            new byte[] {0, 0, (byte) 0xD8, 0, 0, 0, (byte) 0xDC, 0},
            "b\"}".getBytes(utf32));
    assertEquals(
        List.of(new Ok(json("{\"s\":\"a\uFFFD\uFFFDb\"}"), 1, new RawSpan(0, 16))),
        decode(LINES, input));
  }

  @Test
  void strayNulAtTheStartOfUtf8IsNotTakenForUtf16() {
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                "x\0",
                1,
                new RawSpan(0, 2),
                "JsonParseException: Unrecognized token 'x': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')",
                false),
            new Ok(json("{\"a\":1}"), 2, new RawSpan(3, 7))),
        decode(LINES, utf8("x\0\n{\"a\":1}\n")));
  }

  @Test
  void shortFirstRecordComesOutBeforeAGzipFailure() {
    byte[] whole = gzip("{}\n");
    byte[] cut = Arrays.copyOf(whole, whole.length - 3);
    assertEquals(
        List.of(
            new Ok(json("{}"), 1, new RawSpan(0, 2)),
            new Bad(
                ErrorKind.CORRUPT_COMPRESSION,
                "",
                2,
                RawSpan.toEnd(3),
                "CorruptCompressionException: gzip member 1 is truncated",
                true)),
        decode(
            "{\"compression\":\"gzip\",\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}", cut));
  }

  @Test
  void lineAtLimitDecodesAndOneByteMoreIsOversizedThenNextLineStillDecodes() {
    String atLimit = "{\"a\":\"" + "x".repeat(10) + "\"}";
    String overLimit = "{\"a\":\"" + "x".repeat(11) + "\"}";
    DecodeLimits limits = new DecodeLimits(atLimit.length(), 0);
    String input = atLimit + "\n" + overLimit + "\n{\"b\":1}\n";
    assertEquals(
        List.of(
            new Ok(json(atLimit), 1, new RawSpan(0, 18)),
            new Big(
                2,
                1,
                new RawSpan(19, 19),
                LimitKind.MAX_RECORD_BYTES,
                atLimit.length(),
                19,
                overLimit,
                false),
            new Ok(json("{\"b\":1}"), 3, new RawSpan(39, 7))),
        decode(decoder(LINES), ByteInput.ofBytes(utf8(input)), limits));
  }

  @Test
  void oversizedLineKeepsOnlyTheFirstBytesAsPreview() {
    String huge = "0123456789".repeat(10_000);
    DecodeLimits limits = new DecodeLimits(1000, 0);
    assertEquals(
        List.of(
            new Big(
                1,
                1,
                new RawSpan(0, 100_000),
                LimitKind.MAX_RECORD_BYTES,
                1000,
                100_000,
                huge.substring(0, 4096),
                false),
            new Ok(json("{}"), 2, new RawSpan(100_001, 2))),
        decode(decoder(LINES), ByteInput.ofBytes(utf8(huge + "\n{}")), limits));
  }

  @Test
  void jacksonLengthLimitsDoNotApply() {
    String longString = "{\"s\":\"" + "x".repeat(20_000_001) + "\"}";
    String longName = "{\"" + "n".repeat(50_001) + "\":1}";
    List<Object> outcomes = decode(LINES, utf8(longString + "\n" + longName + "\n"));
    assertEquals(
        List.of(
            new Ok(
                new RecordFleakData(
                    Map.of("s", new StringPrimitiveFleakData("x".repeat(20_000_001)))),
                1,
                new RawSpan(0, longString.length())),
            new Ok(
                new RecordFleakData(
                    Map.of("n".repeat(50_001), new NumberPrimitiveFleakData(1, NumberType.LONG))),
                2,
                new RawSpan(longString.length() + 1, longName.length()))),
        outcomes);
  }

  @Test
  void nestingDeeperThanJacksonAllowsIsMalformed() {
    String deep = "{\"a\":" + "[".repeat(1001) + "]".repeat(1001) + "}";
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                deep,
                1,
                new RawSpan(0, deep.length()),
                "StreamConstraintsException: Document nesting depth (1001) exceeds the maximum allowed"
                    + " (1000, from `StreamReadConstraints.getMaxNestingDepth()`)",
                false)),
        decode(LINES, utf8(deep)));
  }

  @Test
  void readFailureMidInputThrowsAfterEarlierLines() {
    byte[] input = utf8("{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n");
    assertEquals(
        List.of(new Ok(json("{\"a\":1}"), 1, new RawSpan(0, 7)), new Threw("SourceReadException")),
        decode(decoder(LINES), failingAt(input, 12), DecodeLimits.UNLIMITED));
  }
}
