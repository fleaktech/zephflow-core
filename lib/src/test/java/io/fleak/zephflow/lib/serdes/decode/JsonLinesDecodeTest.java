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

import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData;
import io.fleak.zephflow.api.structure.NumberPrimitiveFleakData.NumberType;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import java.nio.charset.StandardCharsets;
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
                "IllegalArgumentException: expected a JSON object but found ARRAY",
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

  @Test
  void lineAtLimitDecodesAndOneByteMoreIsOversizedThenNextLineStillDecodes() {
    String atLimit = "{\"a\":\"" + "x".repeat(10) + "\"}";
    String overLimit = "{\"a\":\"" + "x".repeat(11) + "\"}";
    DecodeLimits limits = new DecodeLimits(atLimit.length(), 0);
    String input = atLimit + "\n" + overLimit + "\n{\"b\":1}\n";
    assertEquals(
        List.of(
            new Ok(json(atLimit), 1, new RawSpan(0, 18)),
            new Big(2, new RawSpan(19, 19), 19, overLimit, false),
            new Ok(json("{\"b\":1}"), 3, new RawSpan(39, 7))),
        decode(decoder(LINES), ByteInput.ofBytes(utf8(input)), limits));
  }

  @Test
  void oversizedLineKeepsOnlyTheFirstBytesAsPreview() {
    String huge = "0123456789".repeat(10_000);
    DecodeLimits limits = new DecodeLimits(1000, 0);
    assertEquals(
        List.of(
            new Big(1, new RawSpan(0, 100_000), 100_000, huge.substring(0, 4096), false),
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
