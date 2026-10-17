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

import java.util.List;
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
                ErrorKind.MALFORMED, "not json", 3, new RawSpan(9, 8), "JsonParseException", false),
            new Bad(
                ErrorKind.MALFORMED,
                "[1,2]",
                4,
                new RawSpan(19, 5),
                "IllegalArgumentException",
                false),
            new Bad(
                ErrorKind.MALFORMED,
                "{\"b\":2} trailing",
                5,
                new RawSpan(25, 16),
                "JsonParseException",
                false),
            new Bad(
                ErrorKind.MALFORMED,
                "{\"c\":3}{\"d\":4}",
                6,
                new RawSpan(42, 14),
                "IllegalArgumentException",
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
  void lineAtLimitDecodesAndOneByteMoreIsOversizedThenNextLineStillDecodes() {
    String atLimit = "{\"a\":\"" + "x".repeat(10) + "\"}";
    String overLimit = "{\"a\":\"" + "x".repeat(11) + "\"}";
    DecodeLimits limits = new DecodeLimits(atLimit.length(), 0);
    String input = atLimit + "\n" + overLimit + "\n{\"b\":1}\n";
    assertEquals(
        List.of(
            new Ok(json(atLimit), 1, new RawSpan(0, 18)),
            new Big(2, new RawSpan(19, 19), 19, 19, false),
            new Ok(json("{\"b\":1}"), 3, new RawSpan(39, 7))),
        decode(decoder(LINES), ByteInput.ofBytes(utf8(input)), limits));
  }

  @Test
  void oversizedLineKeepsOnlyAPreview() {
    String huge = "x".repeat(100_000);
    DecodeLimits limits = new DecodeLimits(1000, 0);
    assertEquals(
        List.of(
            new Big(1, new RawSpan(0, 100_000), 100_000, 4096, false),
            new Ok(json("{}"), 2, new RawSpan(100_001, 2))),
        decode(decoder(LINES), ByteInput.ofBytes(utf8(huge + "\n{}")), limits));
  }

  @Test
  void readFailureMidInputThrowsAfterEarlierLines() {
    byte[] input = utf8("{\"a\":1}\n{\"b\":2}\n{\"c\":3}\n");
    assertEquals(
        List.of(new Ok(json("{\"a\":1}"), 1, new RawSpan(0, 7)), new Threw("SourceReadException")),
        decode(decoder(LINES), failingAt(input, 12), DecodeLimits.UNLIMITED));
  }
}
