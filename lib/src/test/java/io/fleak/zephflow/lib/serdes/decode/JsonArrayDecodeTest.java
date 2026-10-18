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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonArrayDecodeTest {

  private static final String ARRAY = "{\"format\":{\"type\":\"json\",\"mode\":\"array\"}}";

  @Test
  void decodesElementsWithExactSpansAndReportsNonObjects() {
    String input = "[{\"a\":1}, 7, {\"b\":[1,2]}]";
    assertEquals(
        List.of(
            new Ok(json("{\"a\":1}"), 1, new RawSpan(1, 7)),
            new Bad(
                ErrorKind.MALFORMED,
                "7",
                2,
                new RawSpan(10, 1),
                "IllegalArgumentException: expected a JSON object but found NUMBER",
                false),
            new Ok(json("{\"b\":[1,2]}"), 3, new RawSpan(13, 11))),
        decode(ARRAY, utf8(input)));
  }

  @Test
  void emptyInputAndEmptyArrayHaveNoOutcomes() {
    assertEquals(List.of(), decode(ARRAY, new byte[0]));
    assertEquals(List.of(), decode(ARRAY, utf8(" [ ] ")));
  }

  @Test
  void whitespaceOnlyInputIsMalformed() {
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                "",
                1,
                RawSpan.toEnd(0),
                "IllegalArgumentException: no JSON array in the input",
                true)),
        decode(ARRAY, utf8(" \n")));
  }

  @Test
  void rootThatIsNotAnArrayIsTerminal() {
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                "",
                1,
                RawSpan.toEnd(0),
                "IllegalArgumentException: expected a JSON array but found START_OBJECT",
                true)),
        decode(ARRAY, utf8("{\"records\":[]}")));
  }

  @Test
  void syntaxErrorEndsTheCursorAfterEarlierElements() {
    assertEquals(
        List.of(
            new Ok(json("{\"a\":1}"), 1, new RawSpan(1, 7)),
            new Bad(
                ErrorKind.MALFORMED,
                "",
                2,
                RawSpan.toEnd(9),
                "JsonParseException: Unexpected character (']' (code 93)): expected a valid value"
                    + " (JSON String, Number, Array, Object or token 'null', 'true' or 'false')",
                true)),
        decode(ARRAY, utf8("[{\"a\":1},{\"b\":]")));
  }

  @Test
  void truncatedArrayAndTrailingContentAreTerminal() {
    assertEquals(
        List.of(
            new Ok(json("{\"a\":1}"), 1, new RawSpan(1, 7)),
            new Bad(
                ErrorKind.MALFORMED,
                "",
                2,
                RawSpan.toEnd(8),
                "JsonEOFException: Unexpected end-of-input: expected close marker for Array (start"
                    + " marker at [Source: REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION`"
                    + " disabled); line: 1, column: 1])",
                true)),
        decode(ARRAY, utf8("[{\"a\":1}")));
    assertEquals(
        List.of(
            new Ok(json("{\"a\":1}"), 1, new RawSpan(1, 7)),
            new Bad(
                ErrorKind.MALFORMED,
                "",
                2,
                RawSpan.toEnd(8),
                "IllegalArgumentException: unexpected content after the JSON array",
                true)),
        decode(ARRAY, utf8("[{\"a\":1}] {}")));
  }

  @Test
  void utf16AndUtf32InputAreTranscoded() {
    List<Object> expected =
        List.of(
            new Ok(json("{\"a\":1}"), 1, new RawSpan(1, 7)),
            new Ok(json("{\"b\":\"é\"}"), 2, new RawSpan(9, 10)));
    String input = "[{\"a\":1},{\"b\":\"é\"}]";
    assertEquals(expected, decode(ARRAY, input.getBytes(StandardCharsets.UTF_16LE)));
    assertEquals(expected, decode(ARRAY, input.getBytes(StandardCharsets.UTF_16BE)));
    assertEquals(expected, decode(ARRAY, input.getBytes(Charset.forName("UTF-32LE"))));
  }

  @Test
  void utf16BigEndianWithBomAndUtf32BigEndianWithoutBom() {
    List<Object> expected =
        List.of(
            new Ok(json("{\"a\":1}"), 1, new RawSpan(1, 7)),
            new Ok(json("{\"b\":2}"), 2, new RawSpan(9, 7)));
    String input = "[{\"a\":1},{\"b\":2}]";
    assertEquals(
        expected,
        decode(
            ARRAY,
            concat(
                new byte[] {(byte) 0xFE, (byte) 0xFF}, input.getBytes(StandardCharsets.UTF_16BE))));
    assertEquals(expected, decode(ARRAY, input.getBytes(Charset.forName("UTF-32BE"))));
  }

  @Test
  void cutGzipKeepsEveryElementTheInflaterReleased() {
    StringBuilder text = new StringBuilder("[");
    for (int i = 0; i < 3000; i++) {
      text.append(i == 0 ? "" : ",").append("{\"i\":").append(i).append('}');
    }
    byte[] whole = gzip(text.append(']').toString());
    byte[] cut = Arrays.copyOf(whole, whole.length / 2);
    String released = new String(recoverablePrefix(cut), StandardCharsets.UTF_8);
    int decoded = (int) released.chars().filter(c -> c == '}').count();
    List<Object> expected = new ArrayList<>();
    long offset = 1;
    for (int i = 0; i < decoded; i++) {
      String element = "{\"i\":" + i + "}";
      expected.add(new Ok(json(element), i + 1, new RawSpan(offset, element.length())));
      offset += element.length() + 1;
    }
    expected.add(
        new Bad(
            ErrorKind.CORRUPT_COMPRESSION,
            "",
            decoded + 1,
            RawSpan.toEnd(offset),
            "CorruptCompressionException: gzip member 1 is truncated",
            true));
    assertEquals(
        expected,
        decode(
            "{\"compression\":\"gzip\",\"format\":{\"type\":\"json\",\"mode\":\"array\"}}", cut));
    assertTrue(decoded > 1000, "elements before the cut must come out, got " + decoded);
  }

  @Test
  void elementOverTheBudgetIsOversizedAndTerminal() {
    String big = "{\"a\":\"" + "x".repeat(200_000) + "\"}";
    List<Object> outcomes =
        decode(
            decoder(ARRAY),
            ByteInput.ofBytes(utf8("[{\"a\":1}," + big + ",{\"b\":2}]")),
            new DecodeLimits(1000, 0));
    assertEquals(2, outcomes.size());
    assertEquals(new Ok(json("{\"a\":1}"), 1, new RawSpan(1, 7)), outcomes.get(0));
    Big oversized = (Big) outcomes.get(1);
    assertEquals(
        new Big(2, RawSpan.toEnd(9), oversized.sizeSoFar(), big.substring(0, 4096), true),
        oversized);
  }

  @Test
  void hugeLimitsDoNotOverflow() {
    assertEquals(
        List.of(new Ok(json("{\"a\":1}"), 1, new RawSpan(1, 7))),
        decode(
            decoder(ARRAY),
            ByteInput.ofBytes(utf8("[{\"a\":1}]")),
            new DecodeLimits(Long.MAX_VALUE, Long.MAX_VALUE)));
  }

  @Test
  void readFailureMidArrayKeepsEarlierElements() {
    byte[] input = utf8("[{\"a\":1},{\"b\":2},{\"c\":3}]");
    assertEquals(
        List.of(new Ok(json("{\"a\":1}"), 1, new RawSpan(1, 7)), new Threw("SourceReadException")),
        decode(decoder(ARRAY), failingAt(input, 12), DecodeLimits.UNLIMITED));
  }
}
