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

import java.nio.charset.StandardCharsets;
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
                ErrorKind.MALFORMED, "7", 2, new RawSpan(10, 1), "IllegalArgumentException", false),
            new Ok(json("{\"b\":[1,2]}"), 3, new RawSpan(13, 11))),
        decode(ARRAY, utf8(input)));
  }

  @Test
  void emptyInputAndEmptyArray() {
    assertEquals(List.of(), decode(ARRAY, new byte[0]));
    assertEquals(List.of(), decode(ARRAY, utf8(" [ ] ")));
  }

  @Test
  void rootThatIsNotAnArrayIsTerminal() {
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED, "", 1, RawSpan.toEnd(0), "IllegalArgumentException", true)),
        decode(ARRAY, utf8("{\"records\":[]}")));
  }

  @Test
  void syntaxErrorEndsTheCursorAfterEarlierElements() {
    String input = "[{\"a\":1},{\"b\":]";
    assertEquals(
        List.of(
            new Ok(json("{\"a\":1}"), 1, new RawSpan(1, 7)),
            new Bad(ErrorKind.MALFORMED, "", 2, RawSpan.toEnd(9), "JsonParseException", true)),
        decode(ARRAY, utf8(input)));
  }

  @Test
  void truncatedArrayAndTrailingContentAreTerminal() {
    assertEquals(
        List.of(
            new Ok(json("{\"a\":1}"), 1, new RawSpan(1, 7)),
            new Bad(ErrorKind.MALFORMED, "", 2, RawSpan.toEnd(8), "JsonEOFException", true)),
        decode(ARRAY, utf8("[{\"a\":1}")));
    assertEquals(
        List.of(
            new Ok(json("{\"a\":1}"), 1, new RawSpan(1, 7)),
            new Bad(
                ErrorKind.MALFORMED, "", 2, RawSpan.toEnd(8), "IllegalArgumentException", true)),
        decode(ARRAY, utf8("[{\"a\":1}] {}")));
  }

  @Test
  void nonUtf8JsonIsRejected() {
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED, "", 1, RawSpan.toEnd(0), "IllegalArgumentException", true)),
        decode(ARRAY, "[{\"a\":1}]".getBytes(StandardCharsets.UTF_16LE)));
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
    assertEquals(new Big(2, RawSpan.toEnd(9), oversized.sizeSoFar(), 4096, true), oversized);
  }

  @Test
  void readFailureIsNotReportedAsMalformedEvenWhenJacksonWrapsIt() {
    byte[] input = utf8("[{\"a\":1},{\"b\":2},{\"c\":3}]");
    assertEquals(
        List.of(new Ok(json("{\"a\":1}"), 1, new RawSpan(1, 7)), new Threw("SourceReadException")),
        decode(decoder(ARRAY), failingAt(input, 12), DecodeLimits.UNLIMITED));
  }
}
