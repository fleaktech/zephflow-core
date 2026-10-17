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

class TextDecodeTest {

  private static final String LINE =
      "{\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"line\"}}}";
  private static final String LINE_KEEP_BLANK =
      "{\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"line\",\"skipBlank\":false}}}";
  private static final String WHOLE =
      "{\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"whole\"}}}";

  @Test
  void splitsOnLfCrlfAndCrAndCountsPhysicalLines() {
    String input = "a\r\nb\rc\n\nd";
    assertEquals(
        List.of(
            new Ok(raw("a"), 1, new RawSpan(0, 1)),
            new Ok(raw("b"), 2, new RawSpan(3, 1)),
            new Ok(raw("c"), 3, new RawSpan(5, 1)),
            new Ok(raw("d"), 5, new RawSpan(8, 1))),
        decode(LINE, utf8(input)));
    assertEquals(
        List.of(
            new Ok(raw("a"), 1, new RawSpan(0, 1)),
            new Ok(raw("b"), 2, new RawSpan(3, 1)),
            new Ok(raw("c"), 3, new RawSpan(5, 1)),
            new Ok(raw(""), 4, new RawSpan(7, 0)),
            new Ok(raw("d"), 5, new RawSpan(8, 1))),
        decode(LINE_KEEP_BLANK, utf8(input)));
  }

  @Test
  void crlfSplitAcrossReadsIsOneLineEnding() {
    assertEquals(
        List.of(new Ok(raw("ab"), 1, new RawSpan(0, 2)), new Ok(raw("cd"), 2, new RawSpan(4, 2))),
        decode(decoder(LINE), trickle(utf8("ab\r\ncd\r\n"), 1), DecodeLimits.UNLIMITED));
  }

  @Test
  void invalidUtf8IsReplacedInTheText() {
    byte[] input = {'a', (byte) 0xFF, 'b'};
    assertEquals(List.of(new Ok(raw("a\uFFFDb"), 1, new RawSpan(0, 3))), decode(LINE, input));
  }

  @Test
  void lineLimitIsExact() {
    DecodeLimits limits = new DecodeLimits(3, 0);
    assertEquals(
        List.of(
            new Ok(raw("abc"), 1, new RawSpan(0, 3)),
            new Big(2, new RawSpan(4, 4), 4, "abcd", false),
            new Ok(raw("e"), 3, new RawSpan(9, 1))),
        decode(decoder(LINE), ByteInput.ofBytes(utf8("abc\nabcd\ne")), limits));
  }

  @Test
  void wholeInputIsOneRecordWithoutBom() {
    byte[] input = concat(new byte[] {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF}, utf8("x\ny\n"));
    assertEquals(List.of(new Ok(raw("x\ny\n"), 1, new RawSpan(0, 7))), decode(WHOLE, input));
    assertEquals(List.of(), decode(WHOLE, new byte[0]));
  }

  @Test
  void wholeInputLimitIsExact() {
    byte[] input = utf8("x\ny\n");
    assertEquals(
        List.of(new Ok(raw("x\ny\n"), 1, new RawSpan(0, 4))),
        decode(decoder(WHOLE), ByteInput.ofBytes(input), new DecodeLimits(0, 4)));
    assertEquals(
        List.of(new Big(-1, RawSpan.toEnd(0), 4, "x\ny\n", true)),
        decode(decoder(WHOLE), ByteInput.ofBytes(input), new DecodeLimits(0, 3)));
  }
}
