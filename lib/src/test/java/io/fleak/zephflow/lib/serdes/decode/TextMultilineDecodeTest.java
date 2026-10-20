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

class TextMultilineDecodeTest {

  private static final String STACK = multiline("\"continuePattern\":\"^(\\\\s|Caused by: )\"");
  private static final String CONTINUE = multiline("\"continuePattern\":\"^\\\\s\"");
  private static final String START = multiline("\"startPattern\":\"^Log Name:\"");

  private static String multiline(String fields) {
    return "{\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"multiline\"," + fields + "}}}";
  }

  /** {@code text} is a unique ASCII substring of {@code input}. */
  private static Ok event(String input, String text, long line) {
    return new Ok(raw(text), line, new RawSpan(input.indexOf(text), text.length()));
  }

  @Test
  void continuationLinesJoinTheEventBeforeThem() {
    String input = "first\n\tat a\nCaused by: c\n\tat d\nsecond\r\n\tat e\r\nthird";
    assertEquals(
        List.of(
            event(input, "first\n\tat a\nCaused by: c\n\tat d", 1),
            event(input, "second\r\n\tat e", 5),
            event(input, "third", 7)),
        decode(STACK, utf8(input)));
  }

  @Test
  void startPatternBeginsEachEvent() {
    String input = "junk\nLog Name: A\nSource: x\n\nLog Name: B\nSource: y\n";
    assertEquals(
        List.of(
            event(input, "junk", 1),
            event(input, "Log Name: A\nSource: x\n", 2),
            event(input, "Log Name: B\nSource: y", 5)),
        decode(START, utf8(input)));
  }

  @Test
  void eventsOfBlankLinesOnlyAreSkipped() {
    assertEquals(
        List.of(new Ok(raw("a"), 1, new RawSpan(0, 1)), new Ok(raw("b"), 4, new RawSpan(5, 1))),
        decode(CONTINUE, utf8("a\n\n \nb")));
  }

  @Test
  void eventOverMaxLinesIsOversizedWhole() {
    assertEquals(
        List.of(
            new Big(1, 1, new RawSpan(0, 10), LimitKind.MAX_LINES, 3, 10, "a\n b\n c\n d", false),
            new Ok(raw("e"), 5, new RawSpan(11, 1))),
        decode(
            multiline("\"continuePattern\":\"^\\\\s\",\"maxLines\":3"), utf8("a\n b\n c\n d\ne")));
  }

  @Test
  void byteLimitIsExactAndCoversTheWholeEvent() {
    String input = "a\n b\nc\n d\n ee\nf";
    assertEquals(
        List.of(
            event(input, "a\n b", 1),
            new Big(3, 1, new RawSpan(5, 8), LimitKind.MAX_RECORD_BYTES, 4, 8, "c\n d\n ee", false),
            event(input, "f", 6)),
        decode(decoder(CONTINUE), ByteInput.ofBytes(utf8(input)), new DecodeLimits(4, 0)));
    assertEquals(
        List.of(event(input, "a\n b", 1), event(input, "c\n d\n ee", 3), event(input, "f", 6)),
        decode(decoder(CONTINUE), ByteInput.ofBytes(utf8(input)), new DecodeLimits(8, 0)));
  }

  @Test
  void anEventOverBothLimitsNamesTheFirstItReaches() {
    RecordDecoder twoLines = decoder(multiline("\"continuePattern\":\"^\\\\s\",\"maxLines\":2"));
    assertEquals(
        List.of(
            new Big(
                1,
                1,
                new RawSpan(0, 11),
                LimitKind.MAX_RECORD_BYTES,
                4,
                11,
                "abcde\n x\n y",
                false),
            new Ok(raw("z"), 4, new RawSpan(12, 1))),
        decode(twoLines, ByteInput.ofBytes(utf8("abcde\n x\n y\nz")), new DecodeLimits(4, 0)));
    assertEquals(
        List.of(
            new Big(
                1,
                1,
                new RawSpan(0, 19),
                LimitKind.MAX_LINES,
                2,
                19,
                "a\n b\n c\n dddddddddd",
                false),
            new Ok(raw("z"), 5, new RawSpan(20, 1))),
        decode(
            twoLines,
            ByteInput.ofBytes(utf8("a\n b\n c\n dddddddddd\nz")),
            new DecodeLimits(10, 0)));
  }

  @Test
  void oversizedLineMakesItsEventOversized() {
    assertEquals(
        List.of(
            new Big(
                1,
                1,
                new RawSpan(0, 15),
                LimitKind.MAX_RECORD_BYTES,
                5,
                15,
                "ab\n xxxxxxxx\n y",
                false),
            new Ok(raw("c"), 4, new RawSpan(16, 1))),
        decode(
            decoder(CONTINUE),
            ByteInput.ofBytes(utf8("ab\n xxxxxxxx\n y\nc")),
            new DecodeLimits(5, 0)));
  }

  @Test
  void oversizedLineIsMatchedByItsPreview() {
    assertEquals(
        List.of(
            new Ok(raw("S1"), 1, new RawSpan(0, 2)),
            new Big(2, 1, new RawSpan(3, 9), LimitKind.MAX_RECORD_BYTES, 5, 9, "Sxxxxxxxx", false),
            new Ok(raw("S2"), 3, new RawSpan(13, 2))),
        decode(
            decoder(multiline("\"startPattern\":\"^S\"")),
            ByteInput.ofBytes(utf8("S1\nSxxxxxxxx\nS2")),
            new DecodeLimits(5, 0)));
  }

  @Test
  void lineEndingsInsideAnEventAreKeptEvenWhenSplitAcrossReads() {
    String input = "a\r\n b\r c\n d";
    assertEquals(
        List.of(event(input, input, 1)),
        decode(decoder(CONTINUE), trickle(utf8(input), 1), DecodeLimits.UNLIMITED));
  }

  @Test
  void utf8BomIsSkippedButCounted() {
    assertEquals(
        List.of(new Ok(raw("a\n b"), 1, new RawSpan(3, 4))),
        decode(
            CONTINUE, concat(new byte[] {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF}, utf8("a\n b"))));
  }

  @Test
  void pendingEventIsNotEmittedWhenTheReadFails() {
    byte[] input = utf8("a\n b\nc\n d\ne");
    assertEquals(
        List.of(new Ok(raw("a\n b"), 1, new RawSpan(0, 4)), new Threw("SourceReadException")),
        decode(decoder(CONTINUE), failingAt(input, 10), DecodeLimits.UNLIMITED));
  }

  @Test
  void corruptCompressionCoversThePendingEvent() {
    byte[] input = concat(gzip("a\n b\nc\n d\n"), utf8("xy"));
    assertEquals(
        List.of(
            new Ok(raw("a\n b"), 1, new RawSpan(0, 4)),
            new Bad(
                ErrorKind.CORRUPT_COMPRESSION,
                "",
                3,
                RawSpan.toEnd(5),
                "CorruptCompressionException: unexpected bytes after gzip member 1",
                true)),
        decode(
            "{\"compression\":\"gzip\",\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"multiline\",\"continuePattern\":\"^\\\\s\"}}}",
            input));
  }

  @Test
  void oversizedLineKeepsItsRealLineEndingInThePreview() {
    String x = "x".repeat(20);
    String y = "y".repeat(20);
    DecodeLimits limits = new DecodeLimits(10, 0);
    RecordDecoder start = decoder(multiline("\"startPattern\":\"^S\""));
    assertEquals(
        List.of(
            new Big(
                1,
                1,
                new RawSpan(0, 28),
                LimitKind.MAX_RECORD_BYTES,
                10,
                28,
                "S" + x + "\n  cont",
                false),
            new Ok(raw("S next"), 3, new RawSpan(29, 6))),
        decode(start, ByteInput.ofBytes(utf8("S" + x + "\n  cont\nS next")), limits));
    assertEquals(
        List.of(
            new Big(
                1,
                1,
                new RawSpan(0, 26),
                LimitKind.MAX_RECORD_BYTES,
                10,
                26,
                "S a\n" + y + "\rc",
                false),
            new Ok(raw("S z"), 4, new RawSpan(27, 3))),
        decode(start, ByteInput.ofBytes(utf8("S a\n" + y + "\rc\nS z")), limits));
  }

  @Test
  void eventAtMaxLinesDecodes() {
    assertEquals(
        List.of(
            new Ok(raw("a\n b\n c"), 1, new RawSpan(0, 7)), new Ok(raw("d"), 4, new RawSpan(8, 1))),
        decode(multiline("\"continuePattern\":\"^\\\\s\",\"maxLines\":3"), utf8("a\n b\n c\nd")));
  }

  @Test
  void patternIsSearchedAnywhereInTheLine() {
    String input = "x ERROR 1\n detail\ny ERROR 2";
    assertEquals(
        List.of(event(input, "x ERROR 1\n detail", 1), event(input, "y ERROR 2", 3)),
        decode(multiline("\"startPattern\":\"ERROR\""), utf8(input)));
  }

  @Test
  void leadingContinuationLineIsAnEventOfItsOwn() {
    String input = "\tat x\nfirst\n\tat y";
    assertEquals(
        List.of(event(input, "\tat x", 1), event(input, "first\n\tat y", 2)),
        decode(CONTINUE, utf8(input)));
  }

  @Test
  void oversizedEventOfBlankLinesIsStillReported() {
    assertEquals(
        List.of(
            new Big(1, 1, new RawSpan(0, 2), LimitKind.MAX_LINES, 2, 2, "\n\n", false),
            new Ok(raw("S1"), 4, new RawSpan(3, 2))),
        decode(multiline("\"startPattern\":\"^S\",\"maxLines\":2"), utf8("\n\n\nS1")));
  }
}
