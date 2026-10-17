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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class GzipDecodeTest {

  private static final String GZIP_LINES =
      "{\"compression\":[\"gzip\"],\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"line\"}}}";
  private static final String AUTO_LINES =
      "{\"compression\":\"auto\",\"format\":{\"type\":\"text\",\"framing\":{\"type\":\"line\"}}}";

  private static List<Object> lines(String... texts) {
    long offset = 0;
    List<Object> expected = new ArrayList<>();
    for (int i = 0; i < texts.length; i++) {
      expected.add(new Ok(raw(texts[i]), i + 1, new RawSpan(offset, texts[i].length())));
      offset += texts[i].length() + 1;
    }
    return expected;
  }

  @Test
  void readsEveryMemberEvenWhenTheStreamReportsNothingAvailable() {
    byte[] input = gzip("a\nb\n", "c\n", "d\n");
    assertEquals(
        lines("a", "b", "c", "d"),
        decode(decoder(GZIP_LINES), trickle(input, 3), DecodeLimits.UNLIMITED));
  }

  @Test
  void autoDetectsGzipAndPassesPlainInputThrough() {
    assertEquals(lines("a", "b"), decode(AUTO_LINES, gzip("a\nb\n")));
    assertEquals(lines("a", "b"), decode(AUTO_LINES, utf8("a\nb\n")));
    assertEquals(List.of(), decode(AUTO_LINES, new byte[0]));
  }

  @Test
  void emptyInputWithGzipConfiguredHasNoOutcomes() {
    assertEquals(List.of(), decode(GZIP_LINES, new byte[0]));
  }

  @Test
  void plainInputWithGzipConfiguredIsCorrupt() {
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.CORRUPT_COMPRESSION,
                "",
                1,
                RawSpan.toEnd(0),
                "CorruptCompressionException",
                true)),
        decode(GZIP_LINES, utf8("a\nb\n")));
  }

  @Test
  void trailingGarbageKeepsEveryEarlierRecord() {
    byte[] input = concat(gzip("a\nb\n", "c\n"), utf8("GARBAGE"));
    List<Object> expected = new ArrayList<>(lines("a", "b", "c"));
    expected.add(
        new Bad(
            ErrorKind.CORRUPT_COMPRESSION,
            "",
            4,
            RawSpan.toEnd(6),
            "CorruptCompressionException",
            true));
    assertEquals(expected, decode(GZIP_LINES, input));
    assertEquals(expected, decode(decoder(GZIP_LINES), trickle(input, 2), DecodeLimits.UNLIMITED));
  }

  @Test
  void truncatedTrailerKeepsEveryCompleteLine() {
    byte[] whole = gzip("a\nb\nc\n");
    byte[] input = Arrays.copyOf(whole, whole.length - 3);
    List<Object> expected = new ArrayList<>(lines("a", "b", "c"));
    expected.add(
        new Bad(
            ErrorKind.CORRUPT_COMPRESSION,
            "",
            4,
            RawSpan.toEnd(6),
            "CorruptCompressionException",
            true));
    assertEquals(expected, decode(GZIP_LINES, input));
  }

  @Test
  void truncatedBodyKeepsCompleteLinesAndPointsAtThePartialOne() {
    StringBuilder text = new StringBuilder();
    for (int i = 0; i < 2000; i++) {
      text.append("line-").append(i).append('\n');
    }
    byte[] whole = gzip(text.toString());
    List<Object> outcomes = decode(GZIP_LINES, Arrays.copyOf(whole, whole.length / 2));
    Object last = outcomes.get(outcomes.size() - 1);
    Bad failure = (Bad) last;
    assertEquals(ErrorKind.CORRUPT_COMPRESSION, failure.kind());
    assertEquals(outcomes.size(), failure.recordIndex());
    for (int i = 0; i < outcomes.size() - 1; i++) {
      assertEquals(new Ok(raw("line-" + i), i + 1, ((Ok) outcomes.get(i)).span()), outcomes.get(i));
    }
  }

  @Test
  void corruptCrcIsReported() {
    byte[] input = gzip("a\n");
    input[input.length - 8] ^= 1;
    assertEquals(
        List.of(
            new Ok(raw("a"), 1, new RawSpan(0, 1)),
            new Bad(
                ErrorKind.CORRUPT_COMPRESSION,
                "",
                2,
                RawSpan.toEnd(2),
                "CorruptCompressionException",
                true)),
        decode(GZIP_LINES, input));
  }

  @Test
  void unterminatedFileNameStopsAtTheHeaderLimit() {
    byte[] header = {0x1f, (byte) 0x8b, 8, 0x08, 0, 0, 0, 0, 0, 0};
    byte[] name = new byte[200_000];
    Arrays.fill(name, (byte) 'n');
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.CORRUPT_COMPRESSION,
                "",
                1,
                RawSpan.toEnd(0),
                "CorruptCompressionException",
                true)),
        decode(GZIP_LINES, concat(header, name)));
  }

  @Test
  void networkFailureAnywhereIsAReadFailure() {
    byte[] input = gzip("a\nb\n", "c\nd\n");
    int firstMember = gzip("a\nb\n").length;
    for (int failAt :
        new int[] {5, firstMember - 2, firstMember, firstMember + 4, input.length - 1}) {
      List<Object> outcomes =
          decode(decoder(GZIP_LINES), failingAt(input, failAt), DecodeLimits.UNLIMITED);
      assertEquals(
          new Threw("SourceReadException"), outcomes.get(outcomes.size() - 1), "failAt=" + failAt);
    }
  }
}
