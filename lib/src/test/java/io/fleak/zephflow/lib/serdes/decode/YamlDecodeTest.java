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
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;
import java.util.TimeZone;
import org.junit.jupiter.api.Test;

class YamlDecodeTest {

  private static final String DOCUMENT = "{\"format\":{\"type\":\"yaml\",\"mode\":\"document\"}}";
  private static final String MULTI =
      "{\"format\":{\"type\":\"yaml\",\"mode\":\"multi_document\"}}";

  private static Bad malformedDocument(String input, String error) {
    return new Bad(ErrorKind.MALFORMED, input, -1, new RawSpan(0, input.length()), error, true);
  }

  private static Bad malformed(long index, String error, boolean terminal) {
    return new Bad(ErrorKind.MALFORMED, "", index, null, error, terminal);
  }

  @Test
  void scalarsAreTypedIndependentlyOfTheJvmTimeZone() {
    String input =
        "a: 2024-01-15\nb: '2024-01-15'\nc: 2024-01-15T10:00:00\nd: 2024-01-15T10:00:00+02:00\n"
            + "e: !!binary aGVsbG8=\nf: 0x1F\ng: 1.5\nh: ~\ni: true\n1: x\nj: [1, two]\nk: {n: 1}\n";
    List<Object> expected =
        List.of(
            new Ok(
                json(
                    "{\"a\":"
                        + LocalDate.of(2024, 1, 15).toEpochDay()
                        + ",\"b\":\"2024-01-15\",\"c\":"
                        + Instant.parse("2024-01-15T10:00:00Z").toEpochMilli()
                        + ",\"d\":"
                        + Instant.parse("2024-01-15T08:00:00Z").toEpochMilli()
                        + ",\"e\":\"aGVsbG8=\",\"f\":31,\"g\":1.5,\"h\":null,\"i\":true,\"1\":\"x\","
                        + "\"j\":[1,\"two\"],\"k\":{\"n\":1}}"),
                1,
                new RawSpan(0, input.length())));
    TimeZone original = TimeZone.getDefault();
    try {
      for (String zone : List.of("America/Los_Angeles", "Asia/Tokyo")) {
        TimeZone.setDefault(TimeZone.getTimeZone(zone));
        assertEquals(expected, decode(DOCUMENT, utf8(input)), zone);
      }
    } finally {
      TimeZone.setDefault(original);
    }
  }

  @Test
  void aliasesAreExpandedIntoCopies() {
    String input = "x: &a [1, 2]\ny: *a\n";
    assertEquals(
        List.of(new Ok(json("{\"x\":[1,2],\"y\":[1,2]}"), 1, new RawSpan(0, input.length()))),
        decode(DOCUMENT, utf8(input)));
  }

  @Test
  void documentProblemsAreTerminal() {
    assertEquals(
        List.of(
            malformedDocument(
                "- 1\n- 2", "MalformedYamlException: expected a mapping but found ArrayList")),
        decode(DOCUMENT, utf8("- 1\n- 2")));
    assertEquals(
        List.of(
            malformedDocument(
                "a: 1\n---\nb: 2", "ComposerException: expected a single document in the stream")),
        decode(DOCUMENT, utf8("a: 1\n---\nb: 2")));
    assertEquals(
        List.of(
            malformedDocument("a: 1\na: 2", "DuplicateKeyException: while constructing a mapping")),
        decode(DOCUMENT, utf8("a: 1\na: 2")));
    assertEquals(
        List.of(
            malformedDocument(
                "a: &x [*x]", "MalformedYamlException: document contains itself through an alias")),
        decode(DOCUMENT, utf8("a: &x [*x]")));
    assertEquals(
        List.of(malformedDocument("a: [1", "ParserException: while parsing a flow sequence")),
        decode(DOCUMENT, utf8("a: [1")));
  }

  @Test
  void emptyAndNullDocumentsGiveNoRecord() {
    assertEquals(List.of(), decode(DOCUMENT, new byte[0]));
    assertEquals(List.of(), decode(DOCUMENT, utf8("~")));
    assertEquals(List.of(), decode(DOCUMENT, utf8("# only a comment\n")));
    assertEquals(List.of(), decode(MULTI, utf8("---\n---\n~\n")));
  }

  @Test
  void documentLimitIsExact() {
    byte[] input = utf8("a: 1");
    assertEquals(
        List.of(new Ok(json("{\"a\":1}"), 1, new RawSpan(0, 4))),
        decode(decoder(DOCUMENT), ByteInput.ofBytes(input), new DecodeLimits(0, 4)));
    assertEquals(
        List.of(new Big(-1, RawSpan.toEnd(0), 4, "a: 1", true)),
        decode(decoder(DOCUMENT), ByteInput.ofBytes(input), new DecodeLimits(0, 3)));
  }

  @Test
  void byteOrderMarkSelectsTheEncoding() {
    byte[] input =
        concat(new byte[] {(byte) 0xFF, (byte) 0xFE}, "a: 1".getBytes(StandardCharsets.UTF_16LE));
    assertEquals(
        List.of(new Ok(json("{\"a\":1}"), 1, new RawSpan(0, input.length))),
        decode(DOCUMENT, input));
  }

  @Test
  void eachDocumentIsARecordAndBadDocumentsDoNotStopTheRest() {
    assertEquals(
        List.of(
            new Ok(json("{\"a\":1}"), 1, null),
            malformed(2, "MalformedYamlException: expected a mapping but found ArrayList", false),
            new Ok(json("{\"b\":2}"), 3, null),
            malformed(4, "DuplicateKeyException: while constructing a mapping", false),
            new Ok(json("{\"c\":3}"), 5, null)),
        decode(MULTI, utf8("a: 1\n---\n- x\n---\n\n---\nb: 2\n---\na: 1\na: 2\n---\nc: 3\n")));
  }

  @Test
  void syntaxErrorsGlobalTagsAndAliasFloodsAreTerminal() {
    assertEquals(
        List.of(
            new Ok(json("{\"a\":1}"), 1, null),
            malformed(2, "ParserException: while parsing a flow sequence", true)),
        decode(MULTI, utf8("a: 1\n---\nb: [1\n---\nc: 3\n")));
    assertEquals(
        List.of(
            malformed(
                1,
                "ComposerException: Global tag is not allowed:"
                    + " tag:yaml.org,2002:python/object:os.system",
                true)),
        decode(MULTI, utf8("a: !!python/object:os.system x\n---\nb: 1\n")));
    StringBuilder flood = new StringBuilder("a: &a [1]\n");
    for (int i = 0; i < 60; i++) {
      flood.append('b').append(i).append(": *a\n");
    }
    assertEquals(
        List.of(
            malformed(
                1,
                "YAMLException: Number of aliases for non-scalar nodes exceeds the specified max=50",
                true)),
        decode(MULTI, utf8(flood + "---\nz: 1\n")));
  }

  @Test
  void aliasExpansionIsCapped() {
    StringBuilder input = new StringBuilder("a: &a [").append("1,".repeat(25_000)).append("1]\n");
    input.append("b: [");
    for (int i = 0; i < 45; i++) {
      input.append(i == 0 ? "" : ",").append("*a");
    }
    input.append("]\n---\nz: 1\n");
    assertEquals(
        List.of(
            malformed(
                1, "MalformedYamlException: document expands to more than 1000000 values", false),
            new Ok(json("{\"z\":1}"), 2, null)),
        decode(MULTI, utf8(input.toString())));
  }

  @Test
  void hugeDocumentIsOversizedAndTerminal() {
    String input = "a: 1\n---\nb: " + "x".repeat(200_000) + "\n---\nc: 2\n";
    List<Object> actual =
        decode(decoder(MULTI), ByteInput.ofBytes(utf8(input)), new DecodeLimits(1000, 0));
    Big big = (Big) actual.get(1);
    assertEquals(
        List.of(new Ok(json("{\"a\":1}"), 1, null), new Big(2, null, big.sizeSoFar(), "", true)),
        actual);
  }

  @Test
  void readFailureIsClassifiedByOrigin() {
    assertEquals(
        List.of(new Threw("SourceReadException")),
        decode(decoder(MULTI), failingAt(utf8("a: 1\n---\nb: 2\n"), 3), DecodeLimits.UNLIMITED));
  }
}
