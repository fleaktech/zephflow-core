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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.fleak.zephflow.api.structure.ArrayFleakData;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
        List.of(new Big(-1, 1, RawSpan.toEnd(0), LimitKind.MAX_DOCUMENT_BYTES, 3, 4, "a: 1", true)),
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
  void syntaxErrorsAndGlobalTagsAreTerminal() {
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
  }

  @Test
  void aliasesAreCountedPerDocumentNotOverTheStream() {
    StringBuilder input = new StringBuilder();
    List<Object> expected = new ArrayList<>();
    for (int d = 0; d < 3; d++) {
      input.append("---\na: &a [1]\n");
      StringBuilder json = new StringBuilder("{\"a\":[1]");
      for (int i = 0; i < 30; i++) {
        input.append('b').append(i).append(": *a\n");
        json.append(",\"b").append(i).append("\":[1]");
      }
      expected.add(new Ok(json(json.append('}').toString()), d + 1, null));
    }
    assertEquals(expected, decode(MULTI, utf8(input.toString())));
  }

  @Test
  void aliasExpansionIsCappedByTheSizeLimit() {
    String anchored = "a: &a [" + "1,".repeat(999) + "1]\nb: [" + "*a,".repeat(99) + "*a]\n";
    assertEquals(
        List.of(
            malformed(
                1, "MalformedYamlException: document expands to more than 90960 values", false),
            new Ok(json("{\"z\":1}"), 2, null)),
        decode(
            decoder(MULTI),
            ByteInput.ofBytes(utf8(anchored + "---\nz: 1\n")),
            new DecodeLimits(100_000, 0)));
    assertEquals(
        List.of(
            malformedDocument(
                anchored, "MalformedYamlException: document expands to more than 5000 values")),
        decode(decoder(DOCUMENT), ByteInput.ofBytes(utf8(anchored)), new DecodeLimits(0, 10_000)));
  }

  @Test
  void documentsThatFailToConstructDoNotStopTheRest() {
    String input =
        "a: 2024-02-30\n---\nb: !!int x\n---\nc: !!binary \"@@@\"\n---\nd: &x {*x : 1}\n---\n"
            + "e: !foo x\n---\n1: a\n'1': b\n---\n? [a]\n: 1\n---\nx: &y [*y]\n---\nok: 1\n";
    assertEquals(
        List.of(
            malformed(
                1,
                "DocumentConstructionException: DateTimeException: Invalid date 'FEBRUARY 30'",
                false),
            malformed(
                2,
                "DocumentConstructionException: NumberFormatException: For input string: \"x\"",
                false),
            malformed(
                3,
                "DocumentConstructionException: IllegalArgumentException: Illegal base64 character 40",
                false),
            malformed(
                4,
                "DocumentConstructionException: YAMLException: Recursive key for mapping is detected"
                    + " but it is not configured to be allowed.",
                false),
            malformed(
                5,
                "ConstructorException: could not determine a constructor for the tag !foo",
                false),
            malformed(6, "MalformedYamlException: two keys map to the same name: 1", false),
            malformed(7, "MalformedYamlException: mapping key is not a scalar", false),
            malformed(
                8, "MalformedYamlException: document contains itself through an alias", false),
            new Ok(json("{\"ok\":1}"), 9, null)),
        decode(MULTI, utf8(input)));
  }

  @Test
  void unusualScalarsAndCollectionsMap() {
    String input =
        "2024-01-15: d\np: !!pairs [x: 1, x: 2]\n"
            + "n: [12345678901234567890, .inf, -.inf, .nan]\nd: "
            + "[".repeat(60)
            + "]".repeat(60)
            + "\n";
    assertEquals(
        List.of(
            new Ok(
                json(
                    "{\""
                        + LocalDate.of(2024, 1, 15).toEpochDay()
                        + "\":\"d\",\"p\":[[\"x\",1],[\"x\",2]],"
                        + "\"n\":[\"12345678901234567890\",\"Infinity\",\"-Infinity\",\"NaN\"],\"d\":"
                        + "[".repeat(60)
                        + "]".repeat(60)
                        + "}"),
                1,
                new RawSpan(0, input.length()))),
        decode(DOCUMENT, utf8(input)));
  }

  @Test
  void hugeDocumentIsOversizedAndTerminal() {
    String input = "a: 1\n---\nb: " + "x".repeat(200_000) + "\n---\nc: 2\n";
    List<Object> actual =
        decode(decoder(MULTI), ByteInput.ofBytes(utf8(input)), new DecodeLimits(1000, 0));
    long size = ((Big) actual.get(1)).sizeSoFar();
    assertTrue(size > 1000 + 65_536 && size <= 1000 + 65_536 + 16_384, "size " + size);
    assertEquals(
        List.of(
            new Ok(json("{\"a\":1}"), 1, null),
            new Big(2, 1, null, LimitKind.MAX_RECORD_BYTES, 1000, size, "", true)),
        actual);
  }

  @Test
  void readFailureIsClassifiedByOrigin() {
    assertEquals(
        List.of(new Threw("SourceReadException")),
        decode(decoder(MULTI), failingAt(utf8("a: 1\n---\nb: 2\n"), 3), DecodeLimits.UNLIMITED));
  }

  @Test
  void nestingIsLimitedInTheSourceAndAfterAliasExpansion() {
    StringBuilder chain = new StringBuilder("k0: &a0 [1]\n");
    for (int n = 1; n <= 2; n++) {
      chain.append('k').append(n).append(": &a").append(n).append(' ');
      chain.append("[".repeat(300)).append("*a").append(n - 1).append("]".repeat(300)).append('\n');
    }
    assertEquals(
        List.of(
            malformed(1, "MalformedYamlException: document nests deeper than 500 levels", false),
            new Ok(json("{\"z\":1}"), 2, null)),
        decode(MULTI, utf8(chain + "---\nz: 1\n")));
    assertEquals(
        List.of(malformed(1, "YAMLException: Nesting Depth exceeded max 500", true)),
        decode(MULTI, utf8("a: " + "[".repeat(501) + "]".repeat(501) + "\n---\nz: 1\n")));
  }

  @Test
  void aliasCopiesAreCappedEvenWithoutLimits() {
    StringBuilder bomb = new StringBuilder("l0: &l0 [1,1,1,1,1,1,1,1,1,1]\n");
    for (int n = 1; n <= 7; n++) {
      bomb.append('l').append(n).append(": &l").append(n).append(" [");
      for (int i = 0; i < 10; i++) {
        bomb.append(i == 0 ? "" : ",").append("*l").append(n - 1);
      }
      bomb.append("]\n");
    }
    assertEquals(
        List.of(
            malformed(
                1,
                "MalformedYamlException: aliases copy more than 1000000 values into the document",
                false),
            new Ok(json("{\"z\":1}"), 2, null)),
        decode(MULTI, utf8(bomb + "---\nz: 1\n")));
  }

  @Test
  void mergeKeyChainsAreCapped() {
    StringBuilder chain = new StringBuilder("m0: &m0 {k0: 0}\n");
    for (int n = 1; n <= 400; n++) {
      chain.append('m').append(n).append(": &m").append(n);
      chain
          .append(" {<<: *m")
          .append(n - 1)
          .append(", k")
          .append(n)
          .append(": ")
          .append(n)
          .append("}\n");
    }
    assertEquals(
        List.of(
            malformed(
                1,
                "DocumentConstructionException: IllegalStateException: merge keys copy more than"
                    + " 40960 entries into the document",
                false),
            new Ok(json("{\"z\":1}"), 2, null)),
        decode(
            decoder(MULTI),
            ByteInput.ofBytes(utf8(chain + "---\nz: 1\n")),
            new DecodeLimits(1, 0)));
  }

  @Test
  void aliasesToALargeScalarShareItsText() {
    String big = "x".repeat(300_000);
    String encoded = Base64.getEncoder().encodeToString(utf8(big));
    StringBuilder input = new StringBuilder("b: &b !!binary ").append(encoded).append("\nc: [");
    StringBuilder json = new StringBuilder("{\"b\":\"").append(encoded).append("\",\"c\":[");
    for (int i = 0; i < 200; i++) {
      input.append(i == 0 ? "" : ",").append("*b");
      json.append(i == 0 ? "" : ",").append('"').append(encoded).append('"');
    }
    List<Object> actual = decode(MULTI, utf8(input.append("]\n").toString()));
    assertEquals(List.of(new Ok(json(json.append("]}").toString()), 1, null)), actual);
    ArrayFleakData copies = (ArrayFleakData) ((Ok) actual.get(0)).record().getPayload().get("c");
    assertSame(
        copies.getArrayPayload().get(0).getStringValue(),
        copies.getArrayPayload().get(199).getStringValue());
  }

  @Test
  void aliasesToAMapWithALargeBinaryKeyShareTheKeyText() {
    String encoded = Base64.getEncoder().encodeToString(utf8("k".repeat(300_000)));
    StringBuilder input =
        new StringBuilder("m: &m {? !!binary ").append(encoded).append(" : 1}\nl: [");
    for (int i = 0; i < 200; i++) {
      input.append(i == 0 ? "" : ",").append("*m");
    }
    Map<String, Integer> m = Map.of(encoded, 1);
    RecordFleakData expected =
        (RecordFleakData) FleakData.wrap(Map.of("m", m, "l", Collections.nCopies(200, m)));
    List<Object> actual = decode(MULTI, utf8(input.append("]\n").toString()));
    assertEquals(List.of(new Ok(expected, 1, null)), actual);
    List<FleakData> copies = ((Ok) actual.get(0)).record().getPayload().get("l").getArrayPayload();
    assertSame(
        copies.get(0).getPayload().keySet().iterator().next(),
        copies.get(199).getPayload().keySet().iterator().next());
  }
}
