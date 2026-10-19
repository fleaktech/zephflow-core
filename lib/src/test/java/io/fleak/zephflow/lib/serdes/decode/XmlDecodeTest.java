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

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class XmlDecodeTest {

  private static final String DOCUMENT = "{\"format\":{\"type\":\"xml\",\"mode\":\"document\"}}";
  private static final String EVENTS =
      "{\"format\":{\"type\":\"xml\",\"mode\":\"elements\",\"recordPath\":\"/Events/Event\"}}";

  private static Bad malformedDocument(String input, String error) {
    return new Bad(ErrorKind.MALFORMED, input, -1, new RawSpan(0, input.length()), error, true);
  }

  @Test
  void documentMapsAttributesRepeatedChildrenAndText() {
    String input = "<r a=\"1\"><b>x</b><b>y</b><c/><d k=\"v\">t</d></r>";
    assertEquals(
        List.of(
            new Ok(
                json("{\"a\":\"1\",\"b\":[\"x\",\"y\"],\"c\":\"\",\"d\":{\"k\":\"v\",\"\":\"t\"}}"),
                1,
                new RawSpan(0, input.length()))),
        decode(DOCUMENT, utf8(input)));
  }

  @Test
  void onlyCommentsProcessingInstructionsAndSpaceMayFollowTheRoot() {
    String input = "<r/><!-- c --><?pi x?>\n";
    assertEquals(
        List.of(new Ok(json("{}"), 1, new RawSpan(0, input.length()))),
        decode(DOCUMENT, utf8(input)));
    assertEquals(
        List.of(
            malformedDocument(
                "<r/> x",
                "WstxUnexpectedCharException: Unexpected character 'x' (code 120) in epilog;"
                    + " expected '<'")),
        decode(DOCUMENT, utf8("<r/> x")));
    assertEquals(
        List.of(
            malformedDocument(
                "<r/><r2/>",
                "WstxParsingException: Illegal to have multiple roots (start tag in epilog?).")),
        decode(DOCUMENT, utf8("<r/><r2/>")));
  }

  @Test
  void brokenDocumentIsMalformedAndEmptyInputGivesNothing() {
    assertEquals(
        List.of(
            malformedDocument(
                "<r><x></r>", "JsonParseException: Unexpected close tag </r>; expected </x>.")),
        decode(DOCUMENT, utf8("<r><x></r>")));
    assertEquals(
        List.of(malformedDocument(" ", "WstxEOFException: Unexpected EOF in prolog")),
        decode(DOCUMENT, utf8(" ")));
    assertEquals(List.of(), decode(DOCUMENT, new byte[0]));
    assertEquals(List.of(), decode(EVENTS, new byte[0]));
  }

  @Test
  void dtdsAreRefusedSoNoEntityIsExpandedOrFetched() {
    for (String input :
        List.of(
            "<!DOCTYPE r [<!ENTITY e \"boom\">]><r>&e;</r>",
            "<!DOCTYPE r SYSTEM \"file:///etc/passwd\"><r/>")) {
      assertEquals(
          List.of(malformedDocument(input, "MalformedXmlException: DTDs are not allowed")),
          decode(DOCUMENT, utf8(input)),
          input);
    }
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                "",
                1,
                null,
                "MalformedXmlException: DTDs are not allowed",
                true)),
        decode(EVENTS, utf8("<!DOCTYPE Events SYSTEM \"http://example.com/x.dtd\"><Events/>")));
  }

  @Test
  void documentLimitIsExact() {
    byte[] input = utf8("<r>12</r>");
    assertEquals(
        List.of(new Ok(json("{\"\":\"12\"}"), 1, new RawSpan(0, 9))),
        decode(decoder(DOCUMENT), ByteInput.ofBytes(input), new DecodeLimits(0, 9)));
    assertEquals(
        List.of(
            new Big(
                -1, 1, RawSpan.toEnd(0), LimitKind.MAX_DOCUMENT_BYTES, 8, 9, "<r>12</r>", true)),
        decode(decoder(DOCUMENT), ByteInput.ofBytes(input), new DecodeLimits(0, 8)));
  }

  @Test
  void elementsAtTheRecordPathAreRecordsAndOthersAreIgnored() {
    assertEquals(
        List.of(
            new Ok(json("{\"id\":\"1\",\"a\":\"x\"}"), 1, null),
            new Ok(json("{\"id\":\"2\"}"), 2, null)),
        decode(
            EVENTS,
            utf8(
                "<Events><Event id=\"1\"><a>x</a></Event><Other><Event>no</Event></Other>"
                    + "<Event id=\"2\"/></Events>")));
  }

  @Test
  void namespacesAreIgnoredWhenMatching() {
    assertEquals(
        List.of(new Ok(json("{\"\":\"1\"}"), 1, null)),
        decode(EVENTS, utf8("<e:Events xmlns:e=\"urn:x\"><e:Event>1</e:Event></e:Events>")));
  }

  @Test
  void recordPathThatMatchesNothingIsMalformed() {
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                "",
                1,
                null,
                "MalformedXmlException: recordPath /Events/Event matched no element",
                true)),
        decode(EVENTS, utf8("<Events><Other/></Events>")));
  }

  @Test
  void brokenStructureEndsDecodingAfterTheRecordsBeforeIt() {
    assertEquals(
        List.of(
            new Ok(json("{\"\":\"1\"}"), 1, null),
            new Bad(
                ErrorKind.MALFORMED,
                "",
                2,
                null,
                "JsonParseException: Unexpected EOF; was expecting a close tag for element <Event>",
                true)),
        decode(EVENTS, utf8("<Events><Event>1</Event><Event>")));
  }

  @Test
  void hugeElementIsOversizedWhetherOrNotItIsARecord() {
    String text = "x".repeat(200_000);
    for (String input :
        List.of(
            "<Events><Event>1</Event><Event>" + text + "</Event></Events>",
            "<Events><Event>1</Event><Header>" + text + "</Header><Event/></Events>")) {
      List<Object> actual =
          decode(decoder(EVENTS), ByteInput.ofBytes(utf8(input)), new DecodeLimits(1000, 0));
      long size = ((Big) actual.get(1)).sizeSoFar();
      assertTrue(size > 1000 + 65_536 && size <= 1000 + 65_536 + 16_384, "size " + size);
      assertEquals(
          List.of(
              new Ok(json("{\"\":\"1\"}"), 1, null),
              new Big(2, 1, null, LimitKind.MAX_RECORD_BYTES, 1000, size, "", true)),
          actual);
    }
  }

  @Test
  void readFailureAndCorruptCompressionAreClassifiedByOrigin() {
    byte[] input = utf8("<Events><Event>1</Event><Event>2</Event></Events>");
    assertEquals(
        List.of(new Threw("SourceReadException")),
        decode(decoder(EVENTS), failingAt(input, 10), DecodeLimits.UNLIMITED));
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.CORRUPT_COMPRESSION,
                "",
                1,
                null,
                "CorruptCompressionException: gzip member 1 is truncated",
                true)),
        decode(
            "{\"compression\":\"gzip\",\"format\":{\"type\":\"xml\",\"mode\":\"elements\","
                + "\"recordPath\":\"/Events/Event\"}}",
            Arrays.copyOf(gzipOne(input), 20)));
  }

  @Test
  void encodingComesFromTheXmlDeclaration() {
    byte[] input =
        concat(
            utf8("<?xml version=\"1.0\" encoding=\"ISO-8859-1\"?><r>caf"),
            new byte[] {(byte) 0xE9},
            utf8("</r>"));
    assertEquals(
        List.of(new Ok(json("{\"\":\"café\"}"), 1, new RawSpan(0, input.length))),
        decode(DOCUMENT, input));
  }

  @Test
  void contentAfterTheRootEndsElementsModeAfterTheRecords() {
    assertEquals(
        List.of(
            new Ok(json("{\"\":\"1\"}"), 1, null),
            new Bad(
                ErrorKind.MALFORMED,
                "",
                2,
                null,
                "WstxParsingException: Illegal to have multiple roots (start tag in epilog?).",
                true)),
        decode(EVENTS, utf8("<Events><Event>1</Event></Events><x/>")));
  }

  @Test
  void manyAttributesAreAllowed() {
    StringBuilder input = new StringBuilder("<r");
    StringBuilder json = new StringBuilder("{");
    for (int i = 0; i < 1500; i++) {
      input.append(" a").append(i).append("=\"").append(i).append('"');
      json.append(i == 0 ? "" : ",").append("\"a").append(i).append("\":\"").append(i).append('"');
    }
    String xml = input.append("/>").toString();
    assertEquals(
        List.of(new Ok(json(json.append('}').toString()), 1, new RawSpan(0, xml.length()))),
        decode(DOCUMENT, utf8(xml)));
  }
}
