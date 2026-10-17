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

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CsvDecodeTest {

  private static final String CSV = csv("");

  private static String csv(String options) {
    return "{\"format\":{\"type\":\"csv\"" + options + "}}";
  }

  /** {@code text} is a unique substring of the ASCII {@code input}. */
  private static RawSpan span(String input, String text) {
    return new RawSpan(input.indexOf(text), text.length());
  }

  @Test
  void headerNamesTheColumnsAndRowsHaveExactSpans() {
    String input = "a,b\r\n1,2\n3,4";
    assertEquals(
        List.of(
            new Ok(json("{\"a\":\"1\",\"b\":\"2\"}"), 1, new RawSpan(5, 3)),
            new Ok(json("{\"a\":\"3\",\"b\":\"4\"}"), 2, new RawSpan(9, 3))),
        decode(CSV, utf8(input)));
  }

  @Test
  void quotedFieldsKeepDelimitersLineBreaksAndDoubledQuotes() {
    String row = "\"x,y\",\"line1\nline2 \"\"q\"\"\"";
    String input = "a,b\n" + row + "\n";
    assertEquals(
        List.of(
            new Ok(json("{\"a\":\"x,y\",\"b\":\"line1\\nline2 \\\"q\\\"\"}"), 1, span(input, row))),
        decode(CSV, utf8(input)));
  }

  @Test
  void emptyLinesAreSkippedAndEmptyFieldsKept() {
    String input = "a,b\n\n,\n1,\n";
    assertEquals(
        List.of(
            new Ok(json("{\"a\":\"\",\"b\":\"\"}"), 1, new RawSpan(5, 1)),
            new Ok(json("{\"a\":\"1\",\"b\":\"\"}"), 2, new RawSpan(7, 2))),
        decode(CSV, utf8(input)));
  }

  @Test
  void wrongColumnCountIsMalformedAndDecodingContinues() {
    String input = "a,b\n1\n1,2,3\n4,5";
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                "1",
                1,
                new RawSpan(4, 1),
                "MalformedCsvException: row has 1 columns, expected 2",
                false),
            new Bad(
                ErrorKind.MALFORMED,
                "1,2,3",
                2,
                new RawSpan(6, 5),
                "MalformedCsvException: row has 3 columns, expected 2",
                false),
            new Ok(json("{\"a\":\"4\",\"b\":\"5\"}"), 3, new RawSpan(12, 3))),
        decode(CSV, utf8(input)));
  }

  @Test
  void withoutHeaderColumnsAreNamedByPosition() {
    assertEquals(
        List.of(
            new Ok(json("{\"_c0\":\"1\",\"_c1\":\"2\"}"), 1, new RawSpan(0, 3)),
            new Ok(json("{\"_c0\":\"3\"}"), 2, new RawSpan(4, 1))),
        decode(csv(",\"header\":false"), utf8("1,2\n3")));
  }

  @Test
  void explicitColumnsReplaceTheHeaderLine() {
    List<Object> expected =
        List.of(new Ok(json("{\"x\":\"1\",\"y\":\"2\"}"), 1, new RawSpan(4, 3)));
    assertEquals(expected, decode(csv(",\"columns\":[\"x\",\"y\"]"), utf8("a,b\n1,2")));
    assertEquals(
        List.of(new Ok(json("{\"x\":\"1\",\"y\":\"2\"}"), 1, new RawSpan(0, 3))),
        decode(csv(",\"header\":false,\"columns\":[\"x\",\"y\"]"), utf8("1,2")));
  }

  @Test
  void dialectOptionsApply() {
    String row = " 'a;b' ; c\\;d ";
    String input = "skipped \"line\n# comment\nh1; h2\n" + row + "\n";
    assertEquals(
        List.of(new Ok(json("{\"h1\":\"a;b\",\"h2\":\"c;d\"}"), 1, span(input, row))),
        decode(
            csv(
                ",\"delimiter\":\";\",\"quote\":\"'\",\"escape\":\"\\\\\",\"commentMarker\":\"#\","
                    + "\"skipLines\":1,\"ignoreSurroundingSpaces\":true"),
            utf8(input)));
  }

  @Test
  void commentMarkerCountsOnlyAtTheStartOfARecord() {
    assertEquals(
        List.of(new Ok(json("{\"a\":\"x#y\"}"), 1, new RawSpan(5, 3))),
        decode(csv(",\"commentMarker\":\"#\""), utf8("a\n#1\nx#y")));
  }

  @Test
  void emptyQuoteTurnsQuotingOff() {
    assertEquals(
        List.of(new Ok(json("{\"a\":\"\\\"x\",\"b\":\"y\\\"\"}"), 1, new RawSpan(4, 5))),
        decode(csv(",\"quote\":\"\""), utf8("a,b\n\"x,y\"")));
  }

  @Test
  void textAfterAClosingQuoteIsMalformedUnlessItIsSpaceThatIsIgnored() {
    String input = "a,b\n\"x\" ,y\n \"z\",w";
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                "\"x\" ,y",
                1,
                new RawSpan(4, 6),
                "MalformedCsvException: unexpected character after a closing quote",
                false),
            new Ok(json("{\"a\":\" \\\"z\\\"\",\"b\":\"w\"}"), 2, new RawSpan(11, 6))),
        decode(CSV, utf8(input)));
    assertEquals(
        List.of(
            new Ok(json("{\"a\":\"x\",\"b\":\"y\"}"), 1, new RawSpan(4, 6)),
            new Ok(json("{\"a\":\"z\",\"b\":\"w\"}"), 2, new RawSpan(11, 6))),
        decode(csv(",\"ignoreSurroundingSpaces\":true"), utf8(input)));
  }

  @Test
  void quoteLeftOpenUntilTheEndIsTerminal() {
    assertEquals(
        List.of(
            new Ok(json("{\"a\":\"1\"}"), 1, new RawSpan(2, 1)),
            new Bad(
                ErrorKind.MALFORMED,
                "\"open\nmore",
                2,
                RawSpan.toEnd(4),
                "MalformedCsvException: quoted field is not closed before the end of the input",
                true)),
        decode(CSV, utf8("a\n1\n\"open\nmore")));
  }

  @Test
  void escapeAtTheEndOfTheInputIsMalformed() {
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                "x\\",
                1,
                new RawSpan(2, 2),
                "MalformedCsvException: escape character at the end of the input",
                false)),
        decode(csv(",\"escape\":\"\\\\\""), utf8("a\nx\\")));
  }

  @Test
  void recordLimitIsExactAndAnOversizedRowIsSkippedToItsEnd() {
    assertEquals(
        List.of(
            new Ok(json("{\"a\":\"123\"}"), 1, new RawSpan(2, 3)),
            new Big(2, new RawSpan(6, 4), 4, "1234", false),
            new Ok(json("{\"a\":\"5\"}"), 3, new RawSpan(11, 1))),
        decode(decoder(CSV), ByteInput.ofBytes(utf8("a\n123\n1234\n5")), new DecodeLimits(3, 0)));
    String big = "\"xxxxxxxx\nyy\",3";
    String input = "a,b\n1,2\n" + big + "\n4,5";
    assertEquals(
        List.of(
            new Ok(json("{\"a\":\"1\",\"b\":\"2\"}"), 1, new RawSpan(4, 3)),
            new Big(2, span(input, big), big.length(), big, false),
            new Ok(json("{\"a\":\"4\",\"b\":\"5\"}"), 3, new RawSpan(24, 3))),
        decode(decoder(CSV), ByteInput.ofBytes(utf8(input)), new DecodeLimits(5, 0)));
  }

  @Test
  void oversizedQuoteLeftOpenIsTerminal() {
    assertEquals(
        List.of(new Big(1, RawSpan.toEnd(2), 11, "\"xxxxxxxxxx", true)),
        decode(decoder(CSV), ByteInput.ofBytes(utf8("a\n\"xxxxxxxxxx")), new DecodeLimits(5, 0)));
  }

  @Test
  void headerProblemsAreTerminalWithRecordIndexZero() {
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                "a,a",
                0,
                RawSpan.toEnd(0),
                "MalformedCsvException: header has a duplicate column name: a",
                true)),
        decode(CSV, utf8("a,a\n1,2")));
    assertEquals(
        List.of(new Big(0, RawSpan.toEnd(0), 6, "abcdef", true)),
        decode(decoder(CSV), ByteInput.ofBytes(utf8("abcdef\n1")), new DecodeLimits(5, 0)));
    assertEquals(
        List.of(new Ok(json("{\"a\":\"1\",\"_c1\":\"2\"}"), 1, new RawSpan(3, 3))),
        decode(CSV, utf8("a,\n1,2")));
    assertEquals(List.of(), decode(CSV, utf8("a,b\n")));
    assertEquals(List.of(), decode(CSV, new byte[0]));
  }

  @Test
  void byteOrderMarkIsSkippedAndOtherCharsetsAreTranscoded() {
    assertEquals(
        List.of(new Ok(json("{\"a\":\"1\"}"), 1, new RawSpan(5, 1))),
        decode(CSV, concat(new byte[] {(byte) 0xEF, (byte) 0xBB, (byte) 0xBF}, utf8("a\n1"))));
    assertEquals(
        List.of(new Ok(json("{\"n\":\"café\"}"), 1, new RawSpan(2, 5))),
        decode(
            csv(",\"charset\":\"windows-1252\""),
            new byte[] {'n', '\n', 'c', 'a', 'f', (byte) 0xE9}));
  }

  @Test
  void crLfSplitAcrossReadsIsOneLineEnding() {
    assertEquals(
        List.of(
            new Ok(json("{\"a\":\"1\"}"), 1, new RawSpan(3, 1)),
            new Ok(json("{\"a\":\"2\"}"), 2, new RawSpan(6, 1))),
        decode(decoder(CSV), trickle(utf8("a\r\n1\r\n2"), 1), DecodeLimits.UNLIMITED));
  }

  @Test
  void rowsBeforeAReadFailureAreKept() {
    assertEquals(
        List.of(
            new Ok(json("{\"a\":\"1\"}"), 1, new RawSpan(2, 1)), new Threw("SourceReadException")),
        decode(decoder(CSV), failingAt(utf8("a\n1\n2\n"), 5), DecodeLimits.UNLIMITED));
  }

  @Test
  void corruptCompressionCoversTheRowBeingRead() {
    assertEquals(
        List.of(
            new Ok(json("{\"a\":\"1\"}"), 1, new RawSpan(2, 1)),
            new Bad(
                ErrorKind.CORRUPT_COMPRESSION,
                "",
                2,
                RawSpan.toEnd(4),
                "CorruptCompressionException: unexpected bytes after gzip member 1",
                true)),
        decode(
            "{\"compression\":\"gzip\",\"format\":{\"type\":\"csv\"}}",
            concat(gzip("a\n1\n2"), utf8("xy"))));
  }

  @Test
  void malformedOrUnclosedHeaderIsTerminalAndCoversTheRest() {
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                "\"a\"x,b",
                0,
                RawSpan.toEnd(0),
                "MalformedCsvException: header: unexpected character after a closing quote",
                true)),
        decode(CSV, utf8("\"a\"x,b\n1,2")));
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                "\"a,b\n1,2",
                0,
                RawSpan.toEnd(0),
                "MalformedCsvException: quoted field is not closed before the end of the input",
                true)),
        decode(CSV, utf8("\"a,b\n1,2")));
  }

  @Test
  void headerLineReplacedByColumnsIsSkippedWithoutBeingChecked() {
    String columns = csv(",\"columns\":[\"p\",\"q\"]");
    List<Object> expected =
        List.of(new Ok(json("{\"p\":\"1\",\"q\":\"2\"}"), 1, new RawSpan(7, 3)));
    assertEquals(expected, decode(columns, utf8("\"a\"x,b\n1,2")));
    assertEquals(
        List.of(new Ok(json("{\"p\":\"1\",\"q\":\"2\"}"), 1, new RawSpan(6, 3))),
        decode(columns, utf8("a,a,a\n1,2")));
    assertEquals(
        List.of(new Ok(json("{\"p\":\"1\",\"q\":\"2\"}"), 1, new RawSpan(8, 3))),
        decode(decoder(columns), ByteInput.ofBytes(utf8("abcdefg\n1,2")), new DecodeLimits(5, 0)));
  }

  @Test
  void headerAtTheRecordLimitDecodes() {
    assertEquals(
        List.of(new Ok(json("{\"abc\":\"1\"}"), 1, new RawSpan(4, 1))),
        decode(decoder(CSV), ByteInput.ofBytes(utf8("abc\n1")), new DecodeLimits(3, 0)));
  }

  @Test
  void escapeWorksInsideQuotesAndAcrossLineBreaks() {
    String escape = csv(",\"header\":false,\"escape\":\"\\\\\"");
    assertEquals(
        List.of(
            new Ok(json("{\"_c0\":\"x\\\"y\"}"), 1, new RawSpan(0, 6)),
            new Ok(json("{\"_c0\":\"p\\nq\"}"), 2, new RawSpan(7, 4))),
        decode(escape, utf8("\"x\\\"y\"\np\\\nq")));
  }

  @Test
  void escapeAfterAClosingQuoteStillEscapes() {
    assertEquals(
        List.of(
            new Bad(
                ErrorKind.MALFORMED,
                "\"a\"\\\nb",
                1,
                new RawSpan(0, 6),
                "MalformedCsvException: unexpected character after a closing quote",
                false)),
        decode(csv(",\"header\":false,\"escape\":\"\\\\\""), utf8("\"a\"\\\nb\n")));
  }

  @Test
  void crOnlyLineEndingsSplitRowsButNotQuotedFields() {
    assertEquals(
        List.of(
            new Ok(json("{\"a\":\"1\"}"), 1, new RawSpan(2, 1)),
            new Ok(json("{\"a\":\"x\\ry\"}"), 2, new RawSpan(4, 5))),
        decode(CSV, utf8("a\r1\r\"x\ry\"")));
  }

  @Test
  void rowWithTooManyColumnsIsMalformedAndDecodingContinues() {
    String atCap = ",".repeat(16_383);
    String wide = atCap + ",";
    List<Object> actual = decode(csv(",\"header\":false"), utf8(atCap + "\n" + wide + "\nlast"));
    Map<String, Object> empty = new LinkedHashMap<>();
    for (int i = 0; i < 16_384; i++) {
      empty.put("_c" + i, "");
    }
    assertEquals(
        List.of(
            new Ok((RecordFleakData) FleakData.wrap(empty), 1, new RawSpan(0, atCap.length())),
            new Bad(
                ErrorKind.MALFORMED,
                wide,
                2,
                new RawSpan(atCap.length() + 1, wide.length()),
                "MalformedCsvException: row has more than 16384 columns",
                false),
            new Ok(
                json("{\"_c0\":\"last\"}"), 3, new RawSpan(atCap.length() + wide.length() + 2, 4))),
        actual);
  }

  @Test
  void headerLeftOpenIsTerminalEvenWhenColumnsReplaceIt() {
    assertEquals(
        List.of(new Big(0, RawSpan.toEnd(0), 11, "\"abcdefgh\n1", true)),
        decode(
            decoder(csv(",\"columns\":[\"p\"]")),
            ByteInput.ofBytes(utf8("\"abcdefgh\n1")),
            new DecodeLimits(5, 0)));
  }
}
