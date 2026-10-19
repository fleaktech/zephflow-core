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
package io.fleak.zephflow.lib.commands.s3realtimesource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.CompressionType;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.SourceReadException;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.utils.CompressionUtils;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.Abortable;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * Feeds the same bytes to the legacy deserializers (as the legacy S3 realtime converter used them)
 * and to {@link S3ObjectDecoder} with the legacy mapping, and pins both results: the same records
 * for valid input, and each decoding difference of the spec's section 4.2.
 */
class LegacyDifferentialTest {

  private static final long MIB = 1024L * 1024;
  private static final long HEAP = 2048 * MIB;
  private static final String FAILS = "FAILS";
  private static final byte[] JSON_LINES = utf8("{\"a\":1}\n{\"a\":2}\n");

  @TempDir Path spoolDir;

  /**
   * What each side gives. {@code legacy} is the records as JSON, or {@code FAILS} when the legacy
   * path failed the whole object; {@code decoded} is each outcome: {@code ok <json>}, {@code bad
   * <kind>[ terminal] <raw>}, or {@code big #<index>}.
   */
  record Observed(List<String> legacy, List<String> decoded) {}

  static Stream<Arguments> cases() throws IOException {
    return Stream.of(
        // valid input decodes the same
        same(
            "jsonObject",
            EncodingType.JSON_OBJECT,
            utf8("{\"a\":1,\"b\":\"x\"}"),
            "{\"a\":1,\"b\":\"x\"}"),
        same(
            "jsonArray",
            EncodingType.JSON_ARRAY,
            utf8("[{\"a\":1},{\"a\":2}]"),
            "{\"a\":1}",
            "{\"a\":2}"),
        same("jsonLines", EncodingType.JSON_OBJECT_LINE, JSON_LINES, "{\"a\":1}", "{\"a\":2}"),
        same(
            "stringLineKeepsBlankLines",
            EncodingType.STRING_LINE,
            utf8("x\n\ny\n"),
            "{\"__raw__\":\"x\"}",
            "{\"__raw__\":\"\"}",
            "{\"__raw__\":\"y\"}"),
        same("text", EncodingType.TEXT, utf8("hello\nworld"), "{\"__raw__\":\"hello\\nworld\"}"),
        same(
            "csvTrimsSpaces",
            EncodingType.CSV,
            utf8("name, age\n bob , 3\n"),
            "{\"name\":\"bob\",\"age\":\"3\"}"),
        same("xml", EncodingType.XML, utf8("<r><a>1</a></r>"), "{\"a\":\"1\"}"),
        same(
            "gzipDetected",
            EncodingType.JSON_OBJECT_LINE,
            gzip(JSON_LINES),
            "{\"a\":1}",
            "{\"a\":2}"),
        Arguments.of(
            "gzipForced",
            EncodingType.JSON_OBJECT_LINE,
            CompressionType.GZIP,
            gzip(JSON_LINES),
            HEAP,
            new Observed(
                List.of("{\"a\":1}", "{\"a\":2}"), List.of("ok {\"a\":1}", "ok {\"a\":2}"))),
        same("emptyJsonLines", EncodingType.JSON_OBJECT_LINE, new byte[0]),
        same(
            "csvQuotedComma",
            EncodingType.CSV,
            utf8("a,b\n\"x,y\",2\n"),
            "{\"a\":\"x,y\",\"b\":\"2\"}"),
        same(
            "csvEscapedQuote",
            EncodingType.CSV,
            utf8("a,b\n\"say \"\"hi\"\"\",2\n"),
            "{\"a\":\"say \\\"hi\\\"\",\"b\":\"2\"}"),
        same(
            "csvEmptyValues",
            EncodingType.CSV,
            utf8("a,b,c\n,2,\n"),
            "{\"a\":\"\",\"b\":\"2\",\"c\":\"\"}"),
        same(
            "csvCrlf",
            EncodingType.CSV,
            utf8("a,b\r\n1,2\r\n3,4\r\n"),
            "{\"a\":\"1\",\"b\":\"2\"}",
            "{\"a\":\"3\",\"b\":\"4\"}"),
        same(
            "csvEmbeddedNewline",
            EncodingType.CSV,
            utf8("a,b\n\"line1\nline2\",2\n"),
            "{\"a\":\"line1\\nline2\",\"b\":\"2\"}"),
        same(
            "csvNoTrailingNewline",
            EncodingType.CSV,
            utf8("a,b\n1,2"),
            "{\"a\":\"1\",\"b\":\"2\"}"),
        same("csvUnicode", EncodingType.CSV, utf8("名前,年齢\n太郎,3\n"), "{\"年齢\":\"3\",\"名前\":\"太郎\"}"),
        same("csvHeaderOnly", EncodingType.CSV, utf8("a,b\n")),
        same(
            "csvQuotedSpaces",
            EncodingType.CSV,
            utf8("a,b\n\" x \",2\n"),
            "{\"a\":\" x \",\"b\":\"2\"}"),
        same(
            "stringLineCrlf",
            EncodingType.STRING_LINE,
            utf8("x\r\ny\r\n"),
            "{\"__raw__\":\"x\"}",
            "{\"__raw__\":\"y\"}"),
        same(
            "stringLineCr",
            EncodingType.STRING_LINE,
            utf8("x\ry\r"),
            "{\"__raw__\":\"x\"}",
            "{\"__raw__\":\"y\"}"),
        same(
            "stringLineNoTrailingNewline",
            EncodingType.STRING_LINE,
            utf8("x\ny"),
            "{\"__raw__\":\"x\"}",
            "{\"__raw__\":\"y\"}"),
        same(
            "stringLineUnicode",
            EncodingType.STRING_LINE,
            utf8("héllo 世界 😀\n"),
            "{\"__raw__\":\"héllo 世界 😀\"}"),
        same(
            "stringLineSpaces",
            EncodingType.STRING_LINE,
            utf8("  x  \n\t\n"),
            "{\"__raw__\":\"  x  \"}",
            "{\"__raw__\":\"\\t\"}"),
        same(
            "stringLineManyBlank",
            EncodingType.STRING_LINE,
            utf8("\n\n\nx\n"),
            "{\"__raw__\":\"\"}",
            "{\"__raw__\":\"\"}",
            "{\"__raw__\":\"\"}",
            "{\"__raw__\":\"x\"}"),
        same("textCrlf", EncodingType.TEXT, utf8("a\r\nb\r\n"), "{\"__raw__\":\"a\\r\\nb\\r\\n\"}"),
        same(
            "textUnicode", EncodingType.TEXT, utf8("héllo 世界 😀"), "{\"__raw__\":\"héllo 世界 😀\"}"),
        same("textSpacesOnly", EncodingType.TEXT, utf8("   "), "{\"__raw__\":\"   \"}"),
        same(
            "jsonLinesCrlf",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"a\":1}\r\n{\"a\":2}\r\n"),
            "{\"a\":1}",
            "{\"a\":2}"),
        same(
            "jsonLinesNested",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"a\":{\"b\":[1,2,{\"c\":null}]}}\n"),
            "{\"a\":{\"b\":[1,2,{\"c\":null}]}}"),
        same(
            "jsonLinesNumbers",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"i\":-12,\"big\":12345678901234567890,\"d\":3.25,\"e\":1e10,\"z\":0}\n"),
            "{\"big\":1.2345678901234567E19,\"d\":3.25,\"e\":1.0E10,\"i\":-12,\"z\":0}"),
        same(
            "jsonLinesBooleansNull",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"t\":true,\"f\":false,\"n\":null}\n"),
            "{\"t\":true,\"f\":false,\"n\":null}"),
        same(
            "jsonLinesUnicodeEscapes",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"s\":\"\\u00e9\\n\\t\\\"\"}\n"),
            "{\"s\":\"é\\n\\t\\\"\"}"),
        same("jsonLinesEmptyObject", EncodingType.JSON_OBJECT_LINE, utf8("{}\n"), "{}"),
        same(
            "jsonLinesBlankLines",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"a\":1}\n\n   \n{\"a\":2}\n"),
            "{\"a\":1}",
            "{\"a\":2}"),
        same(
            "jsonLinesNoTrailingNewline",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"a\":1}\n{\"a\":2}"),
            "{\"a\":1}",
            "{\"a\":2}"),
        same(
            "jsonLinesSpacesInside",
            EncodingType.JSON_OBJECT_LINE,
            utf8("  { \"a\" : 1 }  \n"),
            "{\"a\":1}"),
        same(
            "jsonLinesEmptyStringsAndArrays",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"s\":\"\",\"a\":[]}\n"),
            "{\"a\":[],\"s\":\"\"}"),
        same(
            "jsonObjectNested",
            EncodingType.JSON_OBJECT,
            utf8("{\"a\":{\"b\":{\"c\":[1,\"x\",true]}}}"),
            "{\"a\":{\"b\":{\"c\":[1,\"x\",true]}}}"),
        same(
            "jsonObjectWhitespace",
            EncodingType.JSON_OBJECT,
            utf8("\n  {\n  \"a\": 1,\n  \"b\": [ 1, 2 ]\n}\n"),
            "{\"a\":1,\"b\":[1,2]}"),
        same(
            "jsonObjectNumbers",
            EncodingType.JSON_OBJECT,
            utf8("{\"d\":0.1,\"neg\":-5,\"big\":98765432109876543210}"),
            "{\"neg\":-5,\"big\":9.876543210987654E19,\"d\":0.1}"),
        same(
            "jsonObjectUnicode",
            EncodingType.JSON_OBJECT,
            utf8("{\"名\":\"世界\"}"),
            "{\"名\":\"世界\"}"),
        same("jsonObjectEmpty", EncodingType.JSON_OBJECT, utf8("{}"), "{}"),
        same(
            "jsonArrayWhitespace",
            EncodingType.JSON_ARRAY,
            utf8("[\n  {\"a\":1},\n  {\"a\":2}\n]\n"),
            "{\"a\":1}",
            "{\"a\":2}"),
        same(
            "jsonArraySingle",
            EncodingType.JSON_ARRAY,
            utf8("[{\"a\":{\"b\":2}}]"),
            "{\"a\":{\"b\":2}}"),
        same("jsonArrayEmpty", EncodingType.JSON_ARRAY, utf8("[]")),
        same("jsonArrayEmptyObjects", EncodingType.JSON_ARRAY, utf8("[{},{}]"), "{}", "{}"),
        same(
            "xmlAttributes",
            EncodingType.XML,
            utf8("<r id=\"7\"><a k=\"v\">1</a></r>"),
            "{\"a\":{\"\":\"1\",\"k\":\"v\"},\"id\":\"7\"}"),
        same(
            "xmlRepeated",
            EncodingType.XML,
            utf8("<r><a>1</a><a>2</a><a>3</a></r>"),
            "{\"a\":[\"1\",\"2\",\"3\"]}"),
        same(
            "xmlNested",
            EncodingType.XML,
            utf8("<r><a><b><c>x</c></b></a></r>"),
            "{\"a\":{\"b\":{\"c\":\"x\"}}}"),
        same(
            "xmlCdata",
            EncodingType.XML,
            utf8("<r><a><![CDATA[<x>&]]></a></r>"),
            "{\"a\":\"<x>&\"}"),
        same(
            "xmlEntities",
            EncodingType.XML,
            utf8("<r><a>a &amp; b &lt; c</a></r>"),
            "{\"a\":\"a & b < c\"}"),
        same("xmlComment", EncodingType.XML, utf8("<r><!-- c --><a>1</a></r>"), "{\"a\":\"1\"}"),
        same(
            "xmlDeclaration",
            EncodingType.XML,
            utf8("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<r><a>1</a></r>\n"),
            "{\"a\":\"1\"}"),
        same(
            "xmlEmptyElement",
            EncodingType.XML,
            utf8("<r><a/><b></b></r>"),
            "{\"a\":\"\",\"b\":\"\"}"),
        same(
            "xmlMixedContent",
            EncodingType.XML,
            utf8("<r>text<a>1</a>more</r>"),
            "{\"\":[\"text\",\"more\"],\"a\":\"1\"}"),
        same(
            "xmlNamespace",
            EncodingType.XML,
            utf8("<r xmlns:p=\"urn:x\"><p:a>1</p:a></r>"),
            "{\"a\":\"1\"}"),
        same("xmlUnicode", EncodingType.XML, utf8("<r><名>世界</名></r>"), "{\"名\":\"世界\"}"),
        same(
            "gzipMultiMember",
            EncodingType.JSON_OBJECT_LINE,
            concat(gzip(utf8("{\"a\":1}\n")), gzip(utf8("{\"a\":2}\n"))),
            "{\"a\":1}",
            "{\"a\":2}"),
        // text and csv still replace bad bytes
        same(
            "stringLineInvalidUtf8",
            EncodingType.STRING_LINE,
            concat(utf8("a"), new byte[] {(byte) 0xff}, utf8("b\n")),
            "{\"__raw__\":\"a\uFFFDb\"}"),
        same(
            "textInvalidUtf8",
            EncodingType.TEXT,
            concat(utf8("a"), new byte[] {(byte) 0xff}, utf8("b")),
            "{\"__raw__\":\"a\uFFFDb\"}"),
        same(
            "csvInvalidUtf8",
            EncodingType.CSV,
            concat(utf8("a\nx"), new byte[] {(byte) 0xff}, utf8("y\n")),
            "{\"a\":\"x\uFFFDy\"}"),
        // bad data loses only itself
        legacy(
            "jsonLinesBadLine",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"a\":1}\nnot json\n{\"a\":3}\n"),
            List.of(FAILS),
            "ok {\"a\":1}",
            "bad MALFORMED not json",
            "ok {\"a\":3}"),
        legacy(
            "jsonLinesArrayLineFails",
            EncodingType.JSON_OBJECT_LINE,
            utf8("[{\"a\":1},{\"a\":2}]\n"),
            List.of("{\"a\":1}", "{\"a\":2}"),
            "bad MALFORMED [{\"a\":1},{\"a\":2}]"),
        legacy(
            "jsonLinesPrettyPrintedFailsPerLine",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\n  \"a\": 1\n}\n"),
            List.of(FAILS),
            "bad MALFORMED {",
            "bad MALFORMED   \"a\": 1",
            "bad MALFORMED }"),
        legacy(
            "jsonArrayMalformedElementEndsTheObject",
            EncodingType.JSON_ARRAY,
            utf8("[{\"a\":1},{\"a\" 2},{\"a\":3}]"),
            List.of(FAILS),
            "ok {\"a\":1}",
            "bad MALFORMED terminal "),
        legacy(
            "csvLongRowFails",
            EncodingType.CSV,
            utf8("a,b\n1,2,3\n"),
            List.of("{\"a\":\"1\",\"b\":\"2\"}"),
            "bad MALFORMED 1,2,3"),
        legacy(
            "csvShortRowFailsAlone",
            EncodingType.CSV,
            utf8("a,b\n1\n4,5\n"),
            List.of(FAILS),
            "bad MALFORMED 1",
            "ok {\"a\":\"4\",\"b\":\"5\"}"),
        legacy(
            "csvBadQuoteFailsAlone",
            EncodingType.CSV,
            utf8("a,b\n\"1\"x,2\n3,4\n"),
            List.of(FAILS),
            "bad MALFORMED \"1\"x,2",
            "ok {\"a\":\"3\",\"b\":\"4\"}"),
        legacy(
            "csvUnclosedQuoteEndsTheObject",
            EncodingType.CSV,
            utf8("a,b\n\"1,2\n"),
            List.of(FAILS),
            "bad MALFORMED terminal \"1,2\n"),
        legacy(
            "csvDuplicateHeaderEndsTheObject",
            EncodingType.CSV,
            utf8("a,a\n1,2\n"),
            List.of("{\"a\":\"2\"}"),
            "bad MALFORMED terminal a,a"),
        legacy(
            "csvEmptyHeaderIsNamed",
            EncodingType.CSV,
            utf8("a,\n1,2\n"),
            List.of(FAILS),
            "ok {\"a\":\"1\",\"_c1\":\"2\"}"),
        // byte order marks are removed
        legacy(
            "bomStringLine",
            EncodingType.STRING_LINE,
            utf8("﻿x\n"),
            List.of("{\"__raw__\":\"﻿x\"}"),
            "ok {\"__raw__\":\"x\"}"),
        legacy(
            "bomText",
            EncodingType.TEXT,
            utf8("﻿x"),
            List.of("{\"__raw__\":\"﻿x\"}"),
            "ok {\"__raw__\":\"x\"}"),
        legacy(
            "bomCsv",
            EncodingType.CSV,
            utf8("﻿a\n1\n"),
            List.of("{\"﻿a\":\"1\"}"),
            "ok {\"a\":\"1\"}"),
        legacy(
            "bomJsonLines",
            EncodingType.JSON_OBJECT_LINE,
            utf8("﻿{\"a\":1}\n"),
            List.of(FAILS),
            "ok {\"a\":1}"),
        // an empty object gives nothing
        legacy("emptyJsonObject", EncodingType.JSON_OBJECT, new byte[0], List.of(FAILS)),
        legacy("emptyJsonArray", EncodingType.JSON_ARRAY, new byte[0], List.of(FAILS)),
        legacy("emptyXml", EncodingType.XML, new byte[0], List.of(FAILS)),
        legacy("emptyText", EncodingType.TEXT, new byte[0], List.of("{\"__raw__\":\"\"}")),
        Arguments.of(
            "emptyForcedGzip",
            EncodingType.JSON_OBJECT_LINE,
            CompressionType.GZIP,
            new byte[0],
            HEAP,
            new Observed(List.of(FAILS), List.of())),
        // gzip trailing bytes
        legacy(
            "gzipTrailingBytes",
            EncodingType.JSON_OBJECT_LINE,
            concat(gzip(JSON_LINES), utf8("xx")),
            List.of("{\"a\":1}", "{\"a\":2}"),
            "ok {\"a\":1}",
            "ok {\"a\":2}",
            "bad CORRUPT_COMPRESSION terminal "),
        // xml
        legacy(
            "xmlDoctypeFails",
            EncodingType.XML,
            utf8("<!DOCTYPE r><r><a>1</a></r>"),
            List.of("{\"a\":\"1\"}"),
            "bad MALFORMED terminal <!DOCTYPE r><r><a>1</a></r>"),
        legacy(
            "xmlTextAfterTheRootFails",
            EncodingType.XML,
            utf8("<r><a>1</a></r>junk"),
            List.of("{\"a\":\"1\"}"),
            "bad MALFORMED terminal <r><a>1</a></r>junk"),
        legacy(
            "gzipOfEmptyTextGivesNothing",
            EncodingType.TEXT,
            gzip(new byte[0]),
            List.of("{\"__raw__\":\"\"}")),
        legacy(
            "xmlTrailingContentFails",
            EncodingType.XML,
            utf8("<r><a>1</a></r><s/>"),
            List.of("{\"a\":\"1\"}"),
            "bad MALFORMED terminal <r><a>1</a></r><s/>"),
        // json lines bytes and whitespace
        legacy(
            "jsonLinesInvalidUtf8Fails",
            EncodingType.JSON_OBJECT_LINE,
            concat(utf8("{\"a\":\""), new byte[] {(byte) 0xff}, utf8("\"}\n")),
            List.of("{\"a\":\"�\"}"),
            "bad MALFORMED {\"a\":\"�\"}"),
        legacy(
            "jsonLinesFormFeedLineFails",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"a\":1}\n\f\n{\"a\":2}\n"),
            List.of("{\"a\":1}", "{\"a\":2}"),
            "ok {\"a\":1}",
            "bad MALFORMED \f",
            "ok {\"a\":2}"),
        legacy(
            "jsonLinesUtf16Decodes",
            EncodingType.JSON_OBJECT_LINE,
            concat(
                new byte[] {(byte) 0xFE, (byte) 0xFF},
                "{\"a\":1}\n".getBytes(StandardCharsets.UTF_16BE)),
            List.of(FAILS),
            "ok {\"a\":1}"),
        legacy(
            "jsonLinesUtf32Decodes",
            EncodingType.JSON_OBJECT_LINE,
            concat(
                new byte[] {0, 0, (byte) 0xFE, (byte) 0xFF},
                "{\"a\":1}\n".getBytes(Charset.forName("UTF-32BE"))),
            List.of(FAILS),
            "ok {\"a\":1}"),
        // limits are new: at a 1 MiB heap a record may be 8 KiB and a document 16 KiB
        Arguments.of(
            "aRecordOverTheLimitIsOversized",
            EncodingType.STRING_LINE,
            null,
            utf8("x".repeat(10_000) + "\nok\n"),
            MIB,
            new Observed(
                List.of("{\"__raw__\":\"" + "x".repeat(10_000) + "\"}", "{\"__raw__\":\"ok\"}"),
                List.of("big #1", "ok {\"__raw__\":\"ok\"}"))),
        Arguments.of(
            "aDocumentOverTheLimitIsOversized",
            EncodingType.JSON_OBJECT,
            null,
            utf8("{\"a\":\"" + "x".repeat(20_000) + "\"}"),
            MIB,
            new Observed(List.of("{\"a\":\"" + "x".repeat(20_000) + "\"}"), List.of("big #-1"))));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void legacyAndDecoderResults(
      String name,
      EncodingType encodingType,
      CompressionType compressionType,
      byte[] bytes,
      long heap,
      Observed expected)
      throws SourceReadException {
    assertEquals(
        expected,
        new Observed(
            legacy(encodingType, compressionType, bytes),
            decoded(encodingType, compressionType, bytes, heap)));
  }

  @Test
  void aTruncatedGzipEmitsTheRecordsBeforeTheCutWhereLegacyFailed() throws Exception {
    List<String> lines =
        IntStream.range(0, 3000).mapToObj(i -> "{\"a\":" + i * 7919 % 100003 + "}").toList();
    byte[] whole = gzip(utf8(String.join("\n", lines) + "\n"));
    byte[] truncated = Arrays.copyOf(whole, whole.length / 2);

    List<String> decoded = decoded(EncodingType.JSON_OBJECT_LINE, null, truncated, HEAP);

    assertEquals(List.of(FAILS), legacy(EncodingType.JSON_OBJECT_LINE, null, truncated));
    int emitted = decoded.size() - 1;
    assertTrue(emitted > 0, "records before the cut: " + emitted);
    List<String> expected =
        new ArrayList<>(lines.subList(0, emitted).stream().map(l -> "ok " + l).toList());
    expected.add("bad CORRUPT_COMPRESSION terminal ");
    assertEquals(expected, decoded);
  }

  private static Arguments same(
      String name, EncodingType encodingType, byte[] bytes, String... records) {
    return Arguments.of(
        name,
        encodingType,
        null,
        bytes,
        HEAP,
        new Observed(List.of(records), Arrays.stream(records).map(r -> "ok " + r).toList()));
  }

  private static Arguments legacy(
      String name,
      EncodingType encodingType,
      byte[] bytes,
      List<String> legacy,
      String... decoded) {
    return Arguments.of(
        name, encodingType, null, bytes, HEAP, new Observed(legacy, List.of(decoded)));
  }

  /** The legacy converter's path: gzip by magic bytes or forced, then the legacy deserializer. */
  private static List<String> legacy(
      EncodingType encodingType, CompressionType compressionType, byte[] bytes) {
    try {
      byte[] body = bytes;
      boolean gzip =
          compressionType == CompressionType.GZIP
              || (compressionType == null
                  && body.length >= 2
                  && (body[0] & 0xFF) == 0x1F
                  && (body[1] & 0xFF) == 0x8B);
      if (gzip) {
        body = CompressionUtils.gunzip(body);
      }
      List<RecordFleakData> records =
          DeserializerFactory.createDeserializerFactory(encodingType)
              .createDeserializer()
              .deserialize(new SerializedEvent(null, body, null));
      return records.stream().map(r -> JsonUtils.toJsonString(r.unwrap())).toList();
    } catch (Exception e) {
      return List.of(FAILS);
    }
  }

  private List<String> decoded(
      EncodingType encodingType, CompressionType compressionType, byte[] bytes, long heap)
      throws SourceReadException {
    S3Client s3 = mock(S3Client.class);
    when(s3.getObject(any(GetObjectRequest.class)))
        .thenReturn(
            new ResponseInputStream<>(
                GetObjectResponse.builder().contentLength((long) bytes.length).build(),
                AbortableInputStream.create(
                    new ByteArrayInputStream(bytes), mock(Abortable.class))));
    List<String> seen = new ArrayList<>();
    new S3ObjectDecoder(new S3Decoding(encodingType, compressionType, null, false), heap)
        .decode(
            s3,
            new S3ObjectRef("b", "k", "us-east-1"),
            Long.MAX_VALUE,
            spoolDir,
            outcome -> {
              seen.add(view(outcome));
              return true;
            });
    return seen;
  }

  private static String view(DecodeOutcome outcome) {
    return switch (outcome) {
      case DecodeOutcome.Decoded d -> "ok " + JsonUtils.toJsonString(d.record().unwrap());
      case DecodeOutcome.Failed f ->
          "bad "
              + f.kind()
              + (f.terminal() ? " terminal " : " ")
              + new String(f.raw(), StandardCharsets.UTF_8);
      case DecodeOutcome.Oversized o -> "big #" + o.recordIndex();
    };
  }

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] gzip(byte[] bytes) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
      gzip.write(bytes);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return out.toByteArray();
  }

  private static byte[] concat(byte[]... parts) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    for (byte[] part : parts) {
      out.writeBytes(part);
    }
    return out.toByteArray();
  }
}
