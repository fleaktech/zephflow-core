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

import io.fleak.zephflow.lib.serdes.CompressionType;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.SourceReadException;
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
 * Feeds each object's bytes to {@link S3ObjectDecoder} with the {@code encodingType} mapping and
 * pins every outcome: the records for valid input, and each case of the spec's section 4.2.
 */
class S3RealtimeDecodingCasesTest {

  private static final long MIB = 1024L * 1024;
  private static final long HEAP = 2048 * MIB;
  private static final byte[] JSON_LINES = utf8("{\"a\":1}\n{\"a\":2}\n");

  @TempDir Path spoolDir;

  static Stream<Arguments> cases() throws IOException {
    return Stream.of(
        // valid input decodes to its records
        decodes(
            "jsonObject",
            EncodingType.JSON_OBJECT,
            utf8("{\"a\":1,\"b\":\"x\"}"),
            "{\"a\":1,\"b\":\"x\"}"),
        decodes(
            "jsonArray",
            EncodingType.JSON_ARRAY,
            utf8("[{\"a\":1},{\"a\":2}]"),
            "{\"a\":1}",
            "{\"a\":2}"),
        decodes("jsonLines", EncodingType.JSON_OBJECT_LINE, JSON_LINES, "{\"a\":1}", "{\"a\":2}"),
        decodes(
            "stringLineKeepsBlankLines",
            EncodingType.STRING_LINE,
            utf8("x\n\ny\n"),
            "{\"__raw__\":\"x\"}",
            "{\"__raw__\":\"\"}",
            "{\"__raw__\":\"y\"}"),
        decodes("text", EncodingType.TEXT, utf8("hello\nworld"), "{\"__raw__\":\"hello\\nworld\"}"),
        decodes(
            "csvTrimsSpaces",
            EncodingType.CSV,
            utf8("name, age\n bob , 3\n"),
            "{\"name\":\"bob\",\"age\":\"3\"}"),
        decodes("xml", EncodingType.XML, utf8("<r><a>1</a></r>"), "{\"a\":\"1\"}"),
        decodes(
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
            List.of("ok {\"a\":1}", "ok {\"a\":2}")),
        decodes("emptyJsonLines", EncodingType.JSON_OBJECT_LINE, new byte[0]),
        decodes(
            "csvQuotedComma",
            EncodingType.CSV,
            utf8("a,b\n\"x,y\",2\n"),
            "{\"a\":\"x,y\",\"b\":\"2\"}"),
        decodes(
            "csvEscapedQuote",
            EncodingType.CSV,
            utf8("a,b\n\"say \"\"hi\"\"\",2\n"),
            "{\"a\":\"say \\\"hi\\\"\",\"b\":\"2\"}"),
        decodes(
            "csvEmptyValues",
            EncodingType.CSV,
            utf8("a,b,c\n,2,\n"),
            "{\"a\":\"\",\"b\":\"2\",\"c\":\"\"}"),
        decodes(
            "csvCrlf",
            EncodingType.CSV,
            utf8("a,b\r\n1,2\r\n3,4\r\n"),
            "{\"a\":\"1\",\"b\":\"2\"}",
            "{\"a\":\"3\",\"b\":\"4\"}"),
        decodes(
            "csvEmbeddedNewline",
            EncodingType.CSV,
            utf8("a,b\n\"line1\nline2\",2\n"),
            "{\"a\":\"line1\\nline2\",\"b\":\"2\"}"),
        decodes(
            "csvNoTrailingNewline",
            EncodingType.CSV,
            utf8("a,b\n1,2"),
            "{\"a\":\"1\",\"b\":\"2\"}"),
        decodes(
            "csvUnicode", EncodingType.CSV, utf8("名前,年齢\n太郎,3\n"), "{\"年齢\":\"3\",\"名前\":\"太郎\"}"),
        decodes("csvHeaderOnly", EncodingType.CSV, utf8("a,b\n")),
        decodes(
            "csvQuotedSpaces",
            EncodingType.CSV,
            utf8("a,b\n\" x \",2\n"),
            "{\"a\":\" x \",\"b\":\"2\"}"),
        decodes(
            "stringLineCrlf",
            EncodingType.STRING_LINE,
            utf8("x\r\ny\r\n"),
            "{\"__raw__\":\"x\"}",
            "{\"__raw__\":\"y\"}"),
        decodes(
            "stringLineCr",
            EncodingType.STRING_LINE,
            utf8("x\ry\r"),
            "{\"__raw__\":\"x\"}",
            "{\"__raw__\":\"y\"}"),
        decodes(
            "stringLineNoTrailingNewline",
            EncodingType.STRING_LINE,
            utf8("x\ny"),
            "{\"__raw__\":\"x\"}",
            "{\"__raw__\":\"y\"}"),
        decodes(
            "stringLineUnicode",
            EncodingType.STRING_LINE,
            utf8("héllo 世界 😀\n"),
            "{\"__raw__\":\"héllo 世界 😀\"}"),
        decodes(
            "stringLineSpaces",
            EncodingType.STRING_LINE,
            utf8("  x  \n\t\n"),
            "{\"__raw__\":\"  x  \"}",
            "{\"__raw__\":\"\\t\"}"),
        decodes(
            "stringLineManyBlank",
            EncodingType.STRING_LINE,
            utf8("\n\n\nx\n"),
            "{\"__raw__\":\"\"}",
            "{\"__raw__\":\"\"}",
            "{\"__raw__\":\"\"}",
            "{\"__raw__\":\"x\"}"),
        decodes(
            "textCrlf", EncodingType.TEXT, utf8("a\r\nb\r\n"), "{\"__raw__\":\"a\\r\\nb\\r\\n\"}"),
        decodes(
            "textUnicode", EncodingType.TEXT, utf8("héllo 世界 😀"), "{\"__raw__\":\"héllo 世界 😀\"}"),
        decodes("textSpacesOnly", EncodingType.TEXT, utf8("   "), "{\"__raw__\":\"   \"}"),
        decodes(
            "jsonLinesCrlf",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"a\":1}\r\n{\"a\":2}\r\n"),
            "{\"a\":1}",
            "{\"a\":2}"),
        decodes(
            "jsonLinesNested",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"a\":{\"b\":[1,2,{\"c\":null}]}}\n"),
            "{\"a\":{\"b\":[1,2,{\"c\":null}]}}"),
        decodes(
            "jsonLinesNumbers",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"i\":-12,\"big\":12345678901234567890,\"d\":3.25,\"e\":1e10,\"z\":0}\n"),
            "{\"big\":1.2345678901234567E19,\"d\":3.25,\"e\":1.0E10,\"i\":-12,\"z\":0}"),
        decodes(
            "jsonLinesBooleansNull",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"t\":true,\"f\":false,\"n\":null}\n"),
            "{\"t\":true,\"f\":false,\"n\":null}"),
        decodes(
            "jsonLinesUnicodeEscapes",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"s\":\"\\u00e9\\n\\t\\\"\"}\n"),
            "{\"s\":\"é\\n\\t\\\"\"}"),
        decodes("jsonLinesEmptyObject", EncodingType.JSON_OBJECT_LINE, utf8("{}\n"), "{}"),
        decodes(
            "jsonLinesBlankLines",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"a\":1}\n\n   \n{\"a\":2}\n"),
            "{\"a\":1}",
            "{\"a\":2}"),
        decodes(
            "jsonLinesNoTrailingNewline",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"a\":1}\n{\"a\":2}"),
            "{\"a\":1}",
            "{\"a\":2}"),
        decodes(
            "jsonLinesSpacesInside",
            EncodingType.JSON_OBJECT_LINE,
            utf8("  { \"a\" : 1 }  \n"),
            "{\"a\":1}"),
        decodes(
            "jsonLinesEmptyStringsAndArrays",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"s\":\"\",\"a\":[]}\n"),
            "{\"a\":[],\"s\":\"\"}"),
        decodes(
            "jsonObjectNested",
            EncodingType.JSON_OBJECT,
            utf8("{\"a\":{\"b\":{\"c\":[1,\"x\",true]}}}"),
            "{\"a\":{\"b\":{\"c\":[1,\"x\",true]}}}"),
        decodes(
            "jsonObjectWhitespace",
            EncodingType.JSON_OBJECT,
            utf8("\n  {\n  \"a\": 1,\n  \"b\": [ 1, 2 ]\n}\n"),
            "{\"a\":1,\"b\":[1,2]}"),
        decodes(
            "jsonObjectNumbers",
            EncodingType.JSON_OBJECT,
            utf8("{\"d\":0.1,\"neg\":-5,\"big\":98765432109876543210}"),
            "{\"neg\":-5,\"big\":9.876543210987654E19,\"d\":0.1}"),
        decodes(
            "jsonObjectUnicode",
            EncodingType.JSON_OBJECT,
            utf8("{\"名\":\"世界\"}"),
            "{\"名\":\"世界\"}"),
        decodes("jsonObjectEmpty", EncodingType.JSON_OBJECT, utf8("{}"), "{}"),
        decodes(
            "jsonArrayWhitespace",
            EncodingType.JSON_ARRAY,
            utf8("[\n  {\"a\":1},\n  {\"a\":2}\n]\n"),
            "{\"a\":1}",
            "{\"a\":2}"),
        decodes(
            "jsonArraySingle",
            EncodingType.JSON_ARRAY,
            utf8("[{\"a\":{\"b\":2}}]"),
            "{\"a\":{\"b\":2}}"),
        decodes("jsonArrayEmpty", EncodingType.JSON_ARRAY, utf8("[]")),
        decodes("jsonArrayEmptyObjects", EncodingType.JSON_ARRAY, utf8("[{},{}]"), "{}", "{}"),
        decodes(
            "xmlAttributes",
            EncodingType.XML,
            utf8("<r id=\"7\"><a k=\"v\">1</a></r>"),
            "{\"a\":{\"\":\"1\",\"k\":\"v\"},\"id\":\"7\"}"),
        decodes(
            "xmlRepeated",
            EncodingType.XML,
            utf8("<r><a>1</a><a>2</a><a>3</a></r>"),
            "{\"a\":[\"1\",\"2\",\"3\"]}"),
        decodes(
            "xmlNested",
            EncodingType.XML,
            utf8("<r><a><b><c>x</c></b></a></r>"),
            "{\"a\":{\"b\":{\"c\":\"x\"}}}"),
        decodes(
            "xmlCdata",
            EncodingType.XML,
            utf8("<r><a><![CDATA[<x>&]]></a></r>"),
            "{\"a\":\"<x>&\"}"),
        decodes(
            "xmlEntities",
            EncodingType.XML,
            utf8("<r><a>a &amp; b &lt; c</a></r>"),
            "{\"a\":\"a & b < c\"}"),
        decodes("xmlComment", EncodingType.XML, utf8("<r><!-- c --><a>1</a></r>"), "{\"a\":\"1\"}"),
        decodes(
            "xmlDeclaration",
            EncodingType.XML,
            utf8("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<r><a>1</a></r>\n"),
            "{\"a\":\"1\"}"),
        decodes(
            "xmlEmptyElement",
            EncodingType.XML,
            utf8("<r><a/><b></b></r>"),
            "{\"a\":\"\",\"b\":\"\"}"),
        decodes(
            "xmlMixedContent",
            EncodingType.XML,
            utf8("<r>text<a>1</a>more</r>"),
            "{\"\":[\"text\",\"more\"],\"a\":\"1\"}"),
        decodes(
            "xmlNamespace",
            EncodingType.XML,
            utf8("<r xmlns:p=\"urn:x\"><p:a>1</p:a></r>"),
            "{\"a\":\"1\"}"),
        decodes("xmlUnicode", EncodingType.XML, utf8("<r><名>世界</名></r>"), "{\"名\":\"世界\"}"),
        decodes(
            "gzipMultiMember",
            EncodingType.JSON_OBJECT_LINE,
            concat(gzip(utf8("{\"a\":1}\n")), gzip(utf8("{\"a\":2}\n"))),
            "{\"a\":1}",
            "{\"a\":2}"),
        // text and csv replace bad bytes
        decodes(
            "stringLineInvalidUtf8",
            EncodingType.STRING_LINE,
            concat(utf8("a"), new byte[] {(byte) 0xff}, utf8("b\n")),
            "{\"__raw__\":\"a\uFFFDb\"}"),
        decodes(
            "textInvalidUtf8",
            EncodingType.TEXT,
            concat(utf8("a"), new byte[] {(byte) 0xff}, utf8("b")),
            "{\"__raw__\":\"a\uFFFDb\"}"),
        decodes(
            "csvInvalidUtf8",
            EncodingType.CSV,
            concat(utf8("a\nx"), new byte[] {(byte) 0xff}, utf8("y\n")),
            "{\"a\":\"x\uFFFDy\"}"),
        // bad data loses only itself
        outcomes(
            "jsonLinesBadLine",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"a\":1}\nnot json\n{\"a\":3}\n"),
            "ok {\"a\":1}",
            "bad MALFORMED not json",
            "ok {\"a\":3}"),
        outcomes(
            "jsonLinesArrayLineFails",
            EncodingType.JSON_OBJECT_LINE,
            utf8("[{\"a\":1},{\"a\":2}]\n"),
            "bad MALFORMED [{\"a\":1},{\"a\":2}]"),
        outcomes(
            "jsonLinesPrettyPrintedFailsPerLine",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\n  \"a\": 1\n}\n"),
            "bad MALFORMED {",
            "bad MALFORMED   \"a\": 1",
            "bad MALFORMED }"),
        outcomes(
            "jsonArrayMalformedElementEndsTheObject",
            EncodingType.JSON_ARRAY,
            utf8("[{\"a\":1},{\"a\" 2},{\"a\":3}]"),
            "ok {\"a\":1}",
            "bad MALFORMED terminal "),
        outcomes("csvLongRowFails", EncodingType.CSV, utf8("a,b\n1,2,3\n"), "bad MALFORMED 1,2,3"),
        outcomes(
            "csvShortRowFailsAlone",
            EncodingType.CSV,
            utf8("a,b\n1\n4,5\n"),
            "bad MALFORMED 1",
            "ok {\"a\":\"4\",\"b\":\"5\"}"),
        outcomes(
            "csvBadQuoteFailsAlone",
            EncodingType.CSV,
            utf8("a,b\n\"1\"x,2\n3,4\n"),
            "bad MALFORMED \"1\"x,2",
            "ok {\"a\":\"3\",\"b\":\"4\"}"),
        outcomes(
            "csvUnclosedQuoteEndsTheObject",
            EncodingType.CSV,
            utf8("a,b\n\"1,2\n"),
            "bad MALFORMED terminal \"1,2\n"),
        outcomes(
            "csvDuplicateHeaderEndsTheObject",
            EncodingType.CSV,
            utf8("a,a\n1,2\n"),
            "bad MALFORMED terminal a,a"),
        outcomes(
            "csvEmptyHeaderIsNamed",
            EncodingType.CSV,
            utf8("a,\n1,2\n"),
            "ok {\"a\":\"1\",\"_c1\":\"2\"}"),
        // byte order marks are removed
        outcomes("bomStringLine", EncodingType.STRING_LINE, utf8("﻿x\n"), "ok {\"__raw__\":\"x\"}"),
        outcomes("bomText", EncodingType.TEXT, utf8("﻿x"), "ok {\"__raw__\":\"x\"}"),
        outcomes("bomCsv", EncodingType.CSV, utf8("﻿a\n1\n"), "ok {\"a\":\"1\"}"),
        outcomes(
            "bomJsonLines", EncodingType.JSON_OBJECT_LINE, utf8("﻿{\"a\":1}\n"), "ok {\"a\":1}"),
        // an empty object gives nothing
        outcomes("emptyJsonObject", EncodingType.JSON_OBJECT, new byte[0]),
        outcomes("emptyJsonArray", EncodingType.JSON_ARRAY, new byte[0]),
        outcomes("emptyXml", EncodingType.XML, new byte[0]),
        outcomes("emptyText", EncodingType.TEXT, new byte[0]),
        Arguments.of(
            "emptyForcedGzip",
            EncodingType.JSON_OBJECT_LINE,
            CompressionType.GZIP,
            new byte[0],
            HEAP,
            List.of()),
        // gzip trailing bytes
        outcomes(
            "gzipTrailingBytes",
            EncodingType.JSON_OBJECT_LINE,
            concat(gzip(JSON_LINES), utf8("xx")),
            "ok {\"a\":1}",
            "ok {\"a\":2}",
            "bad CORRUPT_COMPRESSION terminal "),
        // xml
        outcomes(
            "xmlDoctypeFails",
            EncodingType.XML,
            utf8("<!DOCTYPE r><r><a>1</a></r>"),
            "bad MALFORMED terminal <!DOCTYPE r><r><a>1</a></r>"),
        outcomes(
            "xmlTextAfterTheRootFails",
            EncodingType.XML,
            utf8("<r><a>1</a></r>junk"),
            "bad MALFORMED terminal <r><a>1</a></r>junk"),
        outcomes("gzipOfEmptyTextGivesNothing", EncodingType.TEXT, gzip(new byte[0])),
        outcomes(
            "xmlTrailingContentFails",
            EncodingType.XML,
            utf8("<r><a>1</a></r><s/>"),
            "bad MALFORMED terminal <r><a>1</a></r><s/>"),
        // json lines bytes and whitespace
        outcomes(
            "jsonLinesInvalidUtf8Fails",
            EncodingType.JSON_OBJECT_LINE,
            concat(utf8("{\"a\":\""), new byte[] {(byte) 0xff}, utf8("\"}\n")),
            "bad MALFORMED {\"a\":\"�\"}"),
        outcomes(
            "jsonLinesFormFeedLineFails",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"a\":1}\n\f\n{\"a\":2}\n"),
            "ok {\"a\":1}",
            "bad MALFORMED \f",
            "ok {\"a\":2}"),
        outcomes(
            "jsonLinesUtf16Decodes",
            EncodingType.JSON_OBJECT_LINE,
            concat(
                new byte[] {(byte) 0xFE, (byte) 0xFF},
                "{\"a\":1}\n".getBytes(StandardCharsets.UTF_16BE)),
            "ok {\"a\":1}"),
        outcomes(
            "jsonLinesUtf32Decodes",
            EncodingType.JSON_OBJECT_LINE,
            concat(
                new byte[] {0, 0, (byte) 0xFE, (byte) 0xFF},
                "{\"a\":1}\n".getBytes(Charset.forName("UTF-32BE"))),
            "ok {\"a\":1}"),
        // limits: at a 1 MiB heap a record may be 8 KiB and a document 16 KiB
        Arguments.of(
            "aRecordOverTheLimitIsOversized",
            EncodingType.STRING_LINE,
            null,
            utf8("x".repeat(10_000) + "\nok\n"),
            MIB,
            List.of("big #1", "ok {\"__raw__\":\"ok\"}")),
        Arguments.of(
            "aDocumentOverTheLimitIsOversized",
            EncodingType.JSON_OBJECT,
            null,
            utf8("{\"a\":\"" + "x".repeat(20_000) + "\"}"),
            MIB,
            List.of("big #-1")));
  }

  /**
   * Each outcome: {@code ok <json>}, {@code bad <kind>[ terminal] <raw>}, or {@code big #<index>}.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void decodesEachObjectToItsOutcomes(
      String name,
      EncodingType encodingType,
      CompressionType compressionType,
      byte[] bytes,
      long heap,
      List<String> expected)
      throws SourceReadException {
    assertEquals(expected, decoded(encodingType, compressionType, bytes, heap));
  }

  @Test
  void aTruncatedGzipEmitsTheRecordsBeforeTheCut() throws Exception {
    List<String> lines =
        IntStream.range(0, 3000).mapToObj(i -> "{\"a\":" + i * 7919 % 100003 + "}").toList();
    byte[] whole = gzip(utf8(String.join("\n", lines) + "\n"));
    byte[] truncated = Arrays.copyOf(whole, whole.length / 2);

    List<String> decoded = decoded(EncodingType.JSON_OBJECT_LINE, null, truncated, HEAP);

    int emitted = decoded.size() - 1;
    assertTrue(emitted > 0, "records before the cut: " + emitted);
    List<String> expected =
        new ArrayList<>(lines.subList(0, emitted).stream().map(l -> "ok " + l).toList());
    expected.add("bad CORRUPT_COMPRESSION terminal ");
    assertEquals(expected, decoded);
  }

  private static Arguments decodes(
      String name, EncodingType encodingType, byte[] bytes, String... records) {
    return outcomes(
        name,
        encodingType,
        bytes,
        Arrays.stream(records).map(r -> "ok " + r).toArray(String[]::new));
  }

  private static Arguments outcomes(
      String name, EncodingType encodingType, byte[] bytes, String... outcomes) {
    return Arguments.of(name, encodingType, null, bytes, HEAP, List.of(outcomes));
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
