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

import io.fleak.zephflow.lib.commands.filedecode.Decodings;
import io.fleak.zephflow.lib.serdes.decode.DecodeOutcome;
import io.fleak.zephflow.lib.serdes.decode.SourceReadException;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.config.FormatConfig;
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
 * Feeds each object's bytes to {@link S3ObjectDecoder} with the format's decoding (gzip detected
 * automatically unless set) and pins every outcome: the records for valid input, and each case of
 * the spec's section 4.2.
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
            Decodings.JSON_OBJECT,
            utf8("{\"a\":1,\"b\":\"x\"}"),
            "{\"a\":1,\"b\":\"x\"}"),
        decodes(
            "jsonArray",
            Decodings.JSON_ARRAY,
            utf8("[{\"a\":1},{\"a\":2}]"),
            "{\"a\":1}",
            "{\"a\":2}"),
        decodes("jsonLines", Decodings.JSON_LINES, JSON_LINES, "{\"a\":1}", "{\"a\":2}"),
        decodes(
            "stringLineKeepsBlankLines",
            Decodings.TEXT_LINE,
            utf8("x\n\ny\n"),
            "{\"__raw__\":\"x\"}",
            "{\"__raw__\":\"\"}",
            "{\"__raw__\":\"y\"}"),
        decodes("text", Decodings.TEXT, utf8("hello\nworld"), "{\"__raw__\":\"hello\\nworld\"}"),
        decodes(
            "csvTrimsSpaces",
            Decodings.CSV,
            utf8("name, age\n bob , 3\n"),
            "{\"name\":\"bob\",\"age\":\"3\"}"),
        decodes("xml", Decodings.XML, utf8("<r><a>1</a></r>"), "{\"a\":\"1\"}"),
        decodes("gzipDetected", Decodings.JSON_LINES, gzip(JSON_LINES), "{\"a\":1}", "{\"a\":2}"),
        Arguments.of(
            "gzipForced",
            Decodings.JSON_LINES,
            DecompressionCodec.GZIP,
            gzip(JSON_LINES),
            HEAP,
            List.of("ok {\"a\":1}", "ok {\"a\":2}")),
        decodes("emptyJsonLines", Decodings.JSON_LINES, new byte[0]),
        decodes(
            "csvQuotedComma",
            Decodings.CSV,
            utf8("a,b\n\"x,y\",2\n"),
            "{\"a\":\"x,y\",\"b\":\"2\"}"),
        decodes(
            "csvEscapedQuote",
            Decodings.CSV,
            utf8("a,b\n\"say \"\"hi\"\"\",2\n"),
            "{\"a\":\"say \\\"hi\\\"\",\"b\":\"2\"}"),
        decodes(
            "csvEmptyValues",
            Decodings.CSV,
            utf8("a,b,c\n,2,\n"),
            "{\"a\":\"\",\"b\":\"2\",\"c\":\"\"}"),
        decodes(
            "csvCrlf",
            Decodings.CSV,
            utf8("a,b\r\n1,2\r\n3,4\r\n"),
            "{\"a\":\"1\",\"b\":\"2\"}",
            "{\"a\":\"3\",\"b\":\"4\"}"),
        decodes(
            "csvEmbeddedNewline",
            Decodings.CSV,
            utf8("a,b\n\"line1\nline2\",2\n"),
            "{\"a\":\"line1\\nline2\",\"b\":\"2\"}"),
        decodes(
            "csvNoTrailingNewline", Decodings.CSV, utf8("a,b\n1,2"), "{\"a\":\"1\",\"b\":\"2\"}"),
        decodes("csvUnicode", Decodings.CSV, utf8("名前,年齢\n太郎,3\n"), "{\"年齢\":\"3\",\"名前\":\"太郎\"}"),
        decodes("csvHeaderOnly", Decodings.CSV, utf8("a,b\n")),
        decodes(
            "csvQuotedSpaces",
            Decodings.CSV,
            utf8("a,b\n\" x \",2\n"),
            "{\"a\":\" x \",\"b\":\"2\"}"),
        decodes(
            "stringLineCrlf",
            Decodings.TEXT_LINE,
            utf8("x\r\ny\r\n"),
            "{\"__raw__\":\"x\"}",
            "{\"__raw__\":\"y\"}"),
        decodes(
            "stringLineCr",
            Decodings.TEXT_LINE,
            utf8("x\ry\r"),
            "{\"__raw__\":\"x\"}",
            "{\"__raw__\":\"y\"}"),
        decodes(
            "stringLineNoTrailingNewline",
            Decodings.TEXT_LINE,
            utf8("x\ny"),
            "{\"__raw__\":\"x\"}",
            "{\"__raw__\":\"y\"}"),
        decodes(
            "stringLineUnicode",
            Decodings.TEXT_LINE,
            utf8("héllo 世界 😀\n"),
            "{\"__raw__\":\"héllo 世界 😀\"}"),
        decodes(
            "stringLineSpaces",
            Decodings.TEXT_LINE,
            utf8("  x  \n\t\n"),
            "{\"__raw__\":\"  x  \"}",
            "{\"__raw__\":\"\\t\"}"),
        decodes(
            "stringLineManyBlank",
            Decodings.TEXT_LINE,
            utf8("\n\n\nx\n"),
            "{\"__raw__\":\"\"}",
            "{\"__raw__\":\"\"}",
            "{\"__raw__\":\"\"}",
            "{\"__raw__\":\"x\"}"),
        decodes("textCrlf", Decodings.TEXT, utf8("a\r\nb\r\n"), "{\"__raw__\":\"a\\r\\nb\\r\\n\"}"),
        decodes(
            "textUnicode", Decodings.TEXT, utf8("héllo 世界 😀"), "{\"__raw__\":\"héllo 世界 😀\"}"),
        decodes("textSpacesOnly", Decodings.TEXT, utf8("   "), "{\"__raw__\":\"   \"}"),
        decodes(
            "jsonLinesCrlf",
            Decodings.JSON_LINES,
            utf8("{\"a\":1}\r\n{\"a\":2}\r\n"),
            "{\"a\":1}",
            "{\"a\":2}"),
        decodes(
            "jsonLinesNested",
            Decodings.JSON_LINES,
            utf8("{\"a\":{\"b\":[1,2,{\"c\":null}]}}\n"),
            "{\"a\":{\"b\":[1,2,{\"c\":null}]}}"),
        decodes(
            "jsonLinesNumbers",
            Decodings.JSON_LINES,
            utf8("{\"i\":-12,\"big\":12345678901234567890,\"d\":3.25,\"e\":1e10,\"z\":0}\n"),
            "{\"big\":1.2345678901234567E19,\"d\":3.25,\"e\":1.0E10,\"i\":-12,\"z\":0}"),
        decodes(
            "jsonLinesBooleansNull",
            Decodings.JSON_LINES,
            utf8("{\"t\":true,\"f\":false,\"n\":null}\n"),
            "{\"t\":true,\"f\":false,\"n\":null}"),
        decodes(
            "jsonLinesUnicodeEscapes",
            Decodings.JSON_LINES,
            utf8("{\"s\":\"\\u00e9\\n\\t\\\"\"}\n"),
            "{\"s\":\"é\\n\\t\\\"\"}"),
        decodes("jsonLinesEmptyObject", Decodings.JSON_LINES, utf8("{}\n"), "{}"),
        decodes(
            "jsonLinesBlankLines",
            Decodings.JSON_LINES,
            utf8("{\"a\":1}\n\n   \n{\"a\":2}\n"),
            "{\"a\":1}",
            "{\"a\":2}"),
        decodes(
            "jsonLinesNoTrailingNewline",
            Decodings.JSON_LINES,
            utf8("{\"a\":1}\n{\"a\":2}"),
            "{\"a\":1}",
            "{\"a\":2}"),
        decodes(
            "jsonLinesSpacesInside",
            Decodings.JSON_LINES,
            utf8("  { \"a\" : 1 }  \n"),
            "{\"a\":1}"),
        decodes(
            "jsonLinesEmptyStringsAndArrays",
            Decodings.JSON_LINES,
            utf8("{\"s\":\"\",\"a\":[]}\n"),
            "{\"a\":[],\"s\":\"\"}"),
        decodes(
            "jsonObjectNested",
            Decodings.JSON_OBJECT,
            utf8("{\"a\":{\"b\":{\"c\":[1,\"x\",true]}}}"),
            "{\"a\":{\"b\":{\"c\":[1,\"x\",true]}}}"),
        decodes(
            "jsonObjectWhitespace",
            Decodings.JSON_OBJECT,
            utf8("\n  {\n  \"a\": 1,\n  \"b\": [ 1, 2 ]\n}\n"),
            "{\"a\":1,\"b\":[1,2]}"),
        decodes(
            "jsonObjectNumbers",
            Decodings.JSON_OBJECT,
            utf8("{\"d\":0.1,\"neg\":-5,\"big\":98765432109876543210}"),
            "{\"neg\":-5,\"big\":9.876543210987654E19,\"d\":0.1}"),
        decodes(
            "jsonObjectUnicode", Decodings.JSON_OBJECT, utf8("{\"名\":\"世界\"}"), "{\"名\":\"世界\"}"),
        decodes("jsonObjectEmpty", Decodings.JSON_OBJECT, utf8("{}"), "{}"),
        decodes(
            "jsonArrayWhitespace",
            Decodings.JSON_ARRAY,
            utf8("[\n  {\"a\":1},\n  {\"a\":2}\n]\n"),
            "{\"a\":1}",
            "{\"a\":2}"),
        decodes(
            "jsonArraySingle",
            Decodings.JSON_ARRAY,
            utf8("[{\"a\":{\"b\":2}}]"),
            "{\"a\":{\"b\":2}}"),
        decodes("jsonArrayEmpty", Decodings.JSON_ARRAY, utf8("[]")),
        decodes("jsonArrayEmptyObjects", Decodings.JSON_ARRAY, utf8("[{},{}]"), "{}", "{}"),
        decodes(
            "xmlAttributes",
            Decodings.XML,
            utf8("<r id=\"7\"><a k=\"v\">1</a></r>"),
            "{\"a\":{\"\":\"1\",\"k\":\"v\"},\"id\":\"7\"}"),
        decodes(
            "xmlRepeated",
            Decodings.XML,
            utf8("<r><a>1</a><a>2</a><a>3</a></r>"),
            "{\"a\":[\"1\",\"2\",\"3\"]}"),
        decodes(
            "xmlNested",
            Decodings.XML,
            utf8("<r><a><b><c>x</c></b></a></r>"),
            "{\"a\":{\"b\":{\"c\":\"x\"}}}"),
        decodes(
            "xmlCdata", Decodings.XML, utf8("<r><a><![CDATA[<x>&]]></a></r>"), "{\"a\":\"<x>&\"}"),
        decodes(
            "xmlEntities",
            Decodings.XML,
            utf8("<r><a>a &amp; b &lt; c</a></r>"),
            "{\"a\":\"a & b < c\"}"),
        decodes("xmlComment", Decodings.XML, utf8("<r><!-- c --><a>1</a></r>"), "{\"a\":\"1\"}"),
        decodes(
            "xmlDeclaration",
            Decodings.XML,
            utf8("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<r><a>1</a></r>\n"),
            "{\"a\":\"1\"}"),
        decodes(
            "xmlEmptyElement",
            Decodings.XML,
            utf8("<r><a/><b></b></r>"),
            "{\"a\":\"\",\"b\":\"\"}"),
        decodes(
            "xmlMixedContent",
            Decodings.XML,
            utf8("<r>text<a>1</a>more</r>"),
            "{\"\":[\"text\",\"more\"],\"a\":\"1\"}"),
        decodes(
            "xmlNamespace",
            Decodings.XML,
            utf8("<r xmlns:p=\"urn:x\"><p:a>1</p:a></r>"),
            "{\"a\":\"1\"}"),
        decodes("xmlUnicode", Decodings.XML, utf8("<r><名>世界</名></r>"), "{\"名\":\"世界\"}"),
        decodes(
            "gzipMultiMember",
            Decodings.JSON_LINES,
            concat(gzip(utf8("{\"a\":1}\n")), gzip(utf8("{\"a\":2}\n"))),
            "{\"a\":1}",
            "{\"a\":2}"),
        // text and csv replace bad bytes
        decodes(
            "stringLineInvalidUtf8",
            Decodings.TEXT_LINE,
            concat(utf8("a"), new byte[] {(byte) 0xff}, utf8("b\n")),
            "{\"__raw__\":\"a\uFFFDb\"}"),
        decodes(
            "textInvalidUtf8",
            Decodings.TEXT,
            concat(utf8("a"), new byte[] {(byte) 0xff}, utf8("b")),
            "{\"__raw__\":\"a\uFFFDb\"}"),
        decodes(
            "csvInvalidUtf8",
            Decodings.CSV,
            concat(utf8("a\nx"), new byte[] {(byte) 0xff}, utf8("y\n")),
            "{\"a\":\"x\uFFFDy\"}"),
        // bad data loses only itself
        outcomes(
            "jsonLinesBadLine",
            Decodings.JSON_LINES,
            utf8("{\"a\":1}\nnot json\n{\"a\":3}\n"),
            "ok {\"a\":1}",
            "bad MALFORMED not json",
            "ok {\"a\":3}"),
        outcomes(
            "jsonLinesArrayLineFails",
            Decodings.JSON_LINES,
            utf8("[{\"a\":1},{\"a\":2}]\n"),
            "bad MALFORMED [{\"a\":1},{\"a\":2}]"),
        outcomes(
            "jsonLinesPrettyPrintedFailsPerLine",
            Decodings.JSON_LINES,
            utf8("{\n  \"a\": 1\n}\n"),
            "bad MALFORMED {",
            "bad MALFORMED   \"a\": 1",
            "bad MALFORMED }"),
        outcomes(
            "jsonArrayMalformedElementEndsTheObject",
            Decodings.JSON_ARRAY,
            utf8("[{\"a\":1},{\"a\" 2},{\"a\":3}]"),
            "ok {\"a\":1}",
            "bad MALFORMED terminal "),
        outcomes("csvLongRowFails", Decodings.CSV, utf8("a,b\n1,2,3\n"), "bad MALFORMED 1,2,3"),
        outcomes(
            "csvShortRowFailsAlone",
            Decodings.CSV,
            utf8("a,b\n1\n4,5\n"),
            "bad MALFORMED 1",
            "ok {\"a\":\"4\",\"b\":\"5\"}"),
        outcomes(
            "csvBadQuoteFailsAlone",
            Decodings.CSV,
            utf8("a,b\n\"1\"x,2\n3,4\n"),
            "bad MALFORMED \"1\"x,2",
            "ok {\"a\":\"3\",\"b\":\"4\"}"),
        outcomes(
            "csvUnclosedQuoteEndsTheObject",
            Decodings.CSV,
            utf8("a,b\n\"1,2\n"),
            "bad MALFORMED terminal \"1,2\n"),
        outcomes(
            "csvDuplicateHeaderEndsTheObject",
            Decodings.CSV,
            utf8("a,a\n1,2\n"),
            "bad MALFORMED terminal a,a"),
        outcomes(
            "csvEmptyHeaderIsNamed",
            Decodings.CSV,
            utf8("a,\n1,2\n"),
            "ok {\"a\":\"1\",\"_c1\":\"2\"}"),
        // byte order marks are removed
        outcomes("bomStringLine", Decodings.TEXT_LINE, utf8("﻿x\n"), "ok {\"__raw__\":\"x\"}"),
        outcomes("bomText", Decodings.TEXT, utf8("﻿x"), "ok {\"__raw__\":\"x\"}"),
        outcomes("bomCsv", Decodings.CSV, utf8("﻿a\n1\n"), "ok {\"a\":\"1\"}"),
        outcomes("bomJsonLines", Decodings.JSON_LINES, utf8("﻿{\"a\":1}\n"), "ok {\"a\":1}"),
        // an empty object gives nothing
        outcomes("emptyJsonObject", Decodings.JSON_OBJECT, new byte[0]),
        outcomes("emptyJsonArray", Decodings.JSON_ARRAY, new byte[0]),
        outcomes("emptyXml", Decodings.XML, new byte[0]),
        outcomes("emptyText", Decodings.TEXT, new byte[0]),
        Arguments.of(
            "emptyForcedGzip",
            Decodings.JSON_LINES,
            DecompressionCodec.GZIP,
            new byte[0],
            HEAP,
            List.of()),
        // gzip trailing bytes
        outcomes(
            "gzipTrailingBytes",
            Decodings.JSON_LINES,
            concat(gzip(JSON_LINES), utf8("xx")),
            "ok {\"a\":1}",
            "ok {\"a\":2}",
            "bad CORRUPT_COMPRESSION terminal "),
        // xml
        outcomes(
            "xmlDoctypeFails",
            Decodings.XML,
            utf8("<!DOCTYPE r><r><a>1</a></r>"),
            "bad MALFORMED terminal <!DOCTYPE r><r><a>1</a></r>"),
        outcomes(
            "xmlTextAfterTheRootFails",
            Decodings.XML,
            utf8("<r><a>1</a></r>junk"),
            "bad MALFORMED terminal <r><a>1</a></r>junk"),
        outcomes("gzipOfEmptyTextGivesNothing", Decodings.TEXT, gzip(new byte[0])),
        outcomes(
            "xmlTrailingContentFails",
            Decodings.XML,
            utf8("<r><a>1</a></r><s/>"),
            "bad MALFORMED terminal <r><a>1</a></r><s/>"),
        // json lines bytes and whitespace
        outcomes(
            "jsonLinesInvalidUtf8Fails",
            Decodings.JSON_LINES,
            concat(utf8("{\"a\":\""), new byte[] {(byte) 0xff}, utf8("\"}\n")),
            "bad MALFORMED {\"a\":\"�\"}"),
        outcomes(
            "jsonLinesFormFeedLineFails",
            Decodings.JSON_LINES,
            utf8("{\"a\":1}\n\f\n{\"a\":2}\n"),
            "ok {\"a\":1}",
            "bad MALFORMED \f",
            "ok {\"a\":2}"),
        outcomes(
            "jsonLinesUtf16Decodes",
            Decodings.JSON_LINES,
            concat(
                new byte[] {(byte) 0xFE, (byte) 0xFF},
                "{\"a\":1}\n".getBytes(StandardCharsets.UTF_16BE)),
            "ok {\"a\":1}"),
        outcomes(
            "jsonLinesUtf32Decodes",
            Decodings.JSON_LINES,
            concat(
                new byte[] {0, 0, (byte) 0xFE, (byte) 0xFF},
                "{\"a\":1}\n".getBytes(Charset.forName("UTF-32BE"))),
            "ok {\"a\":1}"),
        // limits: at a 1 MiB heap a record may be 8 KiB and a document 16 KiB
        Arguments.of(
            "aRecordOverTheLimitIsOversized",
            Decodings.TEXT_LINE,
            null,
            utf8("x".repeat(10_000) + "\nok\n"),
            MIB,
            List.of("big #1", "ok {\"__raw__\":\"ok\"}")),
        Arguments.of(
            "aDocumentOverTheLimitIsOversized",
            Decodings.JSON_OBJECT,
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
      FormatConfig format,
      DecompressionCodec compression,
      byte[] bytes,
      long heap,
      List<String> expected)
      throws SourceReadException {
    assertEquals(expected, decoded(format, compression, bytes, heap));
  }

  @Test
  void aTruncatedGzipEmitsTheRecordsBeforeTheCut() throws Exception {
    List<String> lines =
        IntStream.range(0, 3000).mapToObj(i -> "{\"a\":" + i * 7919 % 100003 + "}").toList();
    byte[] whole = gzip(utf8(String.join("\n", lines) + "\n"));
    byte[] truncated = Arrays.copyOf(whole, whole.length / 2);

    List<String> decoded = decoded(Decodings.JSON_LINES, null, truncated, HEAP);

    int emitted = decoded.size() - 1;
    assertTrue(emitted > 0, "records before the cut: " + emitted);
    List<String> expected =
        new ArrayList<>(lines.subList(0, emitted).stream().map(l -> "ok " + l).toList());
    expected.add("bad CORRUPT_COMPRESSION terminal ");
    assertEquals(expected, decoded);
  }

  private static Arguments decodes(
      String name, FormatConfig format, byte[] bytes, String... records) {
    return outcomes(
        name, format, bytes, Arrays.stream(records).map(r -> "ok " + r).toArray(String[]::new));
  }

  private static Arguments outcomes(
      String name, FormatConfig format, byte[] bytes, String... outcomes) {
    return Arguments.of(name, format, null, bytes, HEAP, List.of(outcomes));
  }

  private List<String> decoded(
      FormatConfig format, DecompressionCodec compression, byte[] bytes, long heap)
      throws SourceReadException {
    S3Client s3 = mock(S3Client.class);
    when(s3.getObject(any(GetObjectRequest.class)))
        .thenReturn(
            new ResponseInputStream<>(
                GetObjectResponse.builder().contentLength((long) bytes.length).build(),
                AbortableInputStream.create(
                    new ByteArrayInputStream(bytes), mock(Abortable.class))));
    List<String> seen = new ArrayList<>();
    new S3ObjectDecoder(
            new S3Decoding(
                new DecodingConfig(
                    List.of(compression == null ? DecompressionCodec.AUTO : compression), format),
                false),
            heap)
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
