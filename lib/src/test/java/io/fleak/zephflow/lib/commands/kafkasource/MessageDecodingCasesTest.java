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
package io.fleak.zephflow.lib.commands.kafkasource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.filedecode.Decodings;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.MessageDecodingConverter;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.decode.config.CsvFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.FormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.FramingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.serdes.decode.config.TextFormatConfig;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Feeds each message to {@link MessageDecodingConverter} with Kafka's decoding (gzip detected
 * automatically) and pins the result: the records for valid input, the failure kind for a bad
 * message, and the cases of the spec's 4.1 items 5 and 6.
 */
class MessageDecodingCasesTest {

  private static final String FAILS = "FAILS";
  private static final byte[] JSON_LINES = utf8("{\"a\":1}\n{\"a\":2}\n");

  static Stream<Arguments> cases() {
    return Stream.of(
        // valid input decodes to its records
        decodes(
            "jsonObject",
            Decodings.JSON_OBJECT,
            utf8("{\"a\":1,\"b\":\"x\"}"),
            "{\"a\":1,\"b\":\"x\"}"),
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
            "jsonArray",
            Decodings.JSON_ARRAY,
            utf8("[{\"a\":1},{\"a\":2}]"),
            "{\"a\":1}",
            "{\"a\":2}"),
        decodes("jsonArrayEmpty", Decodings.JSON_ARRAY, utf8("[]")),
        decodes("jsonLines", Decodings.JSON_LINES, JSON_LINES, "{\"a\":1}", "{\"a\":2}"),
        decodes(
            "jsonLinesBlankLines",
            Decodings.JSON_LINES,
            utf8("{\"a\":1}\n\n   \n{\"a\":2}\n"),
            "{\"a\":1}",
            "{\"a\":2}"),
        decodes(
            "jsonLinesCrlf",
            Decodings.JSON_LINES,
            utf8("{\"a\":1}\r\n{\"a\":2}\r\n"),
            "{\"a\":1}",
            "{\"a\":2}"),
        decodes(
            "stringLineKeepsBlankLines",
            Decodings.TEXT_LINE,
            utf8("x\n\ny\n"),
            "{\"__raw__\":\"x\"}",
            "{\"__raw__\":\"\"}",
            "{\"__raw__\":\"y\"}"),
        decodes(
            "stringLineCr",
            Decodings.TEXT_LINE,
            utf8("x\ry\r"),
            "{\"__raw__\":\"x\"}",
            "{\"__raw__\":\"y\"}"),
        decodes(
            "stringLineInvalidUtf8",
            Decodings.TEXT_LINE,
            concat(utf8("a"), new byte[] {(byte) 0xff}, utf8("b\n")),
            "{\"__raw__\":\"a�b\"}"),
        decodes("text", Decodings.TEXT, utf8("hello\nworld"), "{\"__raw__\":\"hello\\nworld\"}"),
        decodes(
            "csvTrimsSpaces",
            Decodings.CSV,
            utf8("name, age\n bob , 3\n"),
            "{\"name\":\"bob\",\"age\":\"3\"}"),
        decodes(
            "csvQuotedComma",
            Decodings.CSV,
            utf8("a,b\n\"x,y\",2\n"),
            "{\"a\":\"x,y\",\"b\":\"2\"}"),
        decodes("csvHeaderOnly", Decodings.CSV, utf8("a,b\n")),
        decodes("xml", Decodings.XML, utf8("<r><a>1</a></r>"), "{\"a\":\"1\"}"),
        decodes(
            "xmlAttributes",
            Decodings.XML,
            utf8("<r id=\"7\"><a k=\"v\">1</a></r>"),
            "{\"a\":{\"\":\"1\",\"k\":\"v\"},\"id\":\"7\"}"),
        decodes("gzipDetected", Decodings.JSON_LINES, gzip(JSON_LINES), "{\"a\":1}", "{\"a\":2}"),
        decodes(
            "gzipMultiMember",
            Decodings.JSON_LINES,
            concat(gzip(utf8("{\"a\":1}\n")), gzip(utf8("{\"a\":2}\n"))),
            "{\"a\":1}",
            "{\"a\":2}"),
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
        decodes(
            "csvQuotedSpaces",
            Decodings.CSV,
            utf8("a,b\n\" x \",2\n"),
            "{\"a\":\" x \",\"b\":\"2\"}"),
        decodes(
            "csvInvalidUtf8",
            Decodings.CSV,
            concat(utf8("a\nx"), new byte[] {(byte) 0xff}, utf8("y\n")),
            "{\"a\":\"x\uFFFDy\"}"),
        decodes(
            "stringLineCrlf",
            Decodings.TEXT_LINE,
            utf8("x\r\ny\r\n"),
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
            "textInvalidUtf8",
            Decodings.TEXT,
            concat(utf8("a"), new byte[] {(byte) 0xff}, utf8("b")),
            "{\"__raw__\":\"a\uFFFDb\"}"),
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
        decodes("emptyJsonLines", Decodings.JSON_LINES, new byte[0]),
        decodes(
            "jsonObjectNested",
            Decodings.JSON_OBJECT,
            utf8("{\"a\":{\"b\":{\"c\":[1,\"x\",true]}}}"),
            "{\"a\":{\"b\":{\"c\":[1,\"x\",true]}}}"),
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
        decodes("jsonArrayEmptyObjects", Decodings.JSON_ARRAY, utf8("[{},{}]"), "{}", "{}"),
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
        // a bad record fails the whole message
        fails(
            "jsonLinesBadLine",
            Decodings.JSON_LINES,
            utf8("{\"a\":1}\nnot json\n{\"a\":3}\n"),
            "MALFORMED"),
        fails("jsonObjectMalformed", Decodings.JSON_OBJECT, utf8("{\"a\":"), "MALFORMED"),
        fails(
            "jsonArrayMalformedElement",
            Decodings.JSON_ARRAY,
            utf8("[{\"a\":1},{\"a\" 2},{\"a\":3}]"),
            "MALFORMED"),
        fails(
            "jsonLinesPrettyPrinted",
            Decodings.JSON_LINES,
            utf8("{\n  \"a\": 1\n}\n"),
            "MALFORMED"),
        fails(
            "truncatedGzip",
            Decodings.JSON_LINES,
            Arrays.copyOf(gzip(JSON_LINES), gzip(JSON_LINES).length / 2),
            "CORRUPT_COMPRESSION"),
        fails("csvShortRow", Decodings.CSV, utf8("a,b\n1\n4,5\n"), "MALFORMED"),
        fails("csvBadQuote", Decodings.CSV, utf8("a,b\n\"1\"x,2\n3,4\n"), "MALFORMED"),
        fails("csvUnclosedQuote", Decodings.CSV, utf8("a,b\n\"1,2\n"), "MALFORMED"),
        fails("xmlMalformed", Decodings.XML, utf8("<r><a>1</r>"), "MALFORMED"),
        // 4.1 item 5: an empty value
        decodes("emptyJsonObject", Decodings.JSON_OBJECT, new byte[0]),
        decodes("emptyJsonArray", Decodings.JSON_ARRAY, new byte[0]),
        decodes("emptyXml", Decodings.XML, new byte[0]),
        decodes("emptyText", Decodings.TEXT, new byte[0]),
        decodes("gzipOfNothingText", Decodings.TEXT, gzip(new byte[0])),
        decodes("gzipOfNothingJsonObject", Decodings.JSON_OBJECT, gzip(new byte[0])),
        decodes("gzipOfNothingJsonArray", Decodings.JSON_ARRAY, gzip(new byte[0])),
        decodes("gzipOfNothingXml", Decodings.XML, gzip(new byte[0])),
        // 4.1 item 6: strict lines/rows/documents, BOM and UTF-16/32 detection, long names
        fails(
            "jsonLinesArrayLine",
            Decodings.JSON_LINES,
            utf8("[{\"a\":1},{\"a\":2}]\n"),
            "MALFORMED"),
        fails(
            "jsonLinesFormFeedLine",
            Decodings.JSON_LINES,
            utf8("{\"a\":1}\n\f\n{\"a\":2}\n"),
            "MALFORMED"),
        fails(
            "jsonLinesInvalidUtf8",
            Decodings.JSON_LINES,
            concat(utf8("{\"a\":\""), new byte[] {(byte) 0xff}, utf8("\"}\n")),
            "MALFORMED"),
        decodes(
            "jsonLinesUtf16",
            Decodings.JSON_LINES,
            concat(
                new byte[] {(byte) 0xFE, (byte) 0xFF},
                "{\"a\":1}\n".getBytes(StandardCharsets.UTF_16BE)),
            "{\"a\":1}"),
        decodes(
            "jsonLinesUtf32",
            Decodings.JSON_LINES,
            concat(
                new byte[] {0, 0, (byte) 0xFE, (byte) 0xFF},
                "{\"a\":1}\n".getBytes(Charset.forName("UTF-32BE"))),
            "{\"a\":1}"),
        fails("csvLongRow", Decodings.CSV, utf8("a,b\n1,2,3\n"), "MALFORMED"),
        fails("csvDuplicateHeader", Decodings.CSV, utf8("a,a\n1,2\n"), "MALFORMED"),
        decodes(
            "csvEmptyHeaderName", Decodings.CSV, utf8("a,\n1,2\n"), "{\"a\":\"1\",\"_c1\":\"2\"}"),
        decodes("csvWhitespaceOnly", Decodings.CSV, utf8("  \n")),
        fails("csvTooManyColumns", Decodings.CSV, utf8(columns(16_385) + "\n"), "MALFORMED"),
        fails("xmlDoctype", Decodings.XML, utf8("<!DOCTYPE r><r><a>1</a></r>"), "MALFORMED"),
        fails("xmlTextAfterTheRoot", Decodings.XML, utf8("<r><a>1</a></r>junk"), "MALFORMED"),
        fails("xmlSecondRoot", Decodings.XML, utf8("<r><a>1</a></r><s/>"), "MALFORMED"),
        fails(
            "gzipTrailingBytes",
            Decodings.JSON_LINES,
            concat(gzip(JSON_LINES), utf8("xx")),
            "CORRUPT_COMPRESSION"),
        decodes(
            "jsonLongName",
            Decodings.JSON_OBJECT,
            utf8("{\"" + "n".repeat(50_001) + "\":1}"),
            "{\"" + "n".repeat(50_001) + "\":1}"),
        decodes(
            "jsonLongString",
            Decodings.JSON_OBJECT,
            utf8("{\"s\":\"" + "x".repeat(20_000_001) + "\"}"),
            "{\"s\":\"" + "x".repeat(20_000_001) + "\"}"),
        decodes("bomStringLine", Decodings.TEXT_LINE, utf8("﻿x\n"), "{\"__raw__\":\"x\"}"),
        decodes("bomText", Decodings.TEXT, utf8("﻿x"), "{\"__raw__\":\"x\"}"),
        decodes("bomCsv", Decodings.CSV, utf8("﻿a\n1\n"), "{\"a\":\"1\"}"),
        decodes("bomJsonLines", Decodings.JSON_LINES, utf8("﻿{\"a\":1}\n"), "{\"a\":1}"));
  }

  /**
   * The records as JSON, or {@code FAILS} followed by the kind on the DLQ text's first line when
   * the message failed.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void givesTheRecordsOrTheFailureKind(
      String name, FormatConfig format, byte[] bytes, List<String> expected) {
    assertEquals(expected, decoded(format, new SerializedEvent(null, bytes, null)));
  }

  @Test
  void xmlWithOver1021AttributesDecodes() {
    SerializedEvent message =
        new SerializedEvent(null, utf8("<r><a " + attributes(1022) + "/></r>"), null);

    assertEquals(
        List.of(Map.of("a", attributeMap(1022))),
        decoded(Decodings.XML, message).stream()
            .map(r -> JsonUtils.fromJsonString(r, new TypeReference<Map<String, Object>>() {}))
            .toList());
  }

  @Test
  void decodingDecompressesOnlyWhatItsCompressionSays() {
    MessageDecodingConverter converter =
        new MessageDecodingConverter(
            KafkaSourceCommand.decodingConfig(
                KafkaSourceDto.Config.builder()
                    .decoding(
                        new DecodingConfig(
                            List.of(), new JsonFormatConfig(JsonFormatConfig.Mode.LINES)))
                    .build()),
            MessageDecodingConverter.Unit.MESSAGE);

    ConvertedResult<SerializedEvent> result =
        converter.convert(new SerializedEvent(null, gzip(JSON_LINES), null), context());

    assertEquals("MALFORMED json message: JsonParseException", result.error().getMessage());
  }

  @Test
  void textWithNoCompressionRefusesAGzipMessage() {
    MessageDecodingConverter converter =
        new MessageDecodingConverter(
            KafkaSourceCommand.decodingConfig(
                KafkaSourceDto.Config.builder()
                    .decoding(
                        new DecodingConfig(
                            List.of(), new TextFormatConfig(null, new FramingConfig.Line(null))))
                    .build()),
            MessageDecodingConverter.Unit.MESSAGE);

    ConvertedResult<SerializedEvent> result =
        converter.convert(new SerializedEvent(null, gzip(JSON_LINES), null), context());

    assertEquals(
        "MALFORMED text message: UndeclaredCompressionException", result.error().getMessage());
  }

  @Test
  void csvWithNoCompressionRefusesAGzipMessage() {
    MessageDecodingConverter converter =
        new MessageDecodingConverter(
            KafkaSourceCommand.decodingConfig(
                KafkaSourceDto.Config.builder()
                    .decoding(
                        new DecodingConfig(
                            List.of(),
                            new CsvFormatConfig(
                                null, null, null, null, null, null, null, null, null)))
                    .build()),
            MessageDecodingConverter.Unit.MESSAGE);

    ConvertedResult<SerializedEvent> result =
        converter.convert(new SerializedEvent(null, gzip(JSON_LINES), null), context());

    assertEquals(
        "MALFORMED csv message: UndeclaredCompressionException", result.error().getMessage());
  }

  private static Arguments decodes(
      String name, FormatConfig format, byte[] bytes, String... records) {
    return Arguments.of(name, format, bytes, List.of(records));
  }

  private static Arguments fails(String name, FormatConfig format, byte[] bytes, String kind) {
    return Arguments.of(name, format, bytes, List.of(FAILS + " " + kind));
  }

  private static List<String> decoded(FormatConfig format, SerializedEvent message) {
    MessageDecodingConverter converter =
        new MessageDecodingConverter(
            KafkaSourceCommand.decodingConfig(
                KafkaSourceDto.Config.builder()
                    .decoding(Decodings.autoCompression(format))
                    .build()),
            MessageDecodingConverter.Unit.MESSAGE);
    ConvertedResult<SerializedEvent> result = converter.convert(message, context());
    if (result.transformedData() != null) {
      return json(result.transformedData());
    }
    return List.of(FAILS + " " + result.error().getMessage().split(" ")[0]);
  }

  private static List<String> json(List<RecordFleakData> records) {
    return records.stream().map(r -> JsonUtils.toJsonString(r.unwrap())).toList();
  }

  private static SourceExecutionContext<SerializedEvent> context() {
    return new SourceExecutionContext<>(
        null,
        null,
        null,
        mock(FleakCounter.class),
        mock(FleakCounter.class),
        mock(FleakCounter.class),
        null);
  }

  private static String attributes(int count) {
    return IntStream.range(0, count)
        .mapToObj(i -> "x" + i + "=\"1\"")
        .collect(Collectors.joining(" "));
  }

  private static Map<String, String> attributeMap(int count) {
    Map<String, String> attributes = new HashMap<>();
    IntStream.range(0, count).forEach(i -> attributes.put("x" + i, "1"));
    return attributes;
  }

  private static String columns(int count) {
    return IntStream.range(0, count).mapToObj(i -> "c" + i).collect(Collectors.joining(","));
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
    Arrays.stream(parts).forEach(out::writeBytes);
    return out.toByteArray();
  }
}
