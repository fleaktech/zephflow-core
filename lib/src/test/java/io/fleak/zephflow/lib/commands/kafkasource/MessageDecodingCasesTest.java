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
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.MessageDecodingConverter;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
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
 * Feeds each message to {@link MessageDecodingConverter} with Kafka's decoding and pins the result:
 * the records for valid input, the failure kind for a bad message, and the cases of the spec's 4.1
 * items 5 and 6.
 */
class MessageDecodingCasesTest {

  private static final String FAILS = "FAILS";
  private static final byte[] JSON_LINES = utf8("{\"a\":1}\n{\"a\":2}\n");

  static Stream<Arguments> cases() {
    return Stream.of(
        // valid input decodes to its records
        decodes(
            "jsonObject",
            EncodingType.JSON_OBJECT,
            utf8("{\"a\":1,\"b\":\"x\"}"),
            "{\"a\":1,\"b\":\"x\"}"),
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
            "jsonArray",
            EncodingType.JSON_ARRAY,
            utf8("[{\"a\":1},{\"a\":2}]"),
            "{\"a\":1}",
            "{\"a\":2}"),
        decodes("jsonArrayEmpty", EncodingType.JSON_ARRAY, utf8("[]")),
        decodes("jsonLines", EncodingType.JSON_OBJECT_LINE, JSON_LINES, "{\"a\":1}", "{\"a\":2}"),
        decodes(
            "jsonLinesBlankLines",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"a\":1}\n\n   \n{\"a\":2}\n"),
            "{\"a\":1}",
            "{\"a\":2}"),
        decodes(
            "jsonLinesCrlf",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"a\":1}\r\n{\"a\":2}\r\n"),
            "{\"a\":1}",
            "{\"a\":2}"),
        decodes(
            "stringLineKeepsBlankLines",
            EncodingType.STRING_LINE,
            utf8("x\n\ny\n"),
            "{\"__raw__\":\"x\"}",
            "{\"__raw__\":\"\"}",
            "{\"__raw__\":\"y\"}"),
        decodes(
            "stringLineCr",
            EncodingType.STRING_LINE,
            utf8("x\ry\r"),
            "{\"__raw__\":\"x\"}",
            "{\"__raw__\":\"y\"}"),
        decodes(
            "stringLineInvalidUtf8",
            EncodingType.STRING_LINE,
            concat(utf8("a"), new byte[] {(byte) 0xff}, utf8("b\n")),
            "{\"__raw__\":\"a�b\"}"),
        decodes("text", EncodingType.TEXT, utf8("hello\nworld"), "{\"__raw__\":\"hello\\nworld\"}"),
        decodes(
            "csvTrimsSpaces",
            EncodingType.CSV,
            utf8("name, age\n bob , 3\n"),
            "{\"name\":\"bob\",\"age\":\"3\"}"),
        decodes(
            "csvQuotedComma",
            EncodingType.CSV,
            utf8("a,b\n\"x,y\",2\n"),
            "{\"a\":\"x,y\",\"b\":\"2\"}"),
        decodes("csvHeaderOnly", EncodingType.CSV, utf8("a,b\n")),
        decodes("xml", EncodingType.XML, utf8("<r><a>1</a></r>"), "{\"a\":\"1\"}"),
        decodes(
            "xmlAttributes",
            EncodingType.XML,
            utf8("<r id=\"7\"><a k=\"v\">1</a></r>"),
            "{\"a\":{\"\":\"1\",\"k\":\"v\"},\"id\":\"7\"}"),
        decodes(
            "gzipDetected",
            EncodingType.JSON_OBJECT_LINE,
            gzip(JSON_LINES),
            "{\"a\":1}",
            "{\"a\":2}"),
        decodes(
            "gzipMultiMember",
            EncodingType.JSON_OBJECT_LINE,
            concat(gzip(utf8("{\"a\":1}\n")), gzip(utf8("{\"a\":2}\n"))),
            "{\"a\":1}",
            "{\"a\":2}"),
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
        decodes(
            "csvQuotedSpaces",
            EncodingType.CSV,
            utf8("a,b\n\" x \",2\n"),
            "{\"a\":\" x \",\"b\":\"2\"}"),
        decodes(
            "csvInvalidUtf8",
            EncodingType.CSV,
            concat(utf8("a\nx"), new byte[] {(byte) 0xff}, utf8("y\n")),
            "{\"a\":\"x\uFFFDy\"}"),
        decodes(
            "stringLineCrlf",
            EncodingType.STRING_LINE,
            utf8("x\r\ny\r\n"),
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
            "textInvalidUtf8",
            EncodingType.TEXT,
            concat(utf8("a"), new byte[] {(byte) 0xff}, utf8("b")),
            "{\"__raw__\":\"a\uFFFDb\"}"),
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
        decodes("emptyJsonLines", EncodingType.JSON_OBJECT_LINE, new byte[0]),
        decodes(
            "jsonObjectNested",
            EncodingType.JSON_OBJECT,
            utf8("{\"a\":{\"b\":{\"c\":[1,\"x\",true]}}}"),
            "{\"a\":{\"b\":{\"c\":[1,\"x\",true]}}}"),
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
        decodes("jsonArrayEmptyObjects", EncodingType.JSON_ARRAY, utf8("[{},{}]"), "{}", "{}"),
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
        // a bad record fails the whole message
        fails(
            "jsonLinesBadLine",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"a\":1}\nnot json\n{\"a\":3}\n"),
            "MALFORMED"),
        fails("jsonObjectMalformed", EncodingType.JSON_OBJECT, utf8("{\"a\":"), "MALFORMED"),
        fails(
            "jsonArrayMalformedElement",
            EncodingType.JSON_ARRAY,
            utf8("[{\"a\":1},{\"a\" 2},{\"a\":3}]"),
            "MALFORMED"),
        fails(
            "jsonLinesPrettyPrinted",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\n  \"a\": 1\n}\n"),
            "MALFORMED"),
        fails(
            "truncatedGzip",
            EncodingType.JSON_OBJECT_LINE,
            Arrays.copyOf(gzip(JSON_LINES), gzip(JSON_LINES).length / 2),
            "CORRUPT_COMPRESSION"),
        fails("csvShortRow", EncodingType.CSV, utf8("a,b\n1\n4,5\n"), "MALFORMED"),
        fails("csvBadQuote", EncodingType.CSV, utf8("a,b\n\"1\"x,2\n3,4\n"), "MALFORMED"),
        fails("csvUnclosedQuote", EncodingType.CSV, utf8("a,b\n\"1,2\n"), "MALFORMED"),
        fails("xmlMalformed", EncodingType.XML, utf8("<r><a>1</r>"), "MALFORMED"),
        // 4.1 item 5: an empty value
        decodes("emptyJsonObject", EncodingType.JSON_OBJECT, new byte[0]),
        decodes("emptyJsonArray", EncodingType.JSON_ARRAY, new byte[0]),
        decodes("emptyXml", EncodingType.XML, new byte[0]),
        decodes("emptyText", EncodingType.TEXT, new byte[0]),
        decodes("gzipOfNothingText", EncodingType.TEXT, gzip(new byte[0])),
        decodes("gzipOfNothingJsonObject", EncodingType.JSON_OBJECT, gzip(new byte[0])),
        decodes("gzipOfNothingJsonArray", EncodingType.JSON_ARRAY, gzip(new byte[0])),
        decodes("gzipOfNothingXml", EncodingType.XML, gzip(new byte[0])),
        // 4.1 item 6: strict lines, rows and documents; BOMs and UTF-16/32 detected; long names
        // kept
        fails(
            "jsonLinesArrayLine",
            EncodingType.JSON_OBJECT_LINE,
            utf8("[{\"a\":1},{\"a\":2}]\n"),
            "MALFORMED"),
        fails(
            "jsonLinesFormFeedLine",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"a\":1}\n\f\n{\"a\":2}\n"),
            "MALFORMED"),
        fails(
            "jsonLinesInvalidUtf8",
            EncodingType.JSON_OBJECT_LINE,
            concat(utf8("{\"a\":\""), new byte[] {(byte) 0xff}, utf8("\"}\n")),
            "MALFORMED"),
        decodes(
            "jsonLinesUtf16",
            EncodingType.JSON_OBJECT_LINE,
            concat(
                new byte[] {(byte) 0xFE, (byte) 0xFF},
                "{\"a\":1}\n".getBytes(StandardCharsets.UTF_16BE)),
            "{\"a\":1}"),
        decodes(
            "jsonLinesUtf32",
            EncodingType.JSON_OBJECT_LINE,
            concat(
                new byte[] {0, 0, (byte) 0xFE, (byte) 0xFF},
                "{\"a\":1}\n".getBytes(Charset.forName("UTF-32BE"))),
            "{\"a\":1}"),
        fails("csvLongRow", EncodingType.CSV, utf8("a,b\n1,2,3\n"), "MALFORMED"),
        fails("csvDuplicateHeader", EncodingType.CSV, utf8("a,a\n1,2\n"), "MALFORMED"),
        decodes(
            "csvEmptyHeaderName",
            EncodingType.CSV,
            utf8("a,\n1,2\n"),
            "{\"a\":\"1\",\"_c1\":\"2\"}"),
        decodes("csvWhitespaceOnly", EncodingType.CSV, utf8("  \n")),
        fails("csvTooManyColumns", EncodingType.CSV, utf8(columns(16_385) + "\n"), "MALFORMED"),
        fails("xmlDoctype", EncodingType.XML, utf8("<!DOCTYPE r><r><a>1</a></r>"), "MALFORMED"),
        fails("xmlTextAfterTheRoot", EncodingType.XML, utf8("<r><a>1</a></r>junk"), "MALFORMED"),
        fails("xmlSecondRoot", EncodingType.XML, utf8("<r><a>1</a></r><s/>"), "MALFORMED"),
        fails(
            "gzipTrailingBytes",
            EncodingType.JSON_OBJECT_LINE,
            concat(gzip(JSON_LINES), utf8("xx")),
            "CORRUPT_COMPRESSION"),
        decodes(
            "jsonLongName",
            EncodingType.JSON_OBJECT,
            utf8("{\"" + "n".repeat(50_001) + "\":1}"),
            "{\"" + "n".repeat(50_001) + "\":1}"),
        decodes(
            "jsonLongString",
            EncodingType.JSON_OBJECT,
            utf8("{\"s\":\"" + "x".repeat(20_000_001) + "\"}"),
            "{\"s\":\"" + "x".repeat(20_000_001) + "\"}"),
        decodes("bomStringLine", EncodingType.STRING_LINE, utf8("﻿x\n"), "{\"__raw__\":\"x\"}"),
        decodes("bomText", EncodingType.TEXT, utf8("﻿x"), "{\"__raw__\":\"x\"}"),
        decodes("bomCsv", EncodingType.CSV, utf8("﻿a\n1\n"), "{\"a\":\"1\"}"),
        decodes("bomJsonLines", EncodingType.JSON_OBJECT_LINE, utf8("﻿{\"a\":1}\n"), "{\"a\":1}"));
  }

  /**
   * The records as JSON, or {@code FAILS} followed by the kind on the DLQ text's first line when
   * the message failed.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void givesTheRecordsOrTheFailureKind(
      String name, EncodingType encodingType, byte[] bytes, List<String> expected) {
    assertEquals(expected, decoded(encodingType, new SerializedEvent(null, bytes, null)));
  }

  @Test
  void xmlWithOver1021AttributesDecodes() {
    SerializedEvent message =
        new SerializedEvent(null, utf8("<r><a " + attributes(1022) + "/></r>"), null);

    assertEquals(
        List.of(Map.of("a", attributeMap(1022))),
        decoded(EncodingType.XML, message).stream()
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

  private static Arguments decodes(
      String name, EncodingType encodingType, byte[] bytes, String... records) {
    return Arguments.of(name, encodingType, bytes, List.of(records));
  }

  private static Arguments fails(
      String name, EncodingType encodingType, byte[] bytes, String kind) {
    return Arguments.of(name, encodingType, bytes, List.of(FAILS + " " + kind));
  }

  private static List<String> decoded(EncodingType encodingType, SerializedEvent message) {
    MessageDecodingConverter converter =
        new MessageDecodingConverter(
            KafkaSourceCommand.decodingConfig(
                KafkaSourceDto.Config.builder().encodingType(encodingType).build()),
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
