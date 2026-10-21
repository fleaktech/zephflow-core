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

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.source.BytesRawDataConverter;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.MessageDecodingConverter;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.compression.GzipDetectingDecompressor;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Feeds the same message to the legacy Kafka converter (gzip detection, then the legacy
 * deserializer) and to {@link MessageDecodingConverter} with Kafka's decoding, and pins both
 * results: the same records for valid input, and each decoding difference of the spec's 4.1 items 5
 * and 6.
 */
class MessageDecodingDifferentialTest {

  private static final String FAILS = "FAILS";
  private static final byte[] JSON_LINES = utf8("{\"a\":1}\n{\"a\":2}\n");

  /**
   * What each side gives: the records as JSON, or {@code FAILS} when the message failed (followed,
   * for the new side, by the kind on its DLQ text's first line).
   */
  record Observed(List<String> legacy, List<String> decoded) {}

  static Stream<Arguments> cases() {
    return Stream.of(
        // valid input decodes the same
        same(
            "jsonObject",
            EncodingType.JSON_OBJECT,
            utf8("{\"a\":1,\"b\":\"x\"}"),
            "{\"a\":1,\"b\":\"x\"}"),
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
            "jsonArray",
            EncodingType.JSON_ARRAY,
            utf8("[{\"a\":1},{\"a\":2}]"),
            "{\"a\":1}",
            "{\"a\":2}"),
        same("jsonArrayEmpty", EncodingType.JSON_ARRAY, utf8("[]")),
        same("jsonLines", EncodingType.JSON_OBJECT_LINE, JSON_LINES, "{\"a\":1}", "{\"a\":2}"),
        same(
            "jsonLinesBlankLines",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"a\":1}\n\n   \n{\"a\":2}\n"),
            "{\"a\":1}",
            "{\"a\":2}"),
        same(
            "jsonLinesCrlf",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"a\":1}\r\n{\"a\":2}\r\n"),
            "{\"a\":1}",
            "{\"a\":2}"),
        same(
            "stringLineKeepsBlankLines",
            EncodingType.STRING_LINE,
            utf8("x\n\ny\n"),
            "{\"__raw__\":\"x\"}",
            "{\"__raw__\":\"\"}",
            "{\"__raw__\":\"y\"}"),
        same(
            "stringLineCr",
            EncodingType.STRING_LINE,
            utf8("x\ry\r"),
            "{\"__raw__\":\"x\"}",
            "{\"__raw__\":\"y\"}"),
        same(
            "stringLineInvalidUtf8",
            EncodingType.STRING_LINE,
            concat(utf8("a"), new byte[] {(byte) 0xff}, utf8("b\n")),
            "{\"__raw__\":\"a�b\"}"),
        same("text", EncodingType.TEXT, utf8("hello\nworld"), "{\"__raw__\":\"hello\\nworld\"}"),
        same(
            "csvTrimsSpaces",
            EncodingType.CSV,
            utf8("name, age\n bob , 3\n"),
            "{\"name\":\"bob\",\"age\":\"3\"}"),
        same(
            "csvQuotedComma",
            EncodingType.CSV,
            utf8("a,b\n\"x,y\",2\n"),
            "{\"a\":\"x,y\",\"b\":\"2\"}"),
        same("csvHeaderOnly", EncodingType.CSV, utf8("a,b\n")),
        same("xml", EncodingType.XML, utf8("<r><a>1</a></r>"), "{\"a\":\"1\"}"),
        same(
            "xmlAttributes",
            EncodingType.XML,
            utf8("<r id=\"7\"><a k=\"v\">1</a></r>"),
            "{\"a\":{\"\":\"1\",\"k\":\"v\"},\"id\":\"7\"}"),
        same(
            "gzipDetected",
            EncodingType.JSON_OBJECT_LINE,
            gzip(JSON_LINES),
            "{\"a\":1}",
            "{\"a\":2}"),
        same(
            "gzipMultiMember",
            EncodingType.JSON_OBJECT_LINE,
            concat(gzip(utf8("{\"a\":1}\n")), gzip(utf8("{\"a\":2}\n"))),
            "{\"a\":1}",
            "{\"a\":2}"),
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
        same(
            "csvQuotedSpaces",
            EncodingType.CSV,
            utf8("a,b\n\" x \",2\n"),
            "{\"a\":\" x \",\"b\":\"2\"}"),
        same(
            "csvInvalidUtf8",
            EncodingType.CSV,
            concat(utf8("a\nx"), new byte[] {(byte) 0xff}, utf8("y\n")),
            "{\"a\":\"x\uFFFDy\"}"),
        same(
            "stringLineCrlf",
            EncodingType.STRING_LINE,
            utf8("x\r\ny\r\n"),
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
            "textInvalidUtf8",
            EncodingType.TEXT,
            concat(utf8("a"), new byte[] {(byte) 0xff}, utf8("b")),
            "{\"__raw__\":\"a\uFFFDb\"}"),
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
        same("emptyJsonLines", EncodingType.JSON_OBJECT_LINE, new byte[0]),
        same(
            "jsonObjectNested",
            EncodingType.JSON_OBJECT,
            utf8("{\"a\":{\"b\":{\"c\":[1,\"x\",true]}}}"),
            "{\"a\":{\"b\":{\"c\":[1,\"x\",true]}}}"),
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
        same("jsonArrayEmptyObjects", EncodingType.JSON_ARRAY, utf8("[{},{}]"), "{}", "{}"),
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
        // a bad record fails the whole message on both sides
        both(
            "jsonLinesBadLine",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"a\":1}\nnot json\n{\"a\":3}\n"),
            "MALFORMED"),
        both("jsonObjectMalformed", EncodingType.JSON_OBJECT, utf8("{\"a\":"), "MALFORMED"),
        both(
            "jsonArrayMalformedElement",
            EncodingType.JSON_ARRAY,
            utf8("[{\"a\":1},{\"a\" 2},{\"a\":3}]"),
            "MALFORMED"),
        both(
            "jsonLinesPrettyPrinted",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\n  \"a\": 1\n}\n"),
            "MALFORMED"),
        both(
            "truncatedGzip",
            EncodingType.JSON_OBJECT_LINE,
            Arrays.copyOf(gzip(JSON_LINES), gzip(JSON_LINES).length / 2),
            "CORRUPT_COMPRESSION"),
        both("csvShortRow", EncodingType.CSV, utf8("a,b\n1\n4,5\n"), "MALFORMED"),
        both("csvBadQuote", EncodingType.CSV, utf8("a,b\n\"1\"x,2\n3,4\n"), "MALFORMED"),
        both("csvUnclosedQuote", EncodingType.CSV, utf8("a,b\n\"1,2\n"), "MALFORMED"),
        both("xmlMalformed", EncodingType.XML, utf8("<r><a>1</r>"), "MALFORMED"),
        // 4.1 item 5: an empty value
        differs("emptyJsonObject", EncodingType.JSON_OBJECT, new byte[0], List.of(FAILS)),
        differs("emptyJsonArray", EncodingType.JSON_ARRAY, new byte[0], List.of(FAILS)),
        differs("emptyXml", EncodingType.XML, new byte[0], List.of(FAILS)),
        differs("emptyText", EncodingType.TEXT, new byte[0], List.of("{\"__raw__\":\"\"}")),
        differs(
            "gzipOfNothingText",
            EncodingType.TEXT,
            gzip(new byte[0]),
            List.of("{\"__raw__\":\"\"}")),
        differs(
            "gzipOfNothingJsonObject", EncodingType.JSON_OBJECT, gzip(new byte[0]), List.of(FAILS)),
        differs(
            "gzipOfNothingJsonArray", EncodingType.JSON_ARRAY, gzip(new byte[0]), List.of(FAILS)),
        differs("gzipOfNothingXml", EncodingType.XML, gzip(new byte[0]), List.of(FAILS)),
        // 4.1 item 6: decoding differences
        differs(
            "jsonLinesArrayLine",
            EncodingType.JSON_OBJECT_LINE,
            utf8("[{\"a\":1},{\"a\":2}]\n"),
            List.of("{\"a\":1}", "{\"a\":2}"),
            FAILS + " MALFORMED"),
        differs(
            "jsonLinesFormFeedLine",
            EncodingType.JSON_OBJECT_LINE,
            utf8("{\"a\":1}\n\f\n{\"a\":2}\n"),
            List.of("{\"a\":1}", "{\"a\":2}"),
            FAILS + " MALFORMED"),
        differs(
            "jsonLinesInvalidUtf8",
            EncodingType.JSON_OBJECT_LINE,
            concat(utf8("{\"a\":\""), new byte[] {(byte) 0xff}, utf8("\"}\n")),
            List.of("{\"a\":\"�\"}"),
            FAILS + " MALFORMED"),
        differs(
            "jsonLinesUtf16",
            EncodingType.JSON_OBJECT_LINE,
            concat(
                new byte[] {(byte) 0xFE, (byte) 0xFF},
                "{\"a\":1}\n".getBytes(StandardCharsets.UTF_16BE)),
            List.of(FAILS),
            "{\"a\":1}"),
        differs(
            "jsonLinesUtf32",
            EncodingType.JSON_OBJECT_LINE,
            concat(
                new byte[] {0, 0, (byte) 0xFE, (byte) 0xFF},
                "{\"a\":1}\n".getBytes(Charset.forName("UTF-32BE"))),
            List.of(FAILS),
            "{\"a\":1}"),
        differs(
            "csvLongRow",
            EncodingType.CSV,
            utf8("a,b\n1,2,3\n"),
            List.of("{\"a\":\"1\",\"b\":\"2\"}"),
            FAILS + " MALFORMED"),
        differs(
            "csvDuplicateHeader",
            EncodingType.CSV,
            utf8("a,a\n1,2\n"),
            List.of("{\"a\":\"2\"}"),
            FAILS + " MALFORMED"),
        differs(
            "csvEmptyHeaderName",
            EncodingType.CSV,
            utf8("a,\n1,2\n"),
            List.of(FAILS),
            "{\"a\":\"1\",\"_c1\":\"2\"}"),
        differs(
            "csvTooManyColumns",
            EncodingType.CSV,
            utf8(columns(16_385) + "\n"),
            List.of(),
            FAILS + " MALFORMED"),
        differs(
            "xmlDoctype",
            EncodingType.XML,
            utf8("<!DOCTYPE r><r><a>1</a></r>"),
            List.of("{\"a\":\"1\"}"),
            FAILS + " MALFORMED"),
        differs(
            "xmlTextAfterTheRoot",
            EncodingType.XML,
            utf8("<r><a>1</a></r>junk"),
            List.of("{\"a\":\"1\"}"),
            FAILS + " MALFORMED"),
        differs(
            "xmlSecondRoot",
            EncodingType.XML,
            utf8("<r><a>1</a></r><s/>"),
            List.of("{\"a\":\"1\"}"),
            FAILS + " MALFORMED"),
        differs(
            "gzipTrailingBytes",
            EncodingType.JSON_OBJECT_LINE,
            concat(gzip(JSON_LINES), utf8("xx")),
            List.of("{\"a\":1}", "{\"a\":2}"),
            FAILS + " CORRUPT_COMPRESSION"),
        differs(
            "jsonLongName",
            EncodingType.JSON_OBJECT,
            utf8("{\"" + "n".repeat(50_001) + "\":1}"),
            List.of(FAILS),
            "{\"" + "n".repeat(50_001) + "\":1}"),
        differs(
            "jsonLongString",
            EncodingType.JSON_OBJECT,
            utf8("{\"s\":\"" + "x".repeat(20_000_001) + "\"}"),
            List.of(FAILS),
            "{\"s\":\"" + "x".repeat(20_000_001) + "\"}"),
        differs(
            "bomStringLine",
            EncodingType.STRING_LINE,
            utf8("﻿x\n"),
            List.of("{\"__raw__\":\"﻿x\"}"),
            "{\"__raw__\":\"x\"}"),
        differs(
            "bomText",
            EncodingType.TEXT,
            utf8("﻿x"),
            List.of("{\"__raw__\":\"﻿x\"}"),
            "{\"__raw__\":\"x\"}"),
        differs(
            "bomCsv",
            EncodingType.CSV,
            utf8("﻿a\n1\n"),
            List.of("{\"﻿a\":\"1\"}"),
            "{\"a\":\"1\"}"),
        differs(
            "bomJsonLines",
            EncodingType.JSON_OBJECT_LINE,
            utf8("﻿{\"a\":1}\n"),
            List.of(FAILS),
            "{\"a\":1}"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void legacyAndDecoderResults(
      String name, EncodingType encodingType, byte[] bytes, Observed expected) {
    SerializedEvent message = new SerializedEvent(null, bytes, null);
    assertEquals(
        expected, new Observed(legacy(encodingType, message), decoded(encodingType, message)));
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

  private static Arguments same(
      String name, EncodingType encodingType, byte[] bytes, String... records) {
    return Arguments.of(
        name, encodingType, bytes, new Observed(List.of(records), List.of(records)));
  }

  private static Arguments both(String name, EncodingType encodingType, byte[] bytes, String kind) {
    return Arguments.of(
        name, encodingType, bytes, new Observed(List.of(FAILS), List.of(FAILS + " " + kind)));
  }

  private static Arguments differs(
      String name,
      EncodingType encodingType,
      byte[] bytes,
      List<String> legacy,
      String... decoded) {
    return Arguments.of(name, encodingType, bytes, new Observed(legacy, List.of(decoded)));
  }

  private static List<String> legacy(EncodingType encodingType, SerializedEvent message) {
    BytesRawDataConverter converter =
        new BytesRawDataConverter(
            DeserializerFactory.createDeserializerFactory(encodingType).createDeserializer(),
            new GzipDetectingDecompressor());
    ConvertedResult<SerializedEvent> result = converter.convert(message, context());
    return result.transformedData() == null ? List.of(FAILS) : json(result.transformedData());
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
