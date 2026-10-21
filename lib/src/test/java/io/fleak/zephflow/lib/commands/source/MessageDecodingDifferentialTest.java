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
package io.fleak.zephflow.lib.commands.source;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.compression.GzipDetectingDecompressor;
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
            "gzipOfNothing", EncodingType.TEXT, gzip(new byte[0]), List.of("{\"__raw__\":\"\"}")),
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
        same(
            "xmlLongAttribute",
            EncodingType.XML,
            utf8("<r a=\"" + "x".repeat(524_289) + "\"/>"),
            "{\"a\":\"" + "x".repeat(524_289) + "\"}"),
        same(
            "xmlManyAttributes",
            EncodingType.XML,
            utf8("<r " + attributes(1_001) + "/>"),
            attributesJson(1_001)),
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
            MessageDecodingConverter.decodingConfig(encodingType, null, true),
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

  private static String attributes(int count) {
    return IntStream.range(0, count)
        .mapToObj(i -> "a" + i + "=\"" + i + "\"")
        .collect(Collectors.joining(" "));
  }

  private static String attributesJson(int count) {
    return JsonUtils.toJsonString(
        IntStream.range(0, count).boxed().collect(Collectors.toMap(i -> "a" + i, String::valueOf)));
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
