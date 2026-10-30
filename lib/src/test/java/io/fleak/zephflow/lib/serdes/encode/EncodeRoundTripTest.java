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
package io.fleak.zephflow.lib.serdes.encode;

import static io.fleak.zephflow.lib.serdes.encode.EncodeTestSupport.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.decode.DecodeResult;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class EncodeRoundTripTest {

  private static final String NONE = "[]";
  private static final String GZIP = "[\"gzip\"]";

  /** Values holding every character csv has to protect, under the same keys in each record. */
  private static final List<RecordFleakData> TRICKY =
      List.of(
          record(
              "comma", "a,b",
              "pipe", "a|b",
              "dquote", "say \"hi\"",
              "squote", "it's",
              "backslash", "C:\\dir\\",
              "cr", "x\ry",
              "lf", "x\ny",
              "crlf", "x\r\ny",
              "spaces", "  padded  ",
              "empty", ""),
          record(
              "comma", ",",
              "pipe", "|",
              "dquote", "\"",
              "squote", "'",
              "backslash", "\\",
              "cr", "\r",
              "lf", "\n",
              "crlf", "\r\n",
              "spaces", " ",
              "empty", ""));

  private static final String TRICKY_COLUMNS =
      "[\"comma\",\"pipe\",\"dquote\",\"squote\",\"backslash\",\"cr\",\"lf\",\"crlf\",\"spaces\","
          + "\"empty\"]";

  private static final List<RecordFleakData> ONE_COLUMN =
      List.of(record("a", ""), record("a", "x"), record("a", ""));

  private static final List<RecordFleakData> JSON_RECORDS =
      List.of(
          json(
              "{\"s\":\"line\\nbreak \\\"q\\\" \u00e9\",\"n\":7,\"d\":1.5,\"b\":true,\"z\":null,"
                  + "\"nested\":{\"k\":[1,\"two\",null,{\"deep\":false}]},\"arr\":[]}"),
          json("{\"other\":\"r2\",\"n\":-3}"));

  @TempDir Path dir;

  static Stream<Arguments> csvCases() {
    return Stream.of(
        Arguments.of("{\"type\":\"csv\"}", TRICKY),
        Arguments.of("{\"type\":\"csv\"}", ONE_COLUMN),
        Arguments.of("{\"type\":\"csv\",\"delimiter\":\"|\"}", TRICKY),
        Arguments.of("{\"type\":\"csv\",\"delimiter\":\"\\t\"}", TRICKY),
        Arguments.of("{\"type\":\"csv\",\"escape\":\"\\\\\"}", TRICKY),
        Arguments.of(
            "{\"type\":\"csv\",\"delimiter\":\";\",\"quote\":\"'\",\"escape\":\"\\\\\"}", TRICKY),
        Arguments.of("{\"type\":\"csv\",\"quote\":\"'\"}", TRICKY),
        Arguments.of(
            "{\"type\":\"csv\",\"header\":false,\"columns\":" + TRICKY_COLUMNS + "}", TRICKY),
        Arguments.of("{\"type\":\"csv\",\"header\":false,\"columns\":[\"a\"]}", ONE_COLUMN));
  }

  @ParameterizedTest
  @MethodSource("csvCases")
  void csvRoundTripsInRecordAndFileMode(String format, List<RecordFleakData> records)
      throws Exception {
    assertRoundTripsInRecordMode(format, records);
    assertRoundTripsInFileMode(format, NONE, records);
    assertRoundTripsInFileMode(format, GZIP, records);
  }

  @ParameterizedTest
  @MethodSource("jsonManyRecordModes")
  void jsonArrayAndLinesRoundTripInRecordAndFileMode(String format) throws Exception {
    assertRoundTripsInRecordMode(format, JSON_RECORDS);
    assertRoundTripsInFileMode(format, NONE, JSON_RECORDS);
    assertRoundTripsInFileMode(format, GZIP, JSON_RECORDS);
  }

  static Stream<String> jsonManyRecordModes() {
    return Stream.of(
        "{\"type\":\"json\",\"mode\":\"array\"}", "{\"type\":\"json\",\"mode\":\"lines\"}");
  }

  @Test
  void jsonObjectRoundTripsInRecordMode() throws Exception {
    assertRoundTripsInRecordMode("{\"type\":\"json\",\"mode\":\"object\"}", JSON_RECORDS);
  }

  @Test
  void parquetRoundTripsInFileMode() throws Exception {
    String format =
        "{\"type\":\"parquet\",\"schema\":{\"type\":\"record\",\"name\":\"R\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"long\"},"
            + "{\"name\":\"name\",\"type\":\"string\"},"
            + "{\"name\":\"score\",\"type\":\"double\"},"
            + "{\"name\":\"ok\",\"type\":\"boolean\"},"
            + "{\"name\":\"note\",\"type\":[\"null\",\"string\"]}]}}";
    assertRoundTripsInFileMode(
        format,
        NONE,
        List.of(
            json("{\"id\":1,\"name\":\"a\",\"score\":1.5,\"ok\":true,\"note\":\"x, \\\"y\\\"\"}"),
            json("{\"id\":2,\"name\":\"b\",\"score\":-2.25,\"ok\":false}")));
  }

  private void assertRoundTripsInRecordMode(String format, List<RecordFleakData> records)
      throws Exception {
    RecordEncoder encoder = EncodingCompiler.recordEncoder(encoding(NONE, format), KAFKA);
    List<DecodeResult> expected = new ArrayList<>();
    List<DecodeResult> actual = new ArrayList<>();
    for (RecordFleakData record : records) {
      expected.add(new DecodeResult(List.of(record), List.of()));
      actual.add(decode(decoding(NONE, format), encoder.encode(record)));
    }
    assertEquals(expected, actual);
  }

  private void assertRoundTripsInFileMode(
      String format, String compression, List<RecordFleakData> records) throws Exception {
    List<byte[]> files =
        writeFiles(EncodingCompiler.fileWriter(encoding(compression, format), S3), records, dir);
    assertEquals(
        new DecodeResult(records, List.of()), decodeFiles(decoding(compression, format), files));
  }
}
