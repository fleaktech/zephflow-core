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

import static io.fleak.zephflow.api.ConfigurationPath.of;
import static io.fleak.zephflow.lib.serdes.encode.EncodeTestSupport.*;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.ConfigurationIssue;
import io.fleak.zephflow.api.ConfigurationPath;
import io.fleak.zephflow.api.ConfigurationValidationException;
import io.fleak.zephflow.lib.commands.sink.SinkEncoding;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class EncodingConfigurationDiagnosticsTest {
  private static final WriterCapability TEXT_FILES =
      new WriterCapability(
          "textsink",
          WriterCapability.Unit.MANY_RECORDS,
          Set.of("text"),
          Set.of(),
          false,
          Set.of());
  private static final WriterCapability PARQUET_MESSAGE =
      new WriterCapability(
          "testsink",
          WriterCapability.Unit.ONE_RECORD,
          Set.of("parquet"),
          Set.of(),
          false,
          Set.of());

  static Stream<Arguments> formatAndCompressionRejections() {
    return Stream.of(
        Arguments.of(S3, encoding("{}"), of("format"), "required"),
        Arguments.of(S3, format("{\"type\":\"auto\"}"), of("format", "type"), "allowed formats"),
        Arguments.of(SQS, format("{\"type\":\"csv\"}"), of("format", "type"), "[json]"),
        Arguments.of(
            S3,
            encoding("[\"gzip\",null]", "{\"type\":\"json\",\"mode\":\"lines\"}"),
            of("compression").index(1),
            "null"),
        Arguments.of(
            S3,
            encoding("[\"auto\"]", "{\"type\":\"json\",\"mode\":\"lines\"}"),
            of("compression").index(0),
            "gzip"),
        Arguments.of(
            S3,
            encoding("[\"gzip\",\"gzip\"]", "{\"type\":\"json\",\"mode\":\"lines\"}"),
            of("compression"),
            "at most one"),
        Arguments.of(
            S3,
            encoding("[\"zstd\"]", "{\"type\":\"json\",\"mode\":\"lines\"}"),
            of("compression").index(0),
            "gzip"),
        Arguments.of(
            KAFKA,
            encoding("[\"gzip\"]", "{\"type\":\"json\",\"mode\":\"lines\"}"),
            of("compression").index(0),
            "omitted or empty"));
  }

  static Stream<Arguments> csvOptionRejections() {
    return Stream.of(
        Arguments.of(
            KAFKA,
            format("{\"type\":\"csv\",\"quote\":\"\"}"),
            of("format", "quote"),
            "cannot be disabled"),
        Arguments.of(
            KAFKA,
            format("{\"type\":\"csv\",\"skipLines\":1}"),
            of("format", "skipLines"),
            "absent or 0"),
        Arguments.of(
            KAFKA,
            format("{\"type\":\"csv\",\"commentMarker\":\"#\"}"),
            of("format", "commentMarker"),
            "absent or empty"),
        Arguments.of(
            KAFKA,
            format("{\"type\":\"csv\",\"charset\":\"do-not-copy-this\"}"),
            of("format", "charset"),
            "UTF-8"),
        Arguments.of(
            KAFKA,
            format("{\"type\":\"csv\",\"delimiter\":\"ab\"}"),
            of("format", "delimiter"),
            "single ASCII"),
        Arguments.of(
            KAFKA,
            format("{\"type\":\"csv\",\"escape\":\"ab\"}"),
            of("format", "escape"),
            "single ASCII"));
  }

  static Stream<Arguments> csvColumnRejections() {
    return Stream.of(
        Arguments.of(
            KAFKA,
            format("{\"type\":\"csv\",\"columns\":[]}"),
            of("format", "columns"),
            "at least one"),
        Arguments.of(
            KAFKA,
            format("{\"type\":\"csv\",\"columns\":[\"a\",null]}"),
            of("format", "columns").index(1),
            "must not be blank"),
        Arguments.of(
            KAFKA,
            format("{\"type\":\"csv\",\"columns\":[\"a\",\" a \"]}"),
            of("format", "columns").index(1),
            "unique"),
        Arguments.of(
            KAFKA,
            format("{\"type\":\"csv\",\"header\":false}"),
            of("format", "columns"),
            "when header is false"),
        Arguments.of(
            SPLUNK,
            format("{\"type\":\"csv\",\"columns\":[\"a\"]}"),
            of("format", "header"),
            "header false"));
  }

  static Stream<Arguments> jsonAndKeyValueRejections() {
    return Stream.of(
        Arguments.of(
            KAFKA, format("{\"type\":\"json\"}"), of("format", "mode"), "Allowed JSON modes"),
        Arguments.of(
            S3,
            format("{\"type\":\"json\",\"mode\":\"object\"}"),
            of("format", "mode"),
            "[array, lines]"),
        Arguments.of(
            SQS,
            format("{\"type\":\"json\",\"mode\":\"lines\"}"),
            of("format", "mode"),
            "[object]"),
        Arguments.of(
            SPLUNK,
            format("{\"type\":\"kv\",\"fields\":[\"a\",\"a\"]}"),
            of("format", "fields").index(1),
            "unique"),
        Arguments.of(
            SPLUNK,
            format("{\"type\":\"kv\",\"fields\":[\"a\",\" \"]}"),
            of("format", "fields").index(1),
            "blank"),
        Arguments.of(
            SPLUNK,
            format("{\"type\":\"kv\",\"pairDelimiter\":\"do-not-copy-this\"}"),
            of("format", "pairDelimiter"),
            "must not contain"));
  }

  static Stream<Arguments> textAndParquetRejections() {
    return Stream.of(
        Arguments.of(
            TEXT_FILES,
            format("{\"type\":\"text\",\"charset\":\"UTF-16\"}"),
            of("format", "charset"),
            "UTF-8"),
        Arguments.of(
            TEXT_FILES,
            format("{\"type\":\"text\"}"),
            of("format", "framing"),
            "line, multiline, whole"),
        Arguments.of(
            TEXT_FILES,
            format(
                "{\"type\":\"text\",\"framing\":{\"type\":\"multiline\",\"startPattern\":\"do-not-copy-this(\"}}"),
            of("format", "framing", "startPattern"),
            "index"),
        Arguments.of(
            TEXT_FILES,
            format("{\"type\":\"text\",\"framing\":{\"type\":\"whole\"}}"),
            of("format", "framing", "type"),
            "line or multiline"),
        Arguments.of(
            PARQUET_MESSAGE,
            format("{\"type\":\"parquet\"}"),
            of("format", "type"),
            "many records"),
        Arguments.of(
            S3,
            encoding("[\"gzip\"]", "{\"type\":\"parquet\"}"),
            of("compression"),
            "outer compression"),
        Arguments.of(S3, format("{\"type\":\"parquet\"}"), of("format", "schema"), "required"),
        Arguments.of(
            S3,
            format("{\"type\":\"parquet\",\"schema\":\"do-not-copy-this\"}"),
            of("format", "schema"),
            "JSON object"),
        Arguments.of(
            S3,
            format("{\"type\":\"parquet\",\"schema\":{\"type\":\"string\"}}"),
            of("format", "schema", "type"),
            "record"));
  }

  @ParameterizedTest
  @MethodSource({
    "formatAndCompressionRejections",
    "csvOptionRejections",
    "csvColumnRejections",
    "jsonAndKeyValueRejections",
    "textAndParquetRejections"
  })
  void reportsTheSinkSpecificConstraintUnderEncoding(
      WriterCapability capability,
      EncodingConfig config,
      ConfigurationPath path,
      String constraint) {
    var failure =
        assertThrows(
            ConfigurationValidationException.class,
            () -> SinkEncoding.validate(config, capability));
    assertEquals(
        List.of(of("encoding").append(path)),
        failure.issues().stream().map(ConfigurationIssue::path).toList());
    assertTrue(
        failure.issues().getFirst().message().contains(constraint), failure.issues().toString());
    assertFalse(failure.issues().getFirst().message().contains("do-not-copy-this"));
    assertEquals(IllegalArgumentException.class, failure.originalFailure().getClass());
    assertEquals(failure.originalFailure().getMessage(), failure.getMessage());
    assertSame(failure.originalFailure(), failure.getCause());
  }

  @Test
  void namesOnlyConflictingCsvCharacters() {
    var failure =
        assertThrows(
            ConfigurationValidationException.class,
            () ->
                SinkEncoding.validate(
                    format("{\"type\":\"csv\",\"quote\":\"#\",\"escape\":\"#\"}"), KAFKA));
    assertEquals(
        List.of(of("encoding", "format", "quote"), of("encoding", "format", "escape")),
        failure.issues().stream().map(ConfigurationIssue::path).toList());
  }

  @Test
  void preservesDefaultsAndBoundaryConfigurations() {
    assertDoesNotThrow(
        () ->
            SinkEncoding.validate(
                format(
                    "{\"type\":\"csv\",\"skipLines\":0,\"commentMarker\":\"\",\"escape\":\"\",\"charset\":\"UTF8\"}"),
                KAFKA));
    assertDoesNotThrow(
        () ->
            SinkEncoding.validate(
                format("{\"type\":\"csv\",\"header\":false,\"columns\":[\"a\"]}"), SPLUNK));
    assertDoesNotThrow(
        () ->
            SinkEncoding.validate(
                format("{\"type\":\"kv\",\"fields\":[],\"pairDelimiter\":\"\"}"), SPLUNK));
    assertDoesNotThrow(
        () ->
            SinkEncoding.validate(
                format("{\"type\":\"kv\",\"fields\":[\"a\"],\"pairDelimiter\":\"|\"}"), SPLUNK));
    assertDoesNotThrow(
        () -> SinkEncoding.validate(format("{\"type\":\"json\",\"mode\":\"object\"}"), SQS));
    assertDoesNotThrow(
        () ->
            SinkEncoding.validate(
                encoding("[\"gzip\"]", "{\"type\":\"json\",\"mode\":\"lines\"}"), S3));
    assertDoesNotThrow(
        () ->
            SinkEncoding.validate(
                format("{\"type\":\"text\",\"framing\":{\"type\":\"line\"}}"), TEXT_FILES));
    assertDoesNotThrow(
        () ->
            SinkEncoding.validate(
                format(
                    "{\"type\":\"parquet\",\"schema\":{\"type\":\"record\",\"name\":\"Event\",\"fields\":[{\"name\":\"id\",\"type\":\"long\"}]}}"),
                S3));
  }

  @Test
  void derivesCapabilitiesFromTheCallerAndReportsMissingEncoding() {
    var failure =
        assertThrows(ConfigurationValidationException.class, () -> SinkEncoding.validate(null, S3));
    assertEquals(of("encoding"), failure.issues().getFirst().path());
    var custom =
        new WriterCapability(
            "custom",
            WriterCapability.Unit.ONE_RECORD,
            Set.of("json"),
            Set.of(JsonFormatConfig.Mode.ARRAY),
            false,
            Set.of(DecompressionCodec.GZIP));
    var modeFailure =
        assertThrows(
            ConfigurationValidationException.class,
            () -> SinkEncoding.validate(format("{\"type\":\"json\",\"mode\":\"object\"}"), custom));
    assertEquals(
        "Allowed JSON modes for custom: [array].", modeFailure.issues().getFirst().message());
    assertDoesNotThrow(
        () ->
            SinkEncoding.validate(
                encoding("[\"gzip\"]", "{\"type\":\"json\",\"mode\":\"array\"}"), custom));
  }
}
