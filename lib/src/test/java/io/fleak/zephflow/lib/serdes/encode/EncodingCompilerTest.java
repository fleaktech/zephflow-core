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
import static io.fleak.zephflow.lib.utils.CompressionUtils.gunzip;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.serdes.decode.config.DecompressionCodec;
import io.fleak.zephflow.lib.serdes.decode.config.JsonFormatConfig.Mode;
import io.fleak.zephflow.lib.serdes.encode.WriterCapability.Unit;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class EncodingCompilerTest {

  private static final String SCHEMA =
      "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}";

  private static final WriterCapability ONE_RECORD_PARQUET =
      new WriterCapability(
          "testsink", Unit.ONE_RECORD, Set.of("parquet"), Set.of(), false, Set.of());

  private static final WriterCapability ONE_RECORD_GZIP =
      new WriterCapability(
          "testsink",
          Unit.ONE_RECORD,
          Set.of("json"),
          Set.of(Mode.LINES),
          false,
          Set.of(DecompressionCodec.GZIP));

  private static final String KV_DELIMITER_RULE =
      " must not contain letters, digits, '_', '.', ':', '/', '-', '=', '\"', '\\' or line breaks";

  static Stream<Arguments> refusals() {
    return Stream.of(
        refusal("no format", S3, "{\"compression\":[]}", "s3sink: encoding format is required"),
        refusal(
            "avro",
            KAFKA,
            format("{\"type\":\"avro\",\"container\":\"ocf\"}"),
            "kafkasink: format avro cannot be written"),
        refusal(
            "protobuf",
            KAFKA,
            format("{\"type\":\"protobuf\",\"framing\":\"single\"}"),
            "kafkasink: format protobuf cannot be written"),
        refusal(
            "yaml",
            KAFKA,
            format("{\"type\":\"yaml\",\"mode\":\"document\"}"),
            "kafkasink: format yaml cannot be written"),
        refusal(
            "xml",
            KAFKA,
            format("{\"type\":\"xml\",\"mode\":\"document\"}"),
            "kafkasink: format xml cannot be written"),
        refusal(
            "text",
            S3,
            format("{\"type\":\"text\",\"framing\":{\"type\":\"line\"}}"),
            "s3sink: format text cannot be written"),
        refusal(
            "otlp",
            S3,
            format("{\"type\":\"otlp\",\"signal\":\"logs\"}"),
            "s3sink: format otlp cannot be written"),
        refusal("format not in capability", SQS, csv(""), "sqssink cannot write csv"),
        refusal(
            "parquet not in capability",
            KAFKA,
            format("{\"type\":\"parquet\",\"schema\":" + SCHEMA + "}"),
            "kafkasink cannot write parquet"),
        refusal(
            "compression auto",
            S3,
            "{\"compression\":\"auto\",\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}",
            "s3sink: compression auto describes how a reader guesses; name the codec the data"
                + " carries"),
        refusal(
            "unwritable codec",
            S3,
            "{\"compression\":[\"zstd\"],\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}",
            "compression zstd cannot be written by s3sink"),
        refusal(
            "codec the capability does not allow",
            KAFKA,
            "{\"compression\":[\"gzip\"],\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}",
            "compression gzip cannot be written by kafkasink"),
        refusal(
            "more than one codec",
            S3,
            "{\"compression\":[\"gzip\",\"gzip\"],\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}",
            "s3sink: compression takes at most one codec when writing"),
        refusal(
            "empty codec entry",
            S3,
            "{\"compression\":[null],\"format\":{\"type\":\"json\",\"mode\":\"lines\"}}",
            "s3sink: compression must not contain empty entries"),
        refusal(
            "json without mode",
            KAFKA,
            format("{\"type\":\"json\"}"),
            "kafkasink: json format needs a mode: object, array or lines"),
        refusal(
            "json object on a many-records sink",
            S3,
            format("{\"type\":\"json\",\"mode\":\"object\"}"),
            "s3sink: json mode object holds exactly one record and s3sink writes many records per"
                + " unit; use mode array or lines"),
        refusal(
            "json mode the capability does not allow",
            SQS,
            format("{\"type\":\"json\",\"mode\":\"lines\"}"),
            "sqssink cannot write json mode lines"),
        refusal(
            "parquet on a one-record sink",
            ONE_RECORD_PARQUET,
            format("{\"type\":\"parquet\",\"schema\":" + SCHEMA + "}"),
            "testsink: parquet holds many records per unit and testsink writes one record per"
                + " unit"),
        refusal(
            "parquet without schema",
            S3,
            format("{\"type\":\"parquet\"}"),
            "s3sink: parquet option schema is required when writing"),
        refusal(
            "parquet schema null",
            S3,
            format("{\"type\":\"parquet\",\"schema\":null}"),
            "s3sink: parquet option schema is required when writing"),
        refusal(
            "parquet schema neither object nor string",
            S3,
            format("{\"type\":\"parquet\",\"schema\":[1]}"),
            "s3sink: parquet option schema must be an avro record schema, as a JSON object or a"
                + " string holding one"),
        refusal(
            "parquet schema string that is not JSON",
            S3,
            format("{\"type\":\"parquet\",\"schema\":\"not json\"}"),
            "s3sink: parquet option schema must be an avro record schema, as a JSON object or a"
                + " string holding one"),
        refusal(
            "parquet schema not a record",
            S3,
            format("{\"type\":\"parquet\",\"schema\":{\"type\":\"string\"}}"),
            "s3sink: parquet option schema is not a valid avro record schema: Avro schema must be"
                + " a RECORD type, got: STRING"),
        refusal(
            "parquet with compression",
            S3,
            "{\"compression\":[\"gzip\"],\"format\":{\"type\":\"parquet\",\"schema\":"
                + SCHEMA
                + "}}",
            "s3sink: parquet cannot be combined with compression; its pages carry their own"),
        refusal(
            "csv header off without columns",
            KAFKA,
            csv(",\"header\":false"),
            "kafkasink: csv option columns must be set when header is false"),
        refusal(
            "csv with header on where header off is required",
            SPLUNK,
            csv(",\"columns\":[\"a\"]"),
            "splunkhecsink writes csv only with header false and columns set"),
        refusal(
            "csv with header default where header off is required",
            SPLUNK,
            csv(""),
            "splunkhecsink writes csv only with header false and columns set"),
        refusal(
            "csv skipLines",
            S3,
            csv(",\"skipLines\":1"),
            "s3sink: csv option skipLines cannot be used when writing"),
        refusal(
            "csv commentMarker",
            S3,
            csv(",\"commentMarker\":\"#\""),
            "s3sink: csv option commentMarker cannot be used when writing"),
        refusal(
            "csv quoting off",
            S3,
            csv(",\"quote\":\"\""),
            "s3sink: csv with quoting off cannot be written without losing data"),
        refusal(
            "csv non-UTF-8 charset",
            S3,
            csv(",\"charset\":\"UTF-16\""),
            "s3sink: csv option charset UTF-16 cannot be used when writing; csv is written as"
                + " UTF-8"),
        refusal(
            "csv unknown charset",
            S3,
            csv(",\"charset\":\"nope\""),
            "s3sink: csv option charset nope cannot be used when writing; csv is written as UTF-8"),
        refusal(
            "csv delimiter of two chars",
            S3,
            csv(",\"delimiter\":\";;\""),
            "s3sink: csv option delimiter must be a single ASCII character other than a line"
                + " ending"),
        refusal(
            "csv empty delimiter",
            S3,
            csv(",\"delimiter\":\"\""),
            "s3sink: csv option delimiter must be a single ASCII character other than a line"
                + " ending"),
        refusal(
            "csv line break delimiter",
            S3,
            csv(",\"delimiter\":\"\\n\""),
            "s3sink: csv option delimiter must be a single ASCII character other than a line"
                + " ending"),
        refusal(
            "csv non-ASCII delimiter",
            S3,
            csv(",\"delimiter\":\"\u00a7\""),
            "s3sink: csv option delimiter must be a single ASCII character other than a line"
                + " ending"),
        refusal(
            "csv quote of two chars",
            S3,
            csv(",\"quote\":\"ab\""),
            "s3sink: csv option quote must be a single ASCII character other than a line ending"),
        refusal(
            "csv line break escape",
            S3,
            csv(",\"escape\":\"\\r\""),
            "s3sink: csv option escape must be a single ASCII character other than a line ending"),
        refusal(
            "csv delimiter equal to quote",
            S3,
            csv(",\"delimiter\":\"'\",\"quote\":\"'\""),
            "s3sink: csv options delimiter, quote and escape must be different characters"),
        refusal(
            "csv escape equal to delimiter",
            S3,
            csv(",\"delimiter\":\";\",\"escape\":\";\""),
            "s3sink: csv options delimiter, quote and escape must be different characters"),
        refusal(
            "csv escape equal to the default quote",
            S3,
            csv(",\"escape\":\"\\\"\""),
            "s3sink: csv options delimiter, quote and escape must be different characters"),
        refusal(
            "csv empty columns",
            S3,
            csv(",\"columns\":[]"),
            "s3sink: csv option columns must not be empty"),
        refusal(
            "csv blank column",
            S3,
            csv(",\"columns\":[\"a\",\" \\u200b\"]"),
            "s3sink: csv option columns must not hold blank names"),
        refusal(
            "csv column twice after normalizing",
            S3,
            csv(",\"columns\":[\"a\",\" a\"]"),
            "s3sink: csv option columns holds a twice"),
        refusal(
            "kv bad pair delimiter",
            KAFKA,
            format("{\"type\":\"kv\",\"pairDelimiter\":\"=\"}"),
            "kafkasink: kv option pairDelimiter" + KV_DELIMITER_RULE),
        refusal(
            "kv line break pair delimiter",
            KAFKA,
            format("{\"type\":\"kv\",\"pairDelimiter\":\" \\n\"}"),
            "kafkasink: kv option pairDelimiter" + KV_DELIMITER_RULE),
        refusal(
            "kv blank field",
            KAFKA,
            format("{\"type\":\"kv\",\"fields\":[\"a\",\" \"]}"),
            "kafkasink: kv option fields must not hold blank names"),
        refusal(
            "kv field twice after normalizing",
            KAFKA,
            format("{\"type\":\"kv\",\"fields\":[\"a\",\"a \"]}"),
            "kafkasink: kv option fields holds a twice"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("refusals")
  void configErrorsNameTheSinkTheFormatAndTheOption(
      String name, WriterCapability capability, String json, String message) {
    EncodingConfig config = encoding(json);
    assertEquals(
        message,
        assertThrows(
                IllegalArgumentException.class, () -> EncodingCompiler.validate(config, capability))
            .getMessage());
  }

  @Test
  void acceptsWhatEachSinkCanWrite() {
    record Accepted(WriterCapability capability, String json) {}
    List<Accepted> accepted =
        List.of(
            new Accepted(S3, csv("")),
            new Accepted(
                S3,
                csv(
                    ",\"delimiter\":\",\",\"quote\":\"\\\"\",\"escape\":\"\",\"skipLines\":0,"
                        + "\"commentMarker\":\"\",\"ignoreSurroundingSpaces\":true,"
                        + "\"charset\":\"utf-8\",\"header\":true")),
            new Accepted(S3, "{\"compression\":\"gzip\",\"format\":{\"type\":\"csv\"}}"),
            new Accepted(
                S3, "{\"compression\":[],\"format\":{\"type\":\"json\",\"mode\":\"array\"}}"),
            new Accepted(S3, format("{\"type\":\"json\",\"mode\":\"lines\"}")),
            new Accepted(S3, format("{\"type\":\"kv\",\"fields\":[],\"pairDelimiter\":\"\"}")),
            new Accepted(S3, format("{\"type\":\"parquet\",\"schema\":" + SCHEMA + "}")),
            new Accepted(
                S3,
                format(
                    "{\"type\":\"parquet\",\"schema\":\"" + SCHEMA.replace("\"", "\\\"") + "\"}")),
            new Accepted(KAFKA, format("{\"type\":\"json\",\"mode\":\"object\"}")),
            new Accepted(KAFKA, csv(",\"delimiter\":\"|\",\"quote\":\"'\",\"escape\":\"\\\\\"")),
            new Accepted(SPLUNK, csv(",\"header\":false,\"columns\":[\"a\",\"b\"]")),
            new Accepted(SQS, format("{\"type\":\"json\",\"mode\":\"object\"}")));
    List<String> refused = new ArrayList<>();
    for (Accepted a : accepted) {
      try {
        EncodingCompiler.validate(encoding(a.json()), a.capability());
      } catch (IllegalArgumentException e) {
        refused.add(a.json() + " -> " + e.getMessage());
      }
    }
    assertEquals(List.of(), refused);
  }

  @Test
  void recordEncoderAndFileWriterNeedTheMatchingUnit() {
    EncodingConfig csv = encoding(csv(""));
    assertEquals(
        List.of(
            "s3sink writes many records per unit; it needs a file writer, not a record encoder",
            "kafkasink writes one record per unit; it needs a record encoder, not a file writer"),
        List.of(
            assertThrows(
                    IllegalArgumentException.class, () -> EncodingCompiler.recordEncoder(csv, S3))
                .getMessage(),
            assertThrows(
                    IllegalArgumentException.class, () -> EncodingCompiler.fileWriter(csv, KAFKA))
                .getMessage()));
  }

  @Test
  void recordEncoderGzipsEachRecordWhenTheSinkAllowsIt() throws Exception {
    RecordFleakData record = json("{\"id\":1,\"name\":\"a\"}");
    String lines = "{\"type\":\"json\",\"mode\":\"lines\"}";
    byte[] plain =
        EncodingCompiler.recordEncoder(encoding("[]", lines), ONE_RECORD_GZIP).encode(record);
    byte[] gzipped =
        EncodingCompiler.recordEncoder(encoding("[\"gzip\"]", lines), ONE_RECORD_GZIP)
            .encode(record);
    assertArrayEquals(plain, gunzip(gzipped));
  }

  private static Arguments refusal(
      String name, WriterCapability capability, String json, String message) {
    return Arguments.of(name, capability, json, message);
  }

  private static String format(String format) {
    return "{\"format\":" + format + "}";
  }

  private static String csv(String options) {
    return format("{\"type\":\"csv\"" + options + "}");
  }
}
