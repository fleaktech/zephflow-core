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
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.BlobFileWriter;
import io.fleak.zephflow.lib.commands.sink.ParquetBlobFileWriter;
import io.fleak.zephflow.lib.commands.sink.TextBlobFileWriter;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.compression.GzipCompressor;
import io.fleak.zephflow.lib.serdes.compression.NoopCompressor;
import io.fleak.zephflow.lib.serdes.decode.DecodeResult;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ParquetFormatConfig;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import io.fleak.zephflow.lib.serdes.ser.csv.CsvSerializerOptions;
import io.fleak.zephflow.lib.serdes.ser.kv.KvSerializerOptions;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class LegacyEquivalenceTest {

  private static final List<RecordFleakData> RECORDS =
      List.of(
          json(
              "{\"id\":1,\"name\":\"Alice\",\"score\":9.5,\"ok\":true,\"tags\":[\"a\",\"b\"],"
                  + "\"meta\":{\"k\":\"v\",\"n\":2},\"event.id\":4624,\"src ip\":\"10.0.0.1\","
                  + "\"note\":\"has, comma \\\"q\\\"\\nand a line break\",\"empty\":\"\"}"),
          json("{\"id\":2,\"name\":\"Bob\",\"city\":\"New York\",\"none\":null,\"Name\":\"dup\"}"),
          json("{\"id\":3,\"name\":\" spaced \",\"note\":\"a=b\\\\c\"}"));

  private static final List<RecordFleakData> ONE_NULL_COLUMN =
      List.of(json("{\"a\":null}"), json("{\"a\":\"x\"}"));

  private static final List<String> KV_FIELDS = List.of("name", "event.id", "src ip", "missing");

  private static final String KV_WITH_OPTIONS =
      "{\"type\":\"kv\",\"fields\":[\"name\",\"event.id\",\"src ip\",\"missing\"],"
          + "\"pairDelimiter\":\" | \",\"cleanFields\":true}";

  private static final Map<String, Object> AVRO_SCHEMA =
      Map.of(
          "type",
          "record",
          "name",
          "Row",
          "fields",
          List.of(
              Map.of("name", "id", "type", "int"),
              Map.of("name", "name", "type", "string"),
              Map.of("name", "score", "type", List.of("null", "double")),
              Map.of(
                  "name",
                  "tags",
                  "type",
                  List.of("null", Map.of("type", "array", "items", "string")))));

  @TempDir Path dir;

  record Written(String extension, String bytes) {}

  static Stream<Arguments> fileFormats() {
    KvSerializerOptions kvOptions = new KvSerializerOptions(KV_FIELDS, " | ", true);
    return Stream.of(
        Arguments.of("{\"type\":\"csv\"}", EncodingType.CSV, KvSerializerOptions.DEFAULT, RECORDS),
        Arguments.of(
            "{\"type\":\"csv\"}", EncodingType.CSV, KvSerializerOptions.DEFAULT, ONE_NULL_COLUMN),
        Arguments.of(
            "{\"type\":\"csv\"}", EncodingType.CSV, KvSerializerOptions.DEFAULT, List.of()),
        Arguments.of(
            "{\"type\":\"csv\",\"delimiter\":\",\",\"quote\":\"\\\"\",\"escape\":\"\",\"skipLines\":0,"
                + "\"ignoreSurroundingSpaces\":true,\"charset\":\"UTF-8\",\"header\":true}",
            EncodingType.CSV,
            KvSerializerOptions.DEFAULT,
            RECORDS),
        Arguments.of(
            "{\"type\":\"json\",\"mode\":\"array\"}",
            EncodingType.JSON_ARRAY,
            KvSerializerOptions.DEFAULT,
            RECORDS),
        Arguments.of(
            "{\"type\":\"json\",\"mode\":\"array\"}",
            EncodingType.JSON_ARRAY,
            KvSerializerOptions.DEFAULT,
            List.of()),
        Arguments.of(
            "{\"type\":\"json\",\"mode\":\"lines\"}",
            EncodingType.JSON_OBJECT_LINE,
            KvSerializerOptions.DEFAULT,
            RECORDS),
        Arguments.of("{\"type\":\"kv\"}", EncodingType.KV, KvSerializerOptions.DEFAULT, RECORDS),
        Arguments.of(KV_WITH_OPTIONS, EncodingType.KV, kvOptions, RECORDS));
  }

  @ParameterizedTest
  @MethodSource("fileFormats")
  void fileModeWritesTheLegacyFileWithAndWithoutGzip(
      String format,
      EncodingType legacyType,
      KvSerializerOptions kvOptions,
      List<RecordFleakData> records)
      throws Exception {
    FleakSerializer<?> legacy =
        legacySerializer(legacyType, CsvSerializerOptions.DEFAULT, kvOptions);
    List<Written> expected =
        List.of(
            written(new TextBlobFileWriter(legacy, legacyType, new NoopCompressor()), records),
            written(new TextBlobFileWriter(legacy, legacyType, new GzipCompressor()), records));
    List<Written> actual =
        List.of(
            written(EncodingCompiler.fileWriter(encoding("[]", format), S3), records),
            written(EncodingCompiler.fileWriter(encoding("[\"gzip\"]", format), S3), records));
    assertEquals(expected, actual);
  }

  static Stream<Arguments> recordFormats() {
    List<String> splunkColumns = List.of("id", " name​", "city");
    return Stream.of(
        Arguments.of(
            "{\"type\":\"csv\"}",
            KAFKA,
            EncodingType.CSV,
            CsvSerializerOptions.DEFAULT,
            KvSerializerOptions.DEFAULT),
        Arguments.of(
            "{\"type\":\"csv\",\"header\":false,\"columns\":[\"id\",\" name\\u200b\",\"city\"]}",
            SPLUNK,
            EncodingType.CSV,
            new CsvSerializerOptions(splunkColumns, false),
            KvSerializerOptions.DEFAULT),
        Arguments.of(
            "{\"type\":\"json\",\"mode\":\"object\"}",
            KAFKA,
            EncodingType.JSON_OBJECT,
            CsvSerializerOptions.DEFAULT,
            KvSerializerOptions.DEFAULT),
        Arguments.of(
            "{\"type\":\"json\",\"mode\":\"array\"}",
            KAFKA,
            EncodingType.JSON_ARRAY,
            CsvSerializerOptions.DEFAULT,
            KvSerializerOptions.DEFAULT),
        Arguments.of(
            "{\"type\":\"json\",\"mode\":\"lines\"}",
            KAFKA,
            EncodingType.JSON_OBJECT_LINE,
            CsvSerializerOptions.DEFAULT,
            KvSerializerOptions.DEFAULT),
        Arguments.of(
            "{\"type\":\"kv\"}",
            KAFKA,
            EncodingType.KV,
            CsvSerializerOptions.DEFAULT,
            KvSerializerOptions.DEFAULT),
        Arguments.of(
            KV_WITH_OPTIONS,
            SPLUNK,
            EncodingType.KV,
            CsvSerializerOptions.DEFAULT,
            new KvSerializerOptions(KV_FIELDS, " | ", true)));
  }

  @ParameterizedTest
  @MethodSource("recordFormats")
  void recordModeWritesTheLegacyBytesOfOneRecord(
      String format,
      WriterCapability capability,
      EncodingType legacyType,
      CsvSerializerOptions csvOptions,
      KvSerializerOptions kvOptions)
      throws Exception {
    FleakSerializer<?> legacy = legacySerializer(legacyType, csvOptions, kvOptions);
    RecordEncoder encoder = EncodingCompiler.recordEncoder(encoding("[]", format), capability);
    List<String> expected = new ArrayList<>();
    List<String> actual = new ArrayList<>();
    for (RecordFleakData record : RECORDS) {
      expected.add(latin1(legacy.serialize(List.of(record)).value()));
      actual.add(latin1(encoder.encode(record)));
    }
    assertEquals(expected, actual);
  }

  @Test
  void parquetFilesReadBackAsTheLegacyRecords() throws Exception {
    String schema = JsonUtils.toJsonString(AVRO_SCHEMA);
    List<RecordFleakData> records =
        List.of(
            json("{\"id\":1,\"name\":\"a\",\"score\":1.5,\"tags\":[\"x\",\"y\"]}"),
            json("{\"id\":2,\"name\":\"b\",\"score\":null}"));
    DecodingConfig parquet = new DecodingConfig(null, new ParquetFormatConfig());
    DecodeResult expected =
        decodeFiles(parquet, writeFiles(new ParquetBlobFileWriter(AVRO_SCHEMA), records, dir));
    List<Object> actual = new ArrayList<>();
    for (String schemaJson : List.of(schema, JsonUtils.toJsonString(schema))) {
      BlobFileWriter<RecordFleakData> writer =
          EncodingCompiler.fileWriter(
              encoding("{\"format\":{\"type\":\"parquet\",\"schema\":" + schemaJson + "}}"), S3);
      actual.add(writer.getFileExtension());
      actual.add(decodeFiles(parquet, writeFiles(writer, records, dir)));
    }
    assertEquals(List.of("parquet", expected, "parquet", expected), actual);
  }

  @Test
  void perRecordFailuresThrowAsTheLegacyWritersDo() throws Exception {
    RecordFleakData noKvField = json("{\"other\":1}");
    RecordFleakData badParquet = json("{\"id\":\"not a number\",\"name\":\"a\"}");
    KvSerializerOptions kvOptions = new KvSerializerOptions(KV_FIELDS, " | ", true);
    EncodingConfig kv = encoding("{\"format\":" + KV_WITH_OPTIONS + "}");
    EncodingConfig csv = encoding("{\"format\":{\"type\":\"csv\"}}");
    BlobFileWriter<RecordFleakData> legacyKv =
        new TextBlobFileWriter(
            legacySerializer(EncodingType.KV, CsvSerializerOptions.DEFAULT, kvOptions),
            EncodingType.KV,
            new NoopCompressor());
    BlobFileWriter<RecordFleakData> legacyCsv =
        new TextBlobFileWriter(
            legacySerializer(EncodingType.CSV, CsvSerializerOptions.DEFAULT, kvOptions),
            EncodingType.CSV,
            new NoopCompressor());
    BlobFileWriter<RecordFleakData> legacyParquet = new ParquetBlobFileWriter(AVRO_SCHEMA);
    BlobFileWriter<RecordFleakData> parquet =
        EncodingCompiler.fileWriter(
            encoding(
                "{\"format\":{\"type\":\"parquet\",\"schema\":"
                    + JsonUtils.toJsonString(AVRO_SCHEMA)
                    + "}}"),
            S3);
    assertEquals(
        List.of(
            failure(() -> legacyKv.validateRecord(noKvField)),
            failure(() -> legacyKv.validateRecord(noKvField)),
            failure(() -> legacyKv.validateRecord(null)),
            failure(() -> legacyCsv.validateRecord(null)),
            failure(() -> legacyParquet.validateRecord(badParquet)),
            failure(() -> legacyParquet.validateRecord(null))),
        List.of(
            failure(() -> EncodingCompiler.recordEncoder(kv, KAFKA).encode(noKvField)),
            failure(() -> EncodingCompiler.fileWriter(kv, S3).validateRecord(noKvField)),
            failure(() -> EncodingCompiler.fileWriter(kv, S3).validateRecord(null)),
            failure(() -> EncodingCompiler.fileWriter(csv, S3).validateRecord(null)),
            failure(() -> parquet.validateRecord(badParquet)),
            failure(() -> parquet.validateRecord(null))));
  }

  private static String failure(Executable action) {
    Exception e = assertThrows(Exception.class, action);
    return e.getClass().getSimpleName() + ": " + e.getMessage();
  }

  private Written written(BlobFileWriter<RecordFleakData> writer, List<RecordFleakData> records)
      throws Exception {
    return new Written(writer.getFileExtension(), latin1(writeOneFile(writer, records, dir)));
  }

  private static FleakSerializer<?> legacySerializer(
      EncodingType type, CsvSerializerOptions csvOptions, KvSerializerOptions kvOptions) {
    return SerializerFactory.createSerializerFactory(type, csvOptions, kvOptions)
        .createSerializer();
  }
}
