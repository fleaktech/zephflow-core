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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.BlobFileWriter;
import io.fleak.zephflow.lib.serdes.decode.DecodeResult;
import io.fleak.zephflow.lib.serdes.decode.config.DecodingConfig;
import io.fleak.zephflow.lib.serdes.decode.config.ParquetFormatConfig;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.nio.charset.StandardCharsets;
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

/** The exact bytes each sink format writes, as the earlier per-format serializers wrote them. */
class EncodingOutputTest {

  private static final List<RecordFleakData> RECORDS =
      List.of(
          json(
              "{\"id\":1,\"name\":\"Alice\",\"score\":9.5,\"ok\":true,\"tags\":[\"a\",\"b\"],"
                  + "\"meta\":{\"k\":\"v\",\"n\":2},\"event.id\":4624,\"src ip\":\"10.0.0.1\","
                  + "\"note\":\"has, comma \\\"q\\\"\\nand a line break\",\"empty\":\"\"}"),
          json("{\"id\":2,\"name\":\"Bob\",\"city\":\"New York\",\"none\":null,\"Name\":\"dup\"}"),
          json("{\"id\":3,\"name\":\" spaced \",\"note\":\"a=b\\\\c\"}"),
          json("{\"id\":4,\"name\":\"café 中文\",\"city\":\"é\"}"));

  private static final List<RecordFleakData> ONE_NULL_COLUMN =
      List.of(json("{\"a\":null}"), json("{\"a\":\"x\"}"));

  private static final String KV_WITH_OPTIONS =
      "{\"type\":\"kv\",\"fields\":[\"name\",\"event.id\",\"src ip\",\"missing\",\" id\"],"
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

  private static final String CSV_FILE =
      "score,note,src ip,meta,name,id,ok,event.id,tags,empty,city,none,Name\r\n"
          + "9.5,\"has, comma \"\"q\"\"\nand a line break\",10.0.0.1,\"{k=v, n=2}\",Alice,1,true,"
          + "4624,\"[a, b]\",,,,\r\n"
          + ",,,,Bob,2,,,,,New York,,dup\r\n"
          + ",a=b\\c,,,\" spaced \",3,,,,,,,\r\n"
          + ",,,,café 中文,4,,,,,é,,\r\n";

  private static final List<String> JSON_OBJECTS =
      List.of(
          "{\"score\":9.5,\"note\":\"has, comma \\\"q\\\"\\nand a line break\",\"src ip\":\"10.0.0.1\","
              + "\"meta\":{\"n\":2,\"k\":\"v\"},\"name\":\"Alice\",\"id\":1,\"ok\":true,"
              + "\"event.id\":4624,\"tags\":[\"a\",\"b\"],\"empty\":\"\"}",
          "{\"name\":\"Bob\",\"id\":2,\"none\":null,\"city\":\"New York\",\"Name\":\"dup\"}",
          "{\"name\":\" spaced \",\"note\":\"a=b\\\\c\",\"id\":3}",
          "{\"name\":\"café 中文\",\"city\":\"é\",\"id\":4}");

  private static final List<String> KV_LINES =
      List.of(
          "empty=\"\" event.id=4624 id=1 meta=\"{k=v, n=2}\" name=Alice"
              + " note=\"has, comma \\\"q\\\"\\nand a line break\" ok=true score=9.5"
              + " src_ip=10.0.0.1 tags=\"[a, b]\"",
          "city=\"New York\" id=2 Name=dup name=Bob",
          "id=3 name=\" spaced \" note=\"a=b\\\\c\"",
          "city=\"é\" id=4 name=\"café 中文\"");

  private static final List<String> KV_LINES_WITH_OPTIONS =
      List.of(
          "name=Alice | event_id=4624 | src_ip=10.0.0.1",
          "name=Bob",
          "name=\" spaced \"",
          "name=\"café 中文\"");

  @TempDir Path dir;

  record Written(String extension, String text) {}

  static Stream<Arguments> fileFormats() {
    return Stream.of(
        Arguments.of("{\"type\":\"csv\"}", RECORDS, new Written("csv", CSV_FILE)),
        Arguments.of(
            "{\"type\":\"csv\"}", ONE_NULL_COLUMN, new Written("csv", "a\r\n\"\"\r\nx\r\n")),
        Arguments.of("{\"type\":\"csv\"}", List.of(), new Written("csv", "")),
        Arguments.of(
            "{\"type\":\"csv\",\"delimiter\":\",\",\"quote\":\"\\\"\",\"escape\":\"\",\"skipLines\":0,"
                + "\"ignoreSurroundingSpaces\":true,\"charset\":\"UTF-8\",\"header\":true}",
            RECORDS,
            new Written("csv", CSV_FILE)),
        Arguments.of(
            "{\"type\":\"json\",\"mode\":\"array\"}",
            RECORDS,
            new Written("json", "[" + String.join(",", JSON_OBJECTS) + "]")),
        Arguments.of(
            "{\"type\":\"json\",\"mode\":\"array\"}", List.of(), new Written("json", "[]")),
        Arguments.of(
            "{\"type\":\"json\",\"mode\":\"lines\"}",
            RECORDS,
            new Written("jsonl", String.join("\n", JSON_OBJECTS))),
        Arguments.of("{\"type\":\"kv\"}", RECORDS, new Written("raw", String.join("\n", KV_LINES))),
        Arguments.of(
            KV_WITH_OPTIONS,
            RECORDS,
            new Written("raw", String.join("\n", KV_LINES_WITH_OPTIONS))));
  }

  @ParameterizedTest
  @MethodSource("fileFormats")
  void fileModeWritesTheFileWithAndWithoutGzip(
      String format, List<RecordFleakData> records, Written expected) throws Exception {
    BlobFileWriter<RecordFleakData> plain = EncodingCompiler.fileWriter(encoding("[]", format), S3);
    BlobFileWriter<RecordFleakData> gzipped =
        EncodingCompiler.fileWriter(encoding("[\"gzip\"]", format), S3);
    List<Written> actual =
        List.of(
            new Written(plain.getFileExtension(), utf8(writeOneFile(plain, records, dir))),
            new Written(
                gzipped.getFileExtension(), utf8(gunzip(writeOneFile(gzipped, records, dir)))));
    assertEquals(
        List.of(expected, new Written(expected.extension() + ".gz", expected.text())), actual);
  }

  static Stream<Arguments> recordFormats() {
    return Stream.of(
        Arguments.of(
            "{\"type\":\"csv\"}",
            KAFKA,
            List.of(
                "score,note,src ip,meta,name,id,ok,event.id,tags,empty\r\n"
                    + "9.5,\"has, comma \"\"q\"\"\nand a line break\",10.0.0.1,\"{k=v, n=2}\","
                    + "Alice,1,true,4624,\"[a, b]\",\r\n",
                "city,name,id,none,Name\r\nNew York,Bob,2,,dup\r\n",
                "note,name,id\r\na=b\\c,\" spaced \",3\r\n",
                "city,name,id\r\né,café 中文,4\r\n")),
        Arguments.of(
            "{\"type\":\"csv\",\"header\":false,\"columns\":[\"id\",\" name\\u200b\",\"city\"]}",
            SPLUNK,
            List.of(
                "1,Alice,\r\n", "2,Bob,New York\r\n", "3,\" spaced \",\r\n", "4,café 中文,é\r\n")),
        Arguments.of("{\"type\":\"json\",\"mode\":\"object\"}", KAFKA, JSON_OBJECTS),
        Arguments.of(
            "{\"type\":\"json\",\"mode\":\"array\"}",
            KAFKA,
            JSON_OBJECTS.stream().map(o -> "[" + o + "]").toList()),
        Arguments.of("{\"type\":\"json\",\"mode\":\"lines\"}", KAFKA, JSON_OBJECTS),
        Arguments.of("{\"type\":\"kv\"}", KAFKA, KV_LINES),
        Arguments.of(KV_WITH_OPTIONS, SPLUNK, KV_LINES_WITH_OPTIONS));
  }

  @ParameterizedTest
  @MethodSource("recordFormats")
  void recordModeWritesTheBytesOfOneRecord(
      String format, WriterCapability capability, List<String> expected) throws Exception {
    RecordEncoder encoder = EncodingCompiler.recordEncoder(encoding("[]", format), capability);
    List<String> actual = new ArrayList<>();
    for (RecordFleakData record : RECORDS) {
      actual.add(utf8(encoder.encode(record)));
    }
    assertEquals(expected, actual);
  }

  @Test
  void parquetFilesReadBackAsTheRecords() throws Exception {
    String schema = JsonUtils.toJsonString(AVRO_SCHEMA);
    List<RecordFleakData> records =
        List.of(
            json("{\"id\":1,\"name\":\"a\",\"score\":1.5,\"tags\":[\"x\",\"y\"]}"),
            json("{\"id\":2,\"name\":\"b\",\"score\":null}"));
    DecodingConfig parquet = new DecodingConfig(null, new ParquetFormatConfig());
    DecodeResult expected =
        new DecodeResult(
            List.of(
                json("{\"id\":1,\"name\":\"a\",\"score\":1.5,\"tags\":[\"x\",\"y\"]}"),
                json("{\"id\":2,\"name\":\"b\"}")),
            List.of());
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
  void perRecordFailuresThrow() {
    RecordFleakData noKvField = json("{\"other\":1}");
    RecordFleakData badParquet = json("{\"id\":\"not a number\",\"name\":\"a\"}");
    EncodingConfig kv = encoding("{\"format\":" + KV_WITH_OPTIONS + "}");
    EncodingConfig csv = encoding("{\"format\":{\"type\":\"csv\"}}");
    BlobFileWriter<RecordFleakData> parquet =
        EncodingCompiler.fileWriter(
            encoding(
                "{\"format\":{\"type\":\"parquet\",\"schema\":"
                    + JsonUtils.toJsonString(AVRO_SCHEMA)
                    + "}}"),
            S3);
    assertEquals(
        List.of(
            "IllegalArgumentException: event has none of the configured KV fields",
            "IllegalArgumentException: event has none of the configured KV fields",
            "IllegalArgumentException: Record cannot be null",
            "IllegalArgumentException: Record cannot be null",
            "InvalidRecordException: Cannot convert value 'not a number' to integer for field 'id'",
            "IllegalArgumentException: Record cannot be null"),
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

  private static String utf8(byte[] bytes) {
    return new String(bytes, StandardCharsets.UTF_8);
  }
}
