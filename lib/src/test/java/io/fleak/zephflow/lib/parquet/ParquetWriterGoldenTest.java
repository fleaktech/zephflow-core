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
package io.fleak.zephflow.lib.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import io.delta.kernel.types.StructType;
import io.fleak.zephflow.lib.commands.databrickssink.AvroToDeltaSchemaConverter;
import io.fleak.zephflow.lib.commands.databrickssink.DatabricksParquetWriter;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The Hadoop-free writer must produce what delta-kernel's DefaultEngine wrote. */
class ParquetWriterGoldenTest {

  private static final String AVRO_SCHEMA =
      """
      {"type": "record", "name": "Event", "fields": [
        {"name": "id", "type": "long"},
        {"name": "name", "type": "string"},
        {"name": "kind", "type": {"type": "enum", "name": "Kind", "symbols": ["A", "B"]}},
        {"name": "count", "type": ["null", "int"]},
        {"name": "day", "type": ["null", {"type": "int", "logicalType": "date"}]},
        {"name": "tsMillis", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]},
        {"name": "tsMicros", "type": ["null", {"type": "long", "logicalType": "timestamp-micros"}]},
        {"name": "ratio", "type": ["null", "float"]},
        {"name": "score", "type": ["null", "double"]},
        {"name": "flag", "type": ["null", "boolean"]},
        {"name": "payload", "type": ["null", "bytes"]},
        {"name": "dec9", "type": ["null",
          {"type": "bytes", "logicalType": "decimal", "precision": 9, "scale": 2}]},
        {"name": "dec18", "type": ["null",
          {"type": "bytes", "logicalType": "decimal", "precision": 18, "scale": 3}]},
        {"name": "dec38", "type": ["null",
          {"type": "fixed", "name": "Dec38", "size": 16, "logicalType": "decimal",
           "precision": 38, "scale": 4}]},
        {"name": "tags", "type": ["null", {"type": "array", "items": ["null", "string"]}]},
        {"name": "counters", "type": ["null", {"type": "map", "values": ["null", "long"]}]},
        {"name": "device", "type": ["null", {"type": "record", "name": "Device", "fields": [
          {"name": "host", "type": "string"},
          {"name": "ports", "type": ["null", {"type": "array", "items": "int"}]},
          {"name": "labels", "type": ["null", {"type": "map", "values": "string"}]}
        ]}]}
      ]}
      """;

  private static final String DELTA_KERNEL_FILE = "/parquet/delta-kernel-writer.parquet";
  private static final String DELTA_KERNEL_NESTED_FILE =
      "/parquet/delta-kernel-writer-nested.parquet";

  @TempDir Path tempDir;

  /**
   * The fixture was written by delta-kernel's DefaultEngine from {@link #rows()} on f4839f6, before
   * it was removed; changing the rows means regenerating it there with DatabricksParquetWriter.
   */
  @Test
  void matchesDeltaKernelOutput() throws Exception {
    Path expected = tempDir.resolve("delta-kernel.parquet");
    try (InputStream in = getClass().getResourceAsStream(DELTA_KERNEL_FILE)) {
      Files.copy(in, expected);
    }
    Path out = Files.createDirectory(tempDir.resolve("out"));
    List<File> files = new DatabricksParquetWriter(schema()).writeParquetFiles(rows(), out);
    assertEquals(dump(List.of(expected.toFile())), dump(files));
  }

  /**
   * Same check for nested layouts; its fixture was written the same way from {@link #nestedRows()}.
   */
  @Test
  void matchesDeltaKernelOutputForNestedTypes() throws Exception {
    Path expected = tempDir.resolve("delta-kernel-nested.parquet");
    try (InputStream in = getClass().getResourceAsStream(DELTA_KERNEL_NESTED_FILE)) {
      Files.copy(in, expected);
    }
    Map<String, Object> avro =
        JsonUtils.OBJECT_MAPPER.readValue(NESTED_AVRO_SCHEMA, new TypeReference<>() {});
    Path out = Files.createDirectory(tempDir.resolve("nested-out"));
    List<File> files =
        new DatabricksParquetWriter(AvroToDeltaSchemaConverter.parse(avro))
            .writeParquetFiles(nestedRows(), out);
    assertEquals(dump(List.of(expected.toFile())), dump(files));
  }

  static StructType schema() throws IOException {
    Map<String, Object> avro =
        JsonUtils.OBJECT_MAPPER.readValue(AVRO_SCHEMA, new TypeReference<>() {});
    return AvroToDeltaSchemaConverter.parse(avro);
  }

  /** Every type, with nulls, empties and boundary values; more rows than one 1000-row chunk. */
  static List<Map<String, Object>> rows() {
    List<Map<String, Object>> rows = new ArrayList<>();
    Map<String, Object> full = new LinkedHashMap<>();
    full.put("id", 1L);
    full.put("name", "héllo wörld");
    full.put("kind", "A");
    full.put("count", 42);
    full.put("day", 19_000);
    full.put("tsMillis", 1_700_000_000_123L);
    full.put("tsMicros", 1_700_000_000_456L);
    full.put("ratio", 1.5f);
    full.put("score", -0.25);
    full.put("flag", true);
    full.put("payload", "raw".getBytes(StandardCharsets.UTF_8));
    full.put("dec9", new BigDecimal("-9999999.99"));
    full.put("dec18", new BigDecimal("123456789012345.678"));
    full.put("dec38", new BigDecimal("-1234567890123456789012345678901234.5678"));
    full.put("tags", Arrays.asList("a", null, ""));
    Map<String, Object> counters = new LinkedHashMap<>();
    counters.put("x", 1L);
    counters.put("y", null);
    full.put("counters", counters);
    full.put(
        "device",
        Map.of("host", "web-1", "ports", List.of(80, 443), "labels", Map.of("env", "prod")));
    rows.add(full);

    Map<String, Object> empty = new LinkedHashMap<>();
    empty.put("id", 2L);
    empty.put("name", "");
    empty.put("kind", "B");
    empty.put("tags", List.of());
    empty.put("counters", Map.of());
    empty.put("dec9", new BigDecimal("0.01"));
    empty.put("dec38", new BigDecimal("0"));
    empty.put("device", Map.of("host", "db"));
    rows.add(empty);

    Map<String, Object> sparse = new LinkedHashMap<>();
    sparse.put("id", 3L);
    sparse.put("name", "only required");
    sparse.put("kind", "A");
    rows.add(sparse);

    for (long i = 4; i <= 2_500; i++) {
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("id", i);
      row.put("name", "row-" + i);
      row.put("kind", i % 2 == 0 ? "A" : "B");
      row.put("count", (int) i);
      row.put("score", i / 7.0);
      row.put("dec18", BigDecimal.valueOf(i, 3));
      row.put("tags", List.of("t" + (i % 5)));
      rows.add(row);
    }
    return rows;
  }

  private static final String NESTED_AVRO_SCHEMA =
      """
      {"type": "record", "name": "Nested", "fields": [
        {"name": "id", "type": "long"},
        {"name": "events", "type": ["null", {"type": "array", "items": ["null",
          {"type": "record", "name": "Event", "fields": [
            {"name": "code", "type": "int"},
            {"name": "msg", "type": ["null", "string"]}
          ]}]}]},
        {"name": "byHost", "type": ["null", {"type": "map", "values":
          {"type": "record", "name": "Host", "fields": [
            {"name": "up", "type": "boolean"},
            {"name": "load", "type": ["null", "double"]}
          ]}}]},
        {"name": "matrix", "type": ["null",
          {"type": "array", "items": ["null", {"type": "array", "items": "int"}]}]},
        {"name": "dec20", "type": ["null",
          {"type": "bytes", "logicalType": "decimal", "precision": 20, "scale": 2}]},
        {"name": "dec30", "type": ["null",
          {"type": "bytes", "logicalType": "decimal", "precision": 30, "scale": 5}]},
        {"name": "hash", "type": ["null", {"type": "fixed", "name": "Hash", "size": 8}]}
      ]}
      """;

  /** Records inside arrays and maps, nested arrays, wide decimals and plain fixed bytes. */
  static List<Map<String, Object>> nestedRows() {
    List<Map<String, Object>> rows = new ArrayList<>();
    Map<String, Object> full = new LinkedHashMap<>();
    full.put("id", 1L);
    Map<String, Object> event = new LinkedHashMap<>();
    event.put("code", 7);
    event.put("msg", "boot");
    Map<String, Object> silent = new LinkedHashMap<>();
    silent.put("code", -1);
    full.put("events", Arrays.asList(event, null, silent));
    Map<String, Object> up = new LinkedHashMap<>();
    up.put("up", true);
    up.put("load", 0.75);
    Map<String, Object> down = new LinkedHashMap<>();
    down.put("up", false);
    Map<String, Object> hosts = new LinkedHashMap<>();
    hosts.put("web-1", up);
    hosts.put("web-2", down);
    full.put("byHost", hosts);
    full.put("matrix", Arrays.asList(List.of(1, 2), List.of(), null, List.of(3)));
    full.put("dec20", new BigDecimal("-123456789012345678.90"));
    full.put("dec30", new BigDecimal("1234567890123456789012345.12345"));
    full.put("hash", "abcdefgh".getBytes(StandardCharsets.UTF_8));
    rows.add(full);

    Map<String, Object> empty = new LinkedHashMap<>();
    empty.put("id", 2L);
    empty.put("events", List.of());
    empty.put("byHost", Map.of());
    empty.put("matrix", List.of());
    empty.put("dec20", new BigDecimal("0.01"));
    empty.put("dec30", new BigDecimal("0"));
    rows.add(empty);

    Map<String, Object> sparse = new LinkedHashMap<>();
    sparse.put("id", 3L);
    rows.add(sparse);

    for (long i = 4; i <= 50; i++) {
      Map<String, Object> row = new LinkedHashMap<>();
      row.put("id", i);
      Map<String, Object> e = new LinkedHashMap<>();
      e.put("code", (int) i);
      e.put("msg", "e" + i);
      row.put("events", List.of(e));
      row.put("matrix", List.of(List.of((int) i, (int) -i)));
      row.put("dec20", BigDecimal.valueOf(i * 1_000_000_007L, 2));
      rows.add(row);
    }
    return rows;
  }

  /**
   * Schema, column orders, column layout and every value; writer name and key-value metadata aside.
   */
  static String dump(List<File> files) throws IOException {
    StringBuilder out = new StringBuilder();
    out.append("files: ").append(files.size()).append('\n');
    for (File file : files) {
      ParquetMetadata footer = ParquetTestReader.footer(file);
      out.append(footer.getFileMetaData().getSchema());
      out.append("column orders: ").append(columnOrders(file)).append('\n');
      out.append("size statistics: ").append(sizeStatistics(file)).append('\n');
      for (BlockMetaData block : footer.getBlocks()) {
        out.append("row group: ").append(block.getRowCount()).append(" rows\n");
        for (ColumnChunkMetaData column : block.getColumns()) {
          out.append("  ")
              .append(column.getPath())
              .append(' ')
              .append(column.getCodec())
              .append(' ')
              .append(column.getEncodings().stream().map(Enum::name).sorted().toList())
              .append(" values=")
              .append(column.getValueCount())
              .append(" stats=")
              .append(column.getStatistics())
              .append('\n');
        }
      }
      List<String> rows =
          ParquetTestReader.readGroups(List.of(file)).stream().map(Object::toString).toList();
      out.append(String.join("---\n", rows.subList(0, Math.min(3, rows.size()))));
      out.append("all ").append(rows.size()).append(" rows sha256: ").append(sha256(rows));
      out.append('\n');
    }
    return out.toString();
  }

  private static List<String> columnOrders(File file) throws IOException {
    return rawFooter(file).getColumn_orders().stream().map(Object::toString).toList();
  }

  private static long sizeStatistics(File file) throws IOException {
    return rawFooter(file).getRow_groups().stream()
        .flatMap(group -> group.getColumns().stream())
        .filter(chunk -> chunk.getMeta_data().isSetSize_statistics())
        .count();
  }

  private static FileMetaData rawFooter(File file) throws IOException {
    byte[] bytes = Files.readAllBytes(file.toPath());
    int length =
        ByteBuffer.wrap(bytes, bytes.length - 8, 4).order(ByteOrder.LITTLE_ENDIAN).getInt();
    return Util.readFileMetaData(
        new ByteArrayInputStream(bytes, bytes.length - 8 - length, length));
  }

  private static String sha256(List<String> rows) {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      for (String row : rows) {
        digest.update(row.getBytes(StandardCharsets.UTF_8));
      }
      return HexFormat.of().formatHex(digest.digest());
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
