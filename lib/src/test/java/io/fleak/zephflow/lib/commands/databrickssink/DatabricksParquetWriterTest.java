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
package io.fleak.zephflow.lib.commands.databrickssink;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.types.*;
import io.fleak.zephflow.lib.commands.deltalakesink.InvalidRecordException;
import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DatabricksParquetWriterTest {

  private static final StructType TEST_SCHEMA =
      new StructType()
          .add(new StructField("id", IntegerType.INTEGER, false))
          .add(new StructField("name", StringType.STRING, false));

  private Path tempDir;
  private DatabricksParquetWriter writer;

  @BeforeEach
  void setUp() throws Exception {
    tempDir = Files.createTempDirectory("parquet-writer-test");
    writer = new DatabricksParquetWriter(TEST_SCHEMA);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (tempDir != null && Files.exists(tempDir)) {
      try (var paths = Files.walk(tempDir)) {
        for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
          Files.deleteIfExists(path);
        }
      }
    }
  }

  @Test
  void populatedMapWritesRealParquet() throws Exception {
    StructType schema =
        new StructType()
            .add("attributes", new MapType(StringType.STRING, StringType.STRING, true), true);
    List<File> files =
        new DatabricksParquetWriter(schema)
            .writeParquetFiles(
                List.of(Map.of("attributes", Map.of("region", "eu", "environment", "test"))),
                tempDir);
    assertEquals(1, files.size());
  }

  @Test
  void emptyMapWritesRealParquet() throws Exception {
    StructType schema =
        new StructType()
            .add("attributes", new MapType(StringType.STRING, StringType.STRING, true), true);
    List<File> files =
        new DatabricksParquetWriter(schema)
            .writeParquetFiles(List.of(Map.of("attributes", Map.of())), tempDir);
    assertEquals(1, files.size());
  }

  @Test
  void missingNullableMapWritesRealParquet() throws Exception {
    StructType schema =
        new StructType()
            .add("id", IntegerType.INTEGER, false)
            .add("attributes", new MapType(StringType.STRING, StringType.STRING, true), true);
    List<File> files =
        new DatabricksParquetWriter(schema).writeParquetFiles(List.of(Map.of("id", 1)), tempDir);
    assertEquals(1, files.size());
  }

  @Test
  void decimalOutsidePhysicalWidthFailsAtRealWriter() {
    StructType schema = new StructType().add("amount", new DecimalType(19, 0), false);
    var failure =
        assertThrows(
            InvalidRecordException.class,
            () ->
                new DatabricksParquetWriter(schema)
                    .writeParquetFiles(
                        List.of(
                            Map.of("amount", new BigDecimal("1000000000000000000000000000000"))),
                        tempDir));
    assertTrue(failure.getMessage().contains("fixed width"));
  }

  @Test
  void testWriteParquetFiles_generatesValidFiles() throws Exception {
    List<Map<String, Object>> data =
        List.of(Map.of("id", 1, "name", "Alice"), Map.of("id", 2, "name", "Bob"));

    List<File> files = writer.writeParquetFiles(data, tempDir);

    assertEquals(1, files.size());
    File parquetFile = files.get(0);
    assertTrue(parquetFile.exists(), "Parquet file should exist");
    assertTrue(parquetFile.length() > 0, "Parquet file should have content");
    assertTrue(parquetFile.getName().endsWith(".parquet"), "File should have .parquet extension");
  }

  @Test
  void testWriteParquetFiles_returnsFilesystemPathsNotUriPaths() throws Exception {
    List<Map<String, Object>> data = List.of(Map.of("id", 1, "name", "Test"));

    List<File> files = writer.writeParquetFiles(data, tempDir);

    assertEquals(1, files.size());
    File file = files.get(0);

    // The bug was that Delta Kernel returns file:// URI paths, and File was created with that URI
    // which caused file.exists() to return false and file operations to fail
    assertFalse(
        file.getPath().startsWith("file:"), "File path should not start with 'file:' URI scheme");
    assertTrue(file.exists(), "File should exist at the returned path");
    assertTrue(file.canRead(), "File should be readable");
  }

  @Test
  void testWriteParquetFiles_filePathIsAbsolute() throws Exception {
    List<Map<String, Object>> data = List.of(Map.of("id", 1, "name", "Test"));

    List<File> files = writer.writeParquetFiles(data, tempDir);

    assertEquals(1, files.size());
    File file = files.get(0);
    assertTrue(file.isAbsolute(), "Returned file path should be absolute");
    assertTrue(
        file.getAbsolutePath().contains(tempDir.toAbsolutePath().toString()),
        "File should be in temp directory");
  }

  @Test
  void testWriteParquetFiles_emptyDataReturnsEmptyList() throws Exception {
    List<Map<String, Object>> data = List.of();

    List<File> files = writer.writeParquetFiles(data, tempDir);

    assertTrue(files.isEmpty(), "Empty data should return empty file list");
  }

  @Test
  void testWriteParquetFiles_multipleRecords() throws Exception {
    List<Map<String, Object>> data =
        List.of(
            Map.of("id", 1, "name", "Alice"),
            Map.of("id", 2, "name", "Bob"),
            Map.of("id", 3, "name", "Carol"),
            Map.of("id", 4, "name", "David"),
            Map.of("id", 5, "name", "Eve"));

    List<File> files = writer.writeParquetFiles(data, tempDir);

    assertFalse(files.isEmpty(), "Should generate at least one file");
    for (File file : files) {
      assertTrue(file.exists(), "All returned files should exist");
      assertFalse(file.getPath().startsWith("file:"), "No file path should have URI scheme");
    }
  }

  @Test
  void testWriteParquetFiles_filesCanBeReadAsInputStream() throws Exception {
    List<Map<String, Object>> data = List.of(Map.of("id", 1, "name", "Test"));

    List<File> files = writer.writeParquetFiles(data, tempDir);

    assertEquals(1, files.size());
    File file = files.get(0);

    // This is the actual operation that failed before the fix
    // DatabricksVolumeUploader uses Files.newInputStream() to read the file for upload
    try (var inputStream = Files.newInputStream(file.toPath())) {
      byte[] bytes = inputStream.readAllBytes();
      assertTrue(bytes.length > 0, "Should be able to read file contents");
    }
  }

  @Test
  void mapMatrixRoundTripsTypedNestedValuesAndNulls() throws Exception {
    MapType strings = new MapType(StringType.STRING, StringType.STRING, true);
    MapType longs = new MapType(StringType.STRING, LongType.LONG, true);
    StructType inner =
        new StructType().add("score", LongType.LONG, true).add("attributes", strings, true);
    StructType schema =
        new StructType()
            .add("id", IntegerType.INTEGER, false)
            .add("attributes", strings, true)
            .add("numbers", longs, true)
            .add("nested", new MapType(StringType.STRING, longs, true), true)
            .add(
                "arrays",
                new MapType(StringType.STRING, new ArrayType(LongType.LONG, true), true),
                true)
            .add("objects", new MapType(StringType.STRING, inner, true), true)
            .add("arrayMaps", new ArrayType(longs, true), true)
            .add("structMap", inner, true);
    Map<String, Object> attributes = new LinkedHashMap<>();
    attributes.put("region", "eu");
    attributes.put("empty", "");
    attributes.put("missing", null);
    Map<String, Object> populated = new LinkedHashMap<>();
    populated.put("id", 1);
    populated.put("attributes", attributes);
    populated.put("numbers", Map.of("z", "42", "a", 7));
    populated.put("nested", Map.of("outer", Map.of("n", "13")));
    populated.put("arrays", Map.of("items", Arrays.asList("3", null, 5L)));
    populated.put("objects", Map.of("object", Map.of("score", 9L, "attributes", Map.of("a", "b"))));
    populated.put("arrayMaps", Arrays.asList(Map.of("a", "21"), null, Map.of()));
    populated.put("structMap", Map.of("score", 10L, "attributes", Map.of("nested", "map")));
    Map<String, Object> empty = new LinkedHashMap<>();
    for (String field : List.of("attributes", "numbers", "nested", "arrays", "objects"))
      empty.put(field, Map.of());
    empty.put("id", 2);
    empty.put("arrayMaps", List.of());
    Map<String, Object> absent = new LinkedHashMap<>();
    absent.put("id", 3);
    absent.put("attributes", null);
    List<Map<String, Object>> records = List.of(populated, empty, absent, populated);
    List<File> files = new DatabricksParquetWriter(schema).writeParquetFiles(records, tempDir);
    List<Group> groups = readGroups(files);
    assertEquals(4, groups.size(), "Equal records are separate occurrences");
    Group first = groups.getFirst();
    for (StructField field : schema.fields()) {
      assertPhysicalType(
          first.getType().getType(field.getName()), field.getDataType(), field.isNullable());
    }
    List<Map<String, Object>> decoded =
        groups.stream().map(group -> decodeRecord(group, schema)).toList();
    Map<String, Object> expectedFirst = new LinkedHashMap<>(populated);
    expectedFirst.put("numbers", Map.of("z", 42L, "a", 7L));
    expectedFirst.put("nested", Map.of("outer", Map.of("n", 13L)));
    expectedFirst.put("arrays", Map.of("items", Arrays.asList(3L, null, 5L)));
    expectedFirst.put("arrayMaps", Arrays.asList(Map.of("a", 21L), null, Map.of()));
    assertEquals(expectedFirst, decoded.get(0));
    assertEquals(expectedFirst, decoded.get(3));
    for (String field : List.of("attributes", "numbers", "nested", "arrays", "objects"))
      assertEquals(Map.of(), decoded.get(1).get(field));
    assertEquals(List.of(), decoded.get(1).get("arrayMaps"));
    assertNull(decoded.get(1).get("structMap"));
    for (String field : schema.fieldNames())
      if (!field.equals("id")) assertNull(decoded.get(2).get(field));
    assertEquals("42", ((Map<?, ?>) populated.get("numbers")).get("z"));
    assertEquals("3", ((List<?>) ((Map<?, ?>) populated.get("arrays")).get("items")).getFirst());
  }

  @Test
  void mapScalarValuesRoundTripWithExistingPhysicalRepresentations() throws Exception {
    StructType schema = new StructType();
    Map<String, Object> input = new LinkedHashMap<>();
    Map<String, Object> expected = new LinkedHashMap<>();
    DataType[] types = {
      ByteType.BYTE,
      ShortType.SHORT,
      IntegerType.INTEGER,
      LongType.LONG,
      FloatType.FLOAT,
      DoubleType.DOUBLE,
      BooleanType.BOOLEAN,
      StringType.STRING,
      BinaryType.BINARY,
      DateType.DATE,
      TimestampType.TIMESTAMP,
      TimestampNTZType.TIMESTAMP_NTZ,
      new DecimalType(20, 2)
    };
    Object[] values = {
      "7",
      "123",
      "42",
      "9000000000",
      "1.25",
      "2.5",
      "true",
      123,
      new byte[] {1, 2, 3},
      "20000",
      1234L,
      1234L,
      new BigDecimal("123.45")
    };
    Object[] converted = {
      (byte) 7,
      (short) 123,
      42,
      9000000000L,
      1.25f,
      2.5d,
      true,
      "123",
      new byte[] {1, 2, 3},
      20000,
      1234000L,
      1234000L,
      new BigDecimal("123.45")
    };
    for (int i = 0; i < types.length; i++) {
      String name = "field" + i;
      schema = schema.add(name, new MapType(StringType.STRING, types[i], true), false);
      input.put(name, Map.of("key", values[i]));
      expected.put(name, Map.of("key", converted[i]));
    }
    List<Group> groups =
        readGroups(new DatabricksParquetWriter(schema).writeParquetFiles(List.of(input), tempDir));
    Map<String, Object> decoded = decodeRecord(groups.getFirst(), schema);
    for (String field : expected.keySet()) {
      Object actualValue = ((Map<?, ?>) decoded.get(field)).get("key");
      Object expectedValue = ((Map<?, ?>) expected.get(field)).get("key");
      if (expectedValue instanceof byte[] bytes) assertArrayEquals(bytes, (byte[]) actualValue);
      else assertEquals(expectedValue, actualValue, field);
    }
  }

  @Test
  void decimalFixedWidthPreservesSignedBoundariesAndExistingNarrowing() throws Exception {
    DecimalType type = new DecimalType(19, 0); // Signed nine-byte physical encoding.
    BigInteger minimum = BigInteger.ONE.shiftLeft(71).negate();
    BigInteger maximum = BigInteger.ONE.shiftLeft(71).subtract(BigInteger.ONE);
    StructType schema = new StructType().add("amount", type, false);
    List<Map<String, Object>> records =
        List.of(
            Map.of("amount", new BigDecimal(minimum)),
            Map.of("amount", new BigDecimal(maximum)),
            Map.of("amount", new BigDecimal("1")),
            Map.of("amount", new BigDecimal("-1")));
    List<Group> groups =
        readGroups(new DatabricksParquetWriter(schema).writeParquetFiles(records, tempDir));
    for (int i = 0; i < records.size(); i++)
      assertEquals(records.get(i), decodeRecord(groups.get(i), schema));
    for (BigInteger value :
        List.of(minimum.subtract(BigInteger.ONE), maximum.add(BigInteger.ONE))) {
      assertThrows(
          InvalidRecordException.class,
          () ->
              new DatabricksParquetWriter(schema)
                  .writeParquetFiles(List.of(Map.of("amount", new BigDecimal(value))), tempDir));
    }
    StructType narrow = new StructType().add("amount", new DecimalType(9, 2), false);
    List<Group> narrowRows =
        readGroups(
            new DatabricksParquetWriter(narrow)
                .writeParquetFiles(List.of(Map.of("amount", new BigDecimal("1.239"))), tempDir));
    assertEquals(new BigDecimal("1.23"), decodeRecord(narrowRows.getFirst(), narrow).get("amount"));
  }

  @Test
  void nullableAncestorsAndNullableElementsStillRoundTrip() throws Exception {
    StructType requiredChild = new StructType().add("count", LongType.LONG, false);
    StructType schema =
        new StructType()
            .add("parent", requiredChild, true)
            .add("array", new ArrayType(requiredChild, true), true)
            .add("map", new MapType(StringType.STRING, requiredChild, true), true)
            .add("numbers", new ArrayType(LongType.LONG, true), true);
    Map<String, Object> allNull = new LinkedHashMap<>();
    for (StructField field : schema.fields()) allNull.put(field.getName(), null);
    Map<String, Object> populated = new LinkedHashMap<>();
    populated.put("parent", Map.of("count", 4L));
    populated.put("array", Arrays.asList(null, Map.of("count", 5L)));
    populated.put("map", Collections.singletonMap("nullable", null));
    populated.put("numbers", Arrays.asList(null, 6L));
    List<Map<String, Object>> input = List.of(allNull, populated, allNull);
    List<Group> rows =
        readGroups(new DatabricksParquetWriter(schema).writeParquetFiles(input, tempDir));
    assertEquals(input.size(), rows.size());
    for (int i = 0; i < input.size(); i++)
      assertEquals(input.get(i), decodeRecord(rows.get(i), schema));
  }

  @Test
  void preservesExistingNullableNestedStructShapeConversion() throws Exception {
    StructType inner = new StructType().add("count", LongType.LONG, true);
    StructType schema =
        new StructType().add("value", new StructType().add("inner", inner, false), false);
    List<Map<String, Object>> input =
        List.of(
            Map.of("value", Map.of("inner", Map.of("count", 11L))),
            Map.of("value", Map.of("inner", "bad")),
            Map.of("value", Map.of("inner", Map.of("count", 33L))));
    List<Group> rows =
        readGroups(new DatabricksParquetWriter(schema).writeParquetFiles(input, tempDir));
    assertEquals(3, rows.size());
    assertEquals(input.get(0), decodeRecord(rows.get(0), schema));
    assertEquals(
        Map.of("value", Map.of("inner", Collections.singletonMap("count", null))),
        decodeRecord(rows.get(1), schema));
    assertEquals(input.get(2), decodeRecord(rows.get(2), schema));
  }

  private static void assertPhysicalType(Type physical, DataType expected, boolean nullable) {
    assertEquals(
        nullable ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED,
        physical.getRepetition(),
        physical.getName());
    switch (expected) {
      case MapType map -> {
        assertEquals(LogicalTypeAnnotation.mapType(), physical.getLogicalTypeAnnotation());
        assertEquals(1, physical.asGroupType().getFieldCount());
        var entries = physical.asGroupType().getType("key_value").asGroupType();
        assertEquals(Type.Repetition.REPEATED, entries.getRepetition());
        assertEquals(2, entries.getFieldCount());
        assertPhysicalType(entries.getType("key"), map.getKeyType(), false);
        assertPhysicalType(entries.getType("value"), map.getValueType(), map.isValueContainsNull());
      }
      case ArrayType array -> {
        assertEquals(LogicalTypeAnnotation.listType(), physical.getLogicalTypeAnnotation());
        assertEquals(1, physical.asGroupType().getFieldCount());
        var elements = physical.asGroupType().getType("list").asGroupType();
        assertEquals(Type.Repetition.REPEATED, elements.getRepetition());
        assertEquals(1, elements.getFieldCount());
        assertPhysicalType(
            elements.getType("element"), array.getElementType(), array.containsNull());
      }
      case StructType struct -> {
        assertEquals(struct.length(), physical.asGroupType().getFieldCount());
        for (StructField field : struct.fields())
          assertPhysicalType(
              physical.asGroupType().getType(field.getName()),
              field.getDataType(),
              field.isNullable());
      }
      case StringType ignored -> {
        assertEquals(PrimitiveTypeName.BINARY, physical.asPrimitiveType().getPrimitiveTypeName());
        assertEquals(LogicalTypeAnnotation.stringType(), physical.getLogicalTypeAnnotation());
      }
      case LongType ignored ->
          assertEquals(PrimitiveTypeName.INT64, physical.asPrimitiveType().getPrimitiveTypeName());
      case IntegerType ignored ->
          assertEquals(PrimitiveTypeName.INT32, physical.asPrimitiveType().getPrimitiveTypeName());
      default -> throw new AssertionError("Unhandled complex-type test schema: " + expected);
    }
  }

  private static List<Group> readGroups(List<File> files) throws Exception {
    List<Group> rows = new ArrayList<>();
    for (File file : files) {
      try (ParquetReader<Group> reader =
          ParquetReader.builder(new GroupReadSupport(), new org.apache.hadoop.fs.Path(file.toURI()))
              .build()) {
        Group row;
        while ((row = reader.read()) != null) rows.add(row);
      }
    }
    return rows;
  }

  private static Map<String, Object> decodeRecord(Group row, StructType schema) {
    Map<String, Object> record = new LinkedHashMap<>();
    for (StructField field : schema.fields())
      record.put(field.getName(), decodeValue(row, field.getName(), field.getDataType()));
    return record;
  }

  private static Object decodeValue(Group parent, String field, DataType type) {
    if (parent.getFieldRepetitionCount(field) == 0) return null;
    return switch (type) {
      case MapType map -> {
        Group value = parent.getGroup(field, 0);
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < value.getFieldRepetitionCount("key_value"); i++) {
          Group entry = value.getGroup("key_value", i);
          result.put(entry.getString("key", 0), decodeValue(entry, "value", map.getValueType()));
        }
        yield result;
      }
      case ArrayType array -> {
        Group value = parent.getGroup(field, 0);
        List<Object> result = new ArrayList<>();
        for (int i = 0; i < value.getFieldRepetitionCount("list"); i++)
          result.add(decodeValue(value.getGroup("list", i), "element", array.getElementType()));
        yield result;
      }
      case StructType struct -> decodeRecord(parent.getGroup(field, 0), struct);
      case ByteType ignored -> (byte) parent.getInteger(field, 0);
      case ShortType ignored -> (short) parent.getInteger(field, 0);
      case IntegerType ignored -> parent.getInteger(field, 0);
      case DateType ignored -> parent.getInteger(field, 0);
      case LongType ignored -> parent.getLong(field, 0);
      case TimestampType ignored -> parent.getLong(field, 0);
      case TimestampNTZType ignored -> parent.getLong(field, 0);
      case FloatType ignored -> parent.getFloat(field, 0);
      case DoubleType ignored -> parent.getDouble(field, 0);
      case BooleanType ignored -> parent.getBoolean(field, 0);
      case StringType ignored -> parent.getString(field, 0);
      case BinaryType ignored -> parent.getBinary(field, 0).getBytes();
      case DecimalType decimal -> {
        BigInteger unscaled;
        if (decimal.getPrecision() <= 9) unscaled = BigInteger.valueOf(parent.getInteger(field, 0));
        else if (decimal.getPrecision() <= 18)
          unscaled = BigInteger.valueOf(parent.getLong(field, 0));
        else unscaled = new BigInteger(parent.getBinary(field, 0).getBytes());
        yield new BigDecimal(unscaled, decimal.getScale());
      }
      default -> throw new AssertionError("Unhandled test type: " + type);
    };
  }
}
