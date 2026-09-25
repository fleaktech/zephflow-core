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
package io.fleak.zephflow.lib.commands.deltalakesink;

import static org.junit.jupiter.api.Assertions.*;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.types.*;
import java.util.*;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DeltaLakeDataConverterTest {

  @Test
  void testIntegerParseFailure() {
    StructType schema = new StructType(List.of(new StructField("age", IntegerType.INTEGER, false)));

    List<Map<String, Object>> data = List.of(Map.of("age", "invalid"));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DeltaLakeDataConverter.convertToColumnarBatch(data, schema));

    assertEquals(
        "Cannot convert value 'invalid' to integer for field 'age'", exception.getMessage());
  }

  @Test
  void testLongParseFailure() {
    StructType schema = new StructType(List.of(new StructField("id", LongType.LONG, false)));

    List<Map<String, Object>> data = List.of(Map.of("id", "not_a_number"));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DeltaLakeDataConverter.convertToColumnarBatch(data, schema));

    assertEquals(
        "Cannot convert value 'not_a_number' to long for field 'id'", exception.getMessage());
  }

  @Test
  void testDoubleParseFailure() {
    StructType schema = new StructType(List.of(new StructField("price", DoubleType.DOUBLE, false)));

    List<Map<String, Object>> data = List.of(Map.of("price", "abc"));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DeltaLakeDataConverter.convertToColumnarBatch(data, schema));

    assertEquals("Cannot convert value 'abc' to double for field 'price'", exception.getMessage());
  }

  @Test
  void testTimestampParseFailure() {
    StructType schema =
        new StructType(List.of(new StructField("created_at", TimestampType.TIMESTAMP, false)));

    List<Map<String, Object>> data = List.of(Map.of("created_at", "invalid_timestamp"));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DeltaLakeDataConverter.convertToColumnarBatch(data, schema));

    assertEquals(
        "Cannot convert value 'invalid_timestamp' to timestamp for field 'created_at'",
        exception.getMessage());
  }

  @Test
  void testArrayTypeMismatch() {
    StructType schema =
        new StructType(
            List.of(new StructField("tags", new ArrayType(StringType.STRING, true), false)));

    List<Map<String, Object>> data = List.of(Map.of("tags", "not_an_array"));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DeltaLakeDataConverter.convertToColumnarBatch(data, schema));

    assertEquals("Expected array for field 'tags' but got String", exception.getMessage());
  }

  @Test
  void testStructTypeMismatch() {
    StructType innerStruct =
        new StructType(List.of(new StructField("city", StringType.STRING, true)));
    StructType schema = new StructType(List.of(new StructField("address", innerStruct, false)));

    List<Map<String, Object>> data = List.of(Map.of("address", "not_a_map"));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DeltaLakeDataConverter.convertToColumnarBatch(data, schema));

    assertEquals("Expected map/struct for field 'address' but got String", exception.getMessage());
  }

  @Test
  void testSimpleColumnVectorValidationFailure() {
    StructType schema =
        new StructType(List.of(new StructField("count", IntegerType.INTEGER, false)));

    List<Map<String, Object>> data = List.of(Map.of("count", "not_an_integer"));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DeltaLakeDataConverter.convertToColumnarBatch(data, schema));

    assertEquals(
        "Cannot convert value 'not_an_integer' to integer for field 'count'",
        exception.getMessage());
  }

  @Test
  void testSuccessfulConversion() {
    StructType schema =
        new StructType(
            List.of(
                new StructField("name", StringType.STRING, false),
                new StructField("age", IntegerType.INTEGER, false)));

    List<Map<String, Object>> data =
        List.of(Map.of("name", "Alice", "age", 30), Map.of("name", "Bob", "age", 25));

    assertDoesNotThrow(
        () -> {
          var batch = DeltaLakeDataConverter.convertToColumnarBatch(data, schema);
          assertTrue(batch.hasNext());
          var filteredBatch = batch.next();
          assertEquals(2, filteredBatch.getData().getSize());
        });
  }

  private static ColumnVector column(DataType type, Object value) {
    Map<String, Object> record = new HashMap<>();
    record.put("value", value);
    return DeltaLakeDataConverter.convertSingleBatch(
            List.of(record), new StructType().add("value", type, true))
        .getData()
        .getColumnVector(0);
  }

  @Test
  void mapVectorsPreservePairsTypesNullsAndInput() {
    Map<String, Object> input = new LinkedHashMap<>();
    input.put("z", "42");
    input.put("a", null);
    input.put("m", 9);
    Map<String, Object> original = new LinkedHashMap<>(input);
    MapValue map = column(new MapType(StringType.STRING, LongType.LONG, true), input).getMap(0);
    assertEquals(3, map.getSize());
    assertEquals(StringType.STRING, map.getKeys().getDataType());
    assertEquals(LongType.LONG, map.getValues().getDataType());
    assertEquals(3, map.getKeys().getSize());
    assertEquals(3, map.getValues().getSize());
    assertEquals("z", map.getKeys().getString(0));
    assertEquals(42L, map.getValues().getLong(0));
    assertEquals("a", map.getKeys().getString(1));
    assertTrue(map.getValues().isNullAt(1));
    assertEquals("m", map.getKeys().getString(2));
    assertEquals(9L, map.getValues().getLong(2));
    assertEquals(original, input);
    assertEquals("42", input.get("z"));
  }

  @Test
  void emptyAndNullMapsHaveCorrectVectors() {
    MapType type = new MapType(StringType.STRING, StringType.STRING, true);
    MapValue empty = column(type, Map.of()).getMap(0);
    assertEquals(0, empty.getSize());
    assertEquals(0, empty.getKeys().getSize());
    assertEquals(0, empty.getValues().getSize());
    ColumnVector absent = column(type, null);
    assertTrue(absent.isNullAt(0));
    assertNull(absent.getMap(0));
  }

  @Test
  void invalidMapKeysValuesAndShapeAreMarked() {
    MapType type = new MapType(StringType.STRING, LongType.LONG, false);
    Map<Object, Object> nullKey = new HashMap<>();
    nullKey.put(null, 1L);
    Map<String, Object> nullValue = new HashMap<>();
    nullValue.put("a", null);
    for (Object invalid : List.of(nullKey, Map.of(1, 2L), nullValue, Map.of("bad", "NaN"))) {
      assertThrows(InvalidRecordException.class, () -> column(type, invalid).getMap(0));
    }
    assertThrows(InvalidRecordException.class, () -> column(type, List.of("bad")));
  }

  @Test
  void mapKeysAreNotStringifiedOrDeduplicated() {
    Map<Object, Object> input = new LinkedHashMap<>();
    input.put("1", "string key");
    input.put(1, "numeric key");
    assertThrows(
        InvalidRecordException.class,
        () -> column(new MapType(StringType.STRING, StringType.STRING, true), input).getMap(0));
    assertEquals(2, input.size());
  }

  @Test
  void lazyNestedRepresentationFailuresAreMarkedWithoutChangingValidCoercions() {
    StructType inner = new StructType().add("count", LongType.LONG, true);
    ColumnVector valid = column(inner, Map.of("count", 12));
    assertEquals(12L, valid.getChild(0).getLong(0));
    ColumnVector invalid = column(inner, Map.of("count", "not-a-number"));
    assertThrows(InvalidRecordException.class, () -> invalid.getChild(0));
    ColumnVector parentNull = column(inner, null);
    assertTrue(parentNull.getChild(0).isNullAt(0));
    assertEquals(12L, column(LongType.LONG, "12").getLong(0));
    assertEquals((byte) 1, column(ByteType.BYTE, 257).getByte(0));
    assertFalse(column(BooleanType.BOOLEAN, "anything").getBoolean(0));
    assertEquals("12", column(StringType.STRING, 12).getString(0));
  }

  static Stream<Arguments> invalidScalars() {
    return Stream.of(
            ByteType.BYTE,
            ShortType.SHORT,
            IntegerType.INTEGER,
            LongType.LONG,
            FloatType.FLOAT,
            DoubleType.DOUBLE,
            DateType.DATE,
            TimestampType.TIMESTAMP,
            TimestampNTZType.TIMESTAMP_NTZ,
            new DecimalType(20, 2))
        .map(type -> Arguments.of(type, "invalid"));
  }

  @ParameterizedTest
  @MethodSource("invalidScalars")
  void scalarParsingFailuresAreMarked(DataType type, Object value) {
    assertThrows(InvalidRecordException.class, () -> column(type, value));
    assertThrows(
        InvalidRecordException.class,
        () -> column(new MapType(StringType.STRING, type, true), Map.of("bad", value)).getMap(0));
  }

  @Test
  void schemaGetterAndOrdinalFailuresAreNotDataErrors() {
    ColumnVector vector =
        column(new MapType(StringType.STRING, LongType.LONG, true), Map.of("a", 1));
    assertThrows(UnsupportedOperationException.class, () -> vector.getArray(0));
    assertThrows(IndexOutOfBoundsException.class, () -> vector.getMap(1));
    assertThrows(
        UnsupportedOperationException.class,
        () ->
            column(new MapType(IntegerType.INTEGER, LongType.LONG, true), Map.of(1, 2)).getMap(0));
    ColumnVector struct = column(new StructType().add("id", LongType.LONG, true), Map.of("id", 1L));
    RuntimeException ordinal = assertThrows(RuntimeException.class, () -> struct.getChild(9));
    assertFalse(ordinal instanceof InvalidRecordException);
    assertThrows(
        IllegalArgumentException.class, () -> DeltaLakeDataConverter.inferSchema(List.of()));
  }

  @Test
  void requiredFieldsAndLazyBinaryBooleanArrayFailuresAreMarked() {
    StructType required = new StructType().add("required", StringType.STRING, false);
    assertThrows(
        InvalidRecordException.class,
        () -> DeltaLakeDataConverter.convertSingleBatch(List.of(Map.of()), required));
    for (DataType type :
        List.of(BinaryType.BINARY, BooleanType.BOOLEAN, new ArrayType(LongType.LONG, true))) {
      StructType struct = new StructType().add("nested", type, true);
      ColumnVector invalid = column(struct, Map.of("nested", "invalid"));
      assertThrows(
          InvalidRecordException.class,
          () -> {
            ColumnVector child = invalid.getChild(0);
            if (type instanceof ArrayType) child.getArray(0);
          });
    }
    StructType nullable =
        new StructType()
            .add("binary", BinaryType.BINARY, true)
            .add("bool", BooleanType.BOOLEAN, true);
    ColumnVector valid = column(nullable, Map.of("binary", new byte[] {1, 2}, "bool", true));
    assertArrayEquals(new byte[] {1, 2}, valid.getChild(0).getBinary(0));
    assertTrue(valid.getChild(1).getBoolean(0));
  }
}
