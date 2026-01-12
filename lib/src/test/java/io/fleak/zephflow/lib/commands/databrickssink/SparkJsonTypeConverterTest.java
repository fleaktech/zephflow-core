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

import com.databricks.sdk.service.catalog.ColumnInfo;
import io.delta.kernel.types.*;
import java.util.List;
import org.junit.jupiter.api.Test;

class SparkJsonTypeConverterTest {

  @Test
  void testSimpleString() {
    DataType result = SparkJsonTypeConverter.convert("string");
    assertEquals(StringType.STRING, result);
  }

  @Test
  void testSimpleInteger() {
    DataType result = SparkJsonTypeConverter.convert("integer");
    assertEquals(IntegerType.INTEGER, result);
  }

  @Test
  void testSimpleInt() {
    DataType result = SparkJsonTypeConverter.convert("int");
    assertEquals(IntegerType.INTEGER, result);
  }

  @Test
  void testSimpleLong() {
    DataType result = SparkJsonTypeConverter.convert("long");
    assertEquals(LongType.LONG, result);
  }

  @Test
  void testSimpleBigint() {
    DataType result = SparkJsonTypeConverter.convert("bigint");
    assertEquals(LongType.LONG, result);
  }

  @Test
  void testSimpleShort() {
    DataType result = SparkJsonTypeConverter.convert("short");
    assertEquals(ShortType.SHORT, result);
  }

  @Test
  void testSimpleByte() {
    DataType result = SparkJsonTypeConverter.convert("byte");
    assertEquals(ByteType.BYTE, result);
  }

  @Test
  void testSimpleFloat() {
    DataType result = SparkJsonTypeConverter.convert("float");
    assertEquals(FloatType.FLOAT, result);
  }

  @Test
  void testSimpleDouble() {
    DataType result = SparkJsonTypeConverter.convert("double");
    assertEquals(DoubleType.DOUBLE, result);
  }

  @Test
  void testSimpleBoolean() {
    DataType result = SparkJsonTypeConverter.convert("boolean");
    assertEquals(BooleanType.BOOLEAN, result);
  }

  @Test
  void testSimpleBinary() {
    DataType result = SparkJsonTypeConverter.convert("binary");
    assertEquals(BinaryType.BINARY, result);
  }

  @Test
  void testSimpleDate() {
    DataType result = SparkJsonTypeConverter.convert("date");
    assertEquals(DateType.DATE, result);
  }

  @Test
  void testSimpleTimestamp() {
    DataType result = SparkJsonTypeConverter.convert("timestamp");
    assertEquals(TimestampType.TIMESTAMP, result);
  }

  @Test
  void testSimpleTimestampNtz() {
    DataType result = SparkJsonTypeConverter.convert("timestamp_ntz");
    assertEquals(TimestampNTZType.TIMESTAMP_NTZ, result);
  }

  @Test
  void testDecimal() {
    DataType result = SparkJsonTypeConverter.convert("decimal(10,2)");
    DecimalType expected = new DecimalType(10, 2);
    assertEquals(expected, result);
  }

  @Test
  void testDecimalWithSpaces() {
    DataType result = SparkJsonTypeConverter.convert("decimal(18, 4)");
    DecimalType expected = new DecimalType(18, 4);
    assertEquals(expected, result);
  }

  @Test
  void testDecimalAsObject() {
    String json = "{\"type\":\"decimal\",\"precision\":10,\"scale\":2}";
    DataType result = SparkJsonTypeConverter.convert(json);
    DecimalType expected = new DecimalType(10, 2);
    assertEquals(expected, result);
  }

  @Test
  void testDecimalAsObjectDefaults() {
    String json = "{\"type\":\"decimal\"}";
    DataType result = SparkJsonTypeConverter.convert(json);
    DecimalType expected = new DecimalType(10, 0);
    assertEquals(expected, result);
  }

  @Test
  void testColumnDefinitionWithSimpleType() {
    String json =
        "{\"name\":\"employee_id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}";
    DataType result = SparkJsonTypeConverter.convert(json);
    assertEquals(IntegerType.INTEGER, result);
  }

  @Test
  void testColumnDefinitionWithStringType() {
    String json = "{\"name\":\"first_name\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}";
    DataType result = SparkJsonTypeConverter.convert(json);
    assertEquals(StringType.STRING, result);
  }

  @Test
  void testColumnDefinitionWithDateType() {
    String json = "{\"name\":\"hire_date\",\"type\":\"date\",\"nullable\":true,\"metadata\":{}}";
    DataType result = SparkJsonTypeConverter.convert(json);
    assertEquals(DateType.DATE, result);
  }

  @Test
  void testColumnDefinitionWithArrayType() {
    String json =
        "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true},\"nullable\":true,\"metadata\":{}}";
    DataType result = SparkJsonTypeConverter.convert(json);
    assertEquals(new ArrayType(StringType.STRING, true), result);
  }

  @Test
  void testArrayTypeSimple() {
    String json = "{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true}";
    DataType result = SparkJsonTypeConverter.convert(json);

    ArrayType expected = new ArrayType(StringType.STRING, true);
    assertEquals(expected, result);
  }

  @Test
  void testArrayTypeNotNull() {
    String json = "{\"type\":\"array\",\"elementType\":\"integer\",\"containsNull\":false}";
    DataType result = SparkJsonTypeConverter.convert(json);

    ArrayType expected = new ArrayType(IntegerType.INTEGER, false);
    assertEquals(expected, result);
  }

  @Test
  void testMapTypeSimple() {
    String json =
        "{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"integer\",\"valueContainsNull\":true}";
    DataType result = SparkJsonTypeConverter.convert(json);

    MapType expected = new MapType(StringType.STRING, IntegerType.INTEGER, true);
    assertEquals(expected, result);
  }

  @Test
  void testMapTypeNotNull() {
    String json =
        "{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"long\",\"valueContainsNull\":false}";
    DataType result = SparkJsonTypeConverter.convert(json);

    MapType expected = new MapType(StringType.STRING, LongType.LONG, false);
    assertEquals(expected, result);
  }

  @Test
  void testStructTypeSimple() {
    String json =
        "{\"type\":\"struct\",\"fields\":["
            + "{\"name\":\"city\",\"type\":\"string\",\"nullable\":true},"
            + "{\"name\":\"zip\",\"type\":\"integer\",\"nullable\":false}"
            + "]}";
    DataType result = SparkJsonTypeConverter.convert(json);

    StructType expected =
        new StructType()
            .add("city", StringType.STRING, true)
            .add("zip", IntegerType.INTEGER, false);
    assertEquals(expected, result);
  }

  @Test
  void testNestedArrayInStruct() {
    String json =
        "{\"type\":\"struct\",\"fields\":["
            + "{\"name\":\"tags\",\"type\":{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true},\"nullable\":true}"
            + "]}";
    DataType result = SparkJsonTypeConverter.convert(json);

    StructType expected =
        new StructType().add("tags", new ArrayType(StringType.STRING, true), true);
    assertEquals(expected, result);
  }

  @Test
  void testNestedMapInStruct() {
    String json =
        "{\"type\":\"struct\",\"fields\":["
            + "{\"name\":\"metadata\",\"type\":{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"string\",\"valueContainsNull\":true},\"nullable\":true}"
            + "]}";
    DataType result = SparkJsonTypeConverter.convert(json);

    StructType expected =
        new StructType()
            .add("metadata", new MapType(StringType.STRING, StringType.STRING, true), true);
    assertEquals(expected, result);
  }

  @Test
  void testNestedStructInStruct() {
    String json =
        "{\"type\":\"struct\",\"fields\":["
            + "{\"name\":\"address\",\"type\":{\"type\":\"struct\",\"fields\":["
            + "{\"name\":\"city\",\"type\":\"string\",\"nullable\":true},"
            + "{\"name\":\"zip\",\"type\":\"integer\",\"nullable\":true}"
            + "]},\"nullable\":true}"
            + "]}";
    DataType result = SparkJsonTypeConverter.convert(json);

    StructType innerStruct =
        new StructType().add("city", StringType.STRING, true).add("zip", IntegerType.INTEGER, true);
    StructType expected = new StructType().add("address", innerStruct, true);
    assertEquals(expected, result);
  }

  @Test
  void testUdtWithSimpleSqlType() {
    String json = "{\"type\":\"udt\",\"class\":\"org.example.MyType\",\"sqlType\":\"string\"}";
    DataType result = SparkJsonTypeConverter.convert(json);
    assertEquals(StringType.STRING, result);
  }

  @Test
  void testUdtWithStructSqlType() {
    String json =
        "{\"type\":\"udt\",\"class\":\"org.example.Point\",\"sqlType\":{\"type\":\"struct\",\"fields\":["
            + "{\"name\":\"x\",\"type\":\"double\",\"nullable\":false},"
            + "{\"name\":\"y\",\"type\":\"double\",\"nullable\":false}"
            + "]}}";
    DataType result = SparkJsonTypeConverter.convert(json);

    StructType expected =
        new StructType().add("x", DoubleType.DOUBLE, false).add("y", DoubleType.DOUBLE, false);
    assertEquals(expected, result);
  }

  @Test
  void testUdtMissingSqlType() {
    String json = "{\"type\":\"udt\",\"class\":\"org.example.MyType\"}";
    assertThrows(IllegalArgumentException.class, () -> SparkJsonTypeConverter.convert(json));
  }

  @Test
  void testNullTypeJson() {
    assertThrows(IllegalArgumentException.class, () -> SparkJsonTypeConverter.convert(null));
  }

  @Test
  void testBlankTypeJson() {
    assertThrows(IllegalArgumentException.class, () -> SparkJsonTypeConverter.convert("   "));
  }

  @Test
  void testUnknownType() {
    assertThrows(IllegalArgumentException.class, () -> SparkJsonTypeConverter.convert("unknown"));
  }

  @Test
  void testInvalidJson() {
    assertThrows(
        IllegalArgumentException.class, () -> SparkJsonTypeConverter.convert("{invalid json}"));
  }

  @Test
  void testBuildStructTypeFromColumns() {
    List<ColumnInfo> columns =
        List.of(
            new ColumnInfo().setName("id").setTypeJson("integer").setNullable(false),
            new ColumnInfo().setName("name").setTypeJson("string").setNullable(true),
            new ColumnInfo().setName("balance").setTypeJson("decimal(10,2)").setNullable(true));

    StructType result = SparkJsonTypeConverter.buildStructTypeFromColumns(columns);

    StructType expected =
        new StructType()
            .add("id", IntegerType.INTEGER, false)
            .add("name", StringType.STRING, true)
            .add("balance", new DecimalType(10, 2), true);
    assertEquals(expected, result);
  }

  @Test
  void testBuildStructTypeFromColumnsWithArray() {
    List<ColumnInfo> columns =
        List.of(
            new ColumnInfo()
                .setName("tags")
                .setTypeJson(
                    "{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true}")
                .setNullable(true));

    StructType result = SparkJsonTypeConverter.buildStructTypeFromColumns(columns);

    StructType expected =
        new StructType().add("tags", new ArrayType(StringType.STRING, true), true);
    assertEquals(expected, result);
  }

  @Test
  void testBuildStructTypeFromColumnsNullableDefault() {
    List<ColumnInfo> columns = List.of(new ColumnInfo().setName("id").setTypeJson("integer"));

    StructType result = SparkJsonTypeConverter.buildStructTypeFromColumns(columns);

    StructType expected = new StructType().add("id", IntegerType.INTEGER, true);
    assertEquals(expected, result);
  }

  @Test
  void testBuildStructTypeFromColumnsWithMissingTypeJson() {
    List<ColumnInfo> columns = List.of(new ColumnInfo().setName("id").setTypeJson(null));

    assertThrows(
        IllegalArgumentException.class,
        () -> SparkJsonTypeConverter.buildStructTypeFromColumns(columns));
  }

  @Test
  void testCaseInsensitiveTypes() {
    assertEquals(StringType.STRING, SparkJsonTypeConverter.convert("STRING"));
    assertEquals(IntegerType.INTEGER, SparkJsonTypeConverter.convert("INTEGER"));
    assertEquals(LongType.LONG, SparkJsonTypeConverter.convert("BIGINT"));
  }
}
