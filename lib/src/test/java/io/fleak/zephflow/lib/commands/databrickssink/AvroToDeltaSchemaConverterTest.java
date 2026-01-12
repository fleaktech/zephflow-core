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
import java.util.List;
import java.util.Map;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

class AvroToDeltaSchemaConverterTest {

  @Test
  void testPrimitiveTypes() {
    Schema avroSchema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .requiredString("stringField")
            .requiredInt("intField")
            .requiredLong("longField")
            .requiredFloat("floatField")
            .requiredDouble("doubleField")
            .requiredBoolean("booleanField")
            .requiredBytes("bytesField")
            .endRecord();

    StructType expected =
        new StructType(
            List.of(
                new StructField("stringField", StringType.STRING, false),
                new StructField("intField", IntegerType.INTEGER, false),
                new StructField("longField", LongType.LONG, false),
                new StructField("floatField", FloatType.FLOAT, false),
                new StructField("doubleField", DoubleType.DOUBLE, false),
                new StructField("booleanField", BooleanType.BOOLEAN, false),
                new StructField("bytesField", BinaryType.BINARY, false)));

    StructType actual = AvroToDeltaSchemaConverter.convert(avroSchema);
    assertEquals(expected, actual);
  }

  @Test
  void testEnumType() {
    Schema enumSchema =
        SchemaBuilder.enumeration("Status").symbols("ACTIVE", "INACTIVE", "PENDING");
    Schema avroSchema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .name("status")
            .type(enumSchema)
            .noDefault()
            .endRecord();

    StructType expected =
        new StructType(List.of(new StructField("status", StringType.STRING, false)));

    StructType actual = AvroToDeltaSchemaConverter.convert(avroSchema);
    assertEquals(expected, actual);
  }

  @Test
  void testDateLogicalType() {
    Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema avroSchema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .name("dateField")
            .type(dateSchema)
            .noDefault()
            .endRecord();

    StructType expected =
        new StructType(List.of(new StructField("dateField", DateType.DATE, false)));

    StructType actual = AvroToDeltaSchemaConverter.convert(avroSchema);
    assertEquals(expected, actual);
  }

  @Test
  void testTimestampMillisLogicalType() {
    Schema timestampSchema =
        LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema avroSchema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .name("timestampField")
            .type(timestampSchema)
            .noDefault()
            .endRecord();

    StructType expected =
        new StructType(List.of(new StructField("timestampField", TimestampType.TIMESTAMP, false)));

    StructType actual = AvroToDeltaSchemaConverter.convert(avroSchema);
    assertEquals(expected, actual);
  }

  @Test
  void testTimestampMicrosLogicalType() {
    Schema timestampSchema =
        LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    Schema avroSchema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .name("timestampField")
            .type(timestampSchema)
            .noDefault()
            .endRecord();

    StructType expected =
        new StructType(List.of(new StructField("timestampField", TimestampType.TIMESTAMP, false)));

    StructType actual = AvroToDeltaSchemaConverter.convert(avroSchema);
    assertEquals(expected, actual);
  }

  @Test
  void testTimeLogicalTypesFallback() {
    // Verify that Time types degrade to Primitives (Int/Long) as expected
    Schema timeMillis = LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT));
    Schema timeMicros = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));

    Schema avroSchema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .name("timeMillisField")
            .type(timeMillis)
            .noDefault()
            .name("timeMicrosField")
            .type(timeMicros)
            .noDefault()
            .endRecord();

    StructType expected =
        new StructType(
            List.of(
                new StructField("timeMillisField", IntegerType.INTEGER, false),
                new StructField("timeMicrosField", LongType.LONG, false)));

    StructType actual = AvroToDeltaSchemaConverter.convert(avroSchema);
    assertEquals(expected, actual);
  }

  @Test
  void testDecimalLogicalTypeOnBytes() {
    Schema decimalSchema =
        LogicalTypes.decimal(10, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema avroSchema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .name("decimalField")
            .type(decimalSchema)
            .noDefault()
            .endRecord();

    StructType expected =
        new StructType(List.of(new StructField("decimalField", new DecimalType(10, 2), false)));

    StructType actual = AvroToDeltaSchemaConverter.convert(avroSchema);
    assertEquals(expected, actual);
  }

  @Test
  void testDecimalLogicalTypeOnFixed() {
    Schema fixedSchema = Schema.createFixed("DecimalFixed", null, null, 16);
    Schema decimalSchema = LogicalTypes.decimal(38, 10).addToSchema(fixedSchema);
    Schema avroSchema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .name("decimalField")
            .type(decimalSchema)
            .noDefault()
            .endRecord();

    StructType expected =
        new StructType(List.of(new StructField("decimalField", new DecimalType(38, 10), false)));

    StructType actual = AvroToDeltaSchemaConverter.convert(avroSchema);
    assertEquals(expected, actual);
  }

  @Test
  void testFixedTypeWithoutLogicalType() {
    Schema fixedSchema = Schema.createFixed("FixedBytes", null, null, 16);
    Schema avroSchema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .name("fixedField")
            .type(fixedSchema)
            .noDefault()
            .endRecord();

    StructType expected =
        new StructType(List.of(new StructField("fixedField", BinaryType.BINARY, false)));

    StructType actual = AvroToDeltaSchemaConverter.convert(avroSchema);
    assertEquals(expected, actual);
  }

  @Test
  void testArrayType() {
    Schema avroSchema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .name("tags")
            .type()
            .array()
            .items()
            .stringType()
            .noDefault()
            .endRecord();

    StructType expected =
        new StructType(
            List.of(new StructField("tags", new ArrayType(StringType.STRING, false), false)));

    StructType actual = AvroToDeltaSchemaConverter.convert(avroSchema);
    assertEquals(expected, actual);
  }

  @Test
  void testArrayTypeWithNullableElements() {
    Schema nullableString = SchemaBuilder.nullable().stringType();
    Schema arraySchema = Schema.createArray(nullableString);
    Schema avroSchema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .name("tags")
            .type(arraySchema)
            .noDefault()
            .endRecord();

    StructType expected =
        new StructType(
            List.of(new StructField("tags", new ArrayType(StringType.STRING, true), false)));

    StructType actual = AvroToDeltaSchemaConverter.convert(avroSchema);
    assertEquals(expected, actual);
  }

  @Test
  void testMapType() {
    Schema avroSchema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .name("metadata")
            .type()
            .map()
            .values()
            .intType()
            .noDefault()
            .endRecord();

    StructType expected =
        new StructType(
            List.of(
                new StructField(
                    "metadata",
                    new MapType(StringType.STRING, IntegerType.INTEGER, false),
                    false)));

    StructType actual = AvroToDeltaSchemaConverter.convert(avroSchema);
    assertEquals(expected, actual);
  }

  @Test
  void testMapTypeWithNullableValues() {
    Schema nullableInt = SchemaBuilder.nullable().intType();
    Schema mapSchema = Schema.createMap(nullableInt);
    Schema avroSchema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .name("metadata")
            .type(mapSchema)
            .noDefault()
            .endRecord();

    StructType expected =
        new StructType(
            List.of(
                new StructField(
                    "metadata", new MapType(StringType.STRING, IntegerType.INTEGER, true), false)));

    StructType actual = AvroToDeltaSchemaConverter.convert(avroSchema);
    assertEquals(expected, actual);
  }

  @Test
  void testNestedRecord() {
    Schema addressSchema =
        SchemaBuilder.record("Address")
            .fields()
            .requiredString("street")
            .requiredString("city")
            .optionalInt("zipCode")
            .endRecord();

    Schema avroSchema =
        SchemaBuilder.record("Person")
            .fields()
            .requiredString("name")
            .name("address")
            .type(addressSchema)
            .noDefault()
            .endRecord();

    StructType expectedAddress =
        new StructType(
            List.of(
                new StructField("street", StringType.STRING, false),
                new StructField("city", StringType.STRING, false),
                new StructField("zipCode", IntegerType.INTEGER, true)));

    StructType expected =
        new StructType(
            List.of(
                new StructField("name", StringType.STRING, false),
                new StructField("address", expectedAddress, false)));

    StructType actual = AvroToDeltaSchemaConverter.convert(avroSchema);
    assertEquals(expected, actual);
  }

  @Test
  void testNullableFields() {
    Schema avroSchema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .optionalString("nullableString")
            .optionalInt("nullableInt")
            .optionalLong("nullableLong")
            .endRecord();

    StructType expected =
        new StructType(
            List.of(
                new StructField("nullableString", StringType.STRING, true),
                new StructField("nullableInt", IntegerType.INTEGER, true),
                new StructField("nullableLong", LongType.LONG, true)));

    StructType actual = AvroToDeltaSchemaConverter.convert(avroSchema);
    assertEquals(expected, actual);
  }

  @Test
  void testMixedNullableAndNonNullableFields() {
    Schema avroSchema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .requiredString("requiredField")
            .optionalInt("optionalField")
            .requiredLong("anotherRequired")
            .endRecord();

    StructType expected =
        new StructType(
            List.of(
                new StructField("requiredField", StringType.STRING, false),
                new StructField("optionalField", IntegerType.INTEGER, true),
                new StructField("anotherRequired", LongType.LONG, false)));

    StructType actual = AvroToDeltaSchemaConverter.convert(avroSchema);
    assertEquals(expected, actual);
  }

  @Test
  void testParseFromJsonString() {
    String jsonSchema =
        """
        {
          "type": "record",
          "name": "TestRecord",
          "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"}
          ]
        }
        """;

    StructType expected =
        new StructType(
            List.of(
                new StructField("id", LongType.LONG, false),
                new StructField("name", StringType.STRING, false)));

    StructType actual = AvroToDeltaSchemaConverter.parse(jsonSchema);
    assertEquals(expected, actual);
  }

  @Test
  void testParseFromMap() {
    Map<String, Object> schemaMap =
        Map.of(
            "type", "record",
            "name", "TestRecord",
            "fields",
                List.of(
                    Map.of("name", "count", "type", "int"),
                    Map.of("name", "value", "type", "double")));

    StructType expected =
        new StructType(
            List.of(
                new StructField("count", IntegerType.INTEGER, false),
                new StructField("value", DoubleType.DOUBLE, false)));

    StructType actual = AvroToDeltaSchemaConverter.parse(schemaMap);
    assertEquals(expected, actual);
  }

  @Test
  void testNonRecordSchemaThrowsException() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> AvroToDeltaSchemaConverter.convert(stringSchema));

    assertEquals("Avro schema must be a RECORD type, got: STRING", exception.getMessage());
  }

  @Test
  void testComplexUnionThrowsException() {
    Schema unionSchema = SchemaBuilder.unionOf().stringType().and().intType().endUnion();
    Schema avroSchema =
        SchemaBuilder.record("TestRecord")
            .fields()
            .name("complexUnion")
            .type(unionSchema)
            .noDefault()
            .endRecord();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> AvroToDeltaSchemaConverter.convert(avroSchema));

    assertTrue(exception.getMessage().contains("Complex union types not supported"));
  }

  @Test
  void testComplexUnionFromMapThrowsDescriptiveException() {
    Map<String, Object> schemaMap =
        Map.of(
            "type", "record",
            "name", "TestRecord",
            "fields",
                List.of(
                    Map.of(
                        "name",
                        "attributes",
                        "type",
                        Map.of(
                            "type",
                            "map",
                            "values",
                            List.of("null", "string", "int", "long", "double", "boolean")))));

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> AvroToDeltaSchemaConverter.parse(schemaMap));

    assertTrue(
        exception.getMessage().contains("Complex union types"),
        "Expected error about complex union types, got: " + exception.getMessage());
  }

  @Test
  void testComplexTypesIntegration() {
    Schema timestampSchema =
        LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
    Schema dateSchema = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    Schema decimalSchema =
        LogicalTypes.decimal(18, 4).addToSchema(Schema.create(Schema.Type.BYTES));

    Schema avroSchema =
        SchemaBuilder.record("Transaction")
            .fields()
            .requiredLong("id")
            .name("createdAt")
            .type(timestampSchema)
            .noDefault()
            .name("transactionDate")
            .type(dateSchema)
            .noDefault()
            .name("amount")
            .type(decimalSchema)
            .noDefault()
            .name("tags")
            .type()
            .array()
            .items()
            .stringType()
            .noDefault()
            .name("metadata")
            .type()
            .map()
            .values()
            .stringType()
            .noDefault()
            .optionalString("notes")
            .endRecord();

    StructType expected =
        new StructType(
            List.of(
                new StructField("id", LongType.LONG, false),
                new StructField("createdAt", TimestampType.TIMESTAMP, false),
                new StructField("transactionDate", DateType.DATE, false),
                new StructField("amount", new DecimalType(18, 4), false),
                new StructField("tags", new ArrayType(StringType.STRING, false), false),
                new StructField(
                    "metadata", new MapType(StringType.STRING, StringType.STRING, false), false),
                new StructField("notes", StringType.STRING, true)));

    StructType actual = AvroToDeltaSchemaConverter.convert(avroSchema);
    assertEquals(expected, actual);
  }
}
