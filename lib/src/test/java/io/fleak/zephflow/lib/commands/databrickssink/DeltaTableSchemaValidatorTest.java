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
import static org.mockito.Mockito.*;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.ColumnInfo;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TablesAPI;
import io.delta.kernel.types.*;
import io.fleak.zephflow.lib.commands.databrickssink.DeltaTableSchemaValidator.SchemaValidationException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DeltaTableSchemaValidatorTest {

  private WorkspaceClient workspaceClient;
  private TablesAPI tablesAPI;
  private DeltaTableSchemaValidator validator;

  @BeforeEach
  void setUp() {
    workspaceClient = mock(WorkspaceClient.class);
    tablesAPI = mock(TablesAPI.class);
    when(workspaceClient.tables()).thenReturn(tablesAPI);
    validator = new DeltaTableSchemaValidator(workspaceClient);
  }

  @Test
  void testValidSchema_ExactMatch() {
    StructType expectedSchema =
        new StructType().add("id", IntegerType.INTEGER, false).add("name", StringType.STRING, true);

    TableInfo tableInfo =
        new TableInfo()
            .setColumns(
                List.of(
                    new ColumnInfo().setName("id").setTypeJson("integer").setNullable(false),
                    new ColumnInfo().setName("name").setTypeJson("string").setNullable(true)));

    when(tablesAPI.get("catalog.schema.table")).thenReturn(tableInfo);

    assertDoesNotThrow(() -> validator.validateSchema("catalog.schema.table", expectedSchema));
  }

  @Test
  void testValidSchema_WithTypeWidening_IntToLong() {
    StructType expectedSchema = new StructType().add("count", IntegerType.INTEGER, false);

    TableInfo tableInfo =
        new TableInfo()
            .setColumns(
                List.of(new ColumnInfo().setName("count").setTypeJson("long").setNullable(false)));

    when(tablesAPI.get("catalog.schema.table")).thenReturn(tableInfo);

    assertDoesNotThrow(() -> validator.validateSchema("catalog.schema.table", expectedSchema));
  }

  @Test
  void testValidSchema_WithTypeWidening_FloatToDouble() {
    StructType expectedSchema = new StructType().add("value", FloatType.FLOAT, false);

    TableInfo tableInfo =
        new TableInfo()
            .setColumns(
                List.of(
                    new ColumnInfo().setName("value").setTypeJson("double").setNullable(false)));

    when(tablesAPI.get("catalog.schema.table")).thenReturn(tableInfo);

    assertDoesNotThrow(() -> validator.validateSchema("catalog.schema.table", expectedSchema));
  }

  @Test
  void testValidSchema_ExtraTableColumns() {
    StructType expectedSchema = new StructType().add("id", IntegerType.INTEGER, false);

    TableInfo tableInfo =
        new TableInfo()
            .setColumns(
                List.of(
                    new ColumnInfo().setName("id").setTypeJson("integer").setNullable(false),
                    new ColumnInfo().setName("extra_col").setTypeJson("string").setNullable(true)));

    when(tablesAPI.get("catalog.schema.table")).thenReturn(tableInfo);

    assertDoesNotThrow(() -> validator.validateSchema("catalog.schema.table", expectedSchema));
  }

  @Test
  void testValidSchema_CaseInsensitiveColumnNames() {
    StructType expectedSchema = new StructType().add("UserId", IntegerType.INTEGER, false);

    TableInfo tableInfo =
        new TableInfo()
            .setColumns(
                List.of(
                    new ColumnInfo().setName("userid").setTypeJson("integer").setNullable(false)));

    when(tablesAPI.get("catalog.schema.table")).thenReturn(tableInfo);

    assertDoesNotThrow(() -> validator.validateSchema("catalog.schema.table", expectedSchema));
  }

  @Test
  void testInvalidSchema_MissingColumn() {
    StructType expectedSchema =
        new StructType()
            .add("id", IntegerType.INTEGER, false)
            .add("missing_col", StringType.STRING, true);

    TableInfo tableInfo =
        new TableInfo()
            .setColumns(
                List.of(new ColumnInfo().setName("id").setTypeJson("integer").setNullable(false)));

    when(tablesAPI.get("catalog.schema.table")).thenReturn(tableInfo);

    SchemaValidationException ex =
        assertThrows(
            SchemaValidationException.class,
            () -> validator.validateSchema("catalog.schema.table", expectedSchema));

    assertTrue(ex.getMessage().contains("Missing field"));
    assertTrue(ex.getMessage().contains("missing_col"));
  }

  @Test
  void testInvalidSchema_TypeMismatch() {
    StructType expectedSchema = new StructType().add("id", StringType.STRING, false);

    TableInfo tableInfo =
        new TableInfo()
            .setColumns(
                List.of(new ColumnInfo().setName("id").setTypeJson("integer").setNullable(false)));

    when(tablesAPI.get("catalog.schema.table")).thenReturn(tableInfo);

    SchemaValidationException ex =
        assertThrows(
            SchemaValidationException.class,
            () -> validator.validateSchema("catalog.schema.table", expectedSchema));

    assertTrue(ex.getMessage().contains("expected STRING"));
    assertTrue(ex.getMessage().contains("actual INTEGER"));
  }

  @Test
  void testInvalidSchema_TypeMismatch_LongToInt() {
    StructType expectedSchema = new StructType().add("bignum", LongType.LONG, false);

    TableInfo tableInfo =
        new TableInfo()
            .setColumns(
                List.of(
                    new ColumnInfo().setName("bignum").setTypeJson("integer").setNullable(false)));

    when(tablesAPI.get("catalog.schema.table")).thenReturn(tableInfo);

    SchemaValidationException ex =
        assertThrows(
            SchemaValidationException.class,
            () -> validator.validateSchema("catalog.schema.table", expectedSchema));

    assertTrue(ex.getMessage().contains("expected LONG"));
    assertTrue(ex.getMessage().contains("actual INTEGER"));
  }

  @Test
  void testTableNotFound() {
    StructType expectedSchema = new StructType().add("id", IntegerType.INTEGER, false);

    when(tablesAPI.get("catalog.schema.nonexistent"))
        .thenThrow(new RuntimeException("TABLE_NOT_FOUND: Table does not exist"));

    SchemaValidationException ex =
        assertThrows(
            SchemaValidationException.class,
            () -> validator.validateSchema("catalog.schema.nonexistent", expectedSchema));

    assertTrue(ex.getMessage().contains("does not exist"));
  }

  @Test
  void testPermissionError() {
    StructType expectedSchema = new StructType().add("id", IntegerType.INTEGER, false);

    when(tablesAPI.get("catalog.schema.restricted"))
        .thenThrow(new RuntimeException("PERMISSION_DENIED: Access denied"));

    SchemaValidationException ex =
        assertThrows(
            SchemaValidationException.class,
            () -> validator.validateSchema("catalog.schema.restricted", expectedSchema));

    assertTrue(ex.getMessage().contains("Cannot access table metadata"));
    assertTrue(ex.getMessage().contains("SELECT permission"));
  }

  @Test
  void testDecimalCompatibility_Valid() {
    StructType expectedSchema = new StructType().add("amount", new DecimalType(10, 2), false);

    TableInfo tableInfo =
        new TableInfo()
            .setColumns(
                List.of(
                    new ColumnInfo()
                        .setName("amount")
                        .setTypeJson("decimal(18,4)")
                        .setNullable(false)));

    when(tablesAPI.get("catalog.schema.table")).thenReturn(tableInfo);

    assertDoesNotThrow(() -> validator.validateSchema("catalog.schema.table", expectedSchema));
  }

  @Test
  void testDecimalCompatibility_Invalid_PrecisionTooSmall() {
    StructType expectedSchema = new StructType().add("amount", new DecimalType(20, 5), false);

    TableInfo tableInfo =
        new TableInfo()
            .setColumns(
                List.of(
                    new ColumnInfo()
                        .setName("amount")
                        .setTypeJson("decimal(10,2)")
                        .setNullable(false)));

    when(tablesAPI.get("catalog.schema.table")).thenReturn(tableInfo);

    SchemaValidationException ex =
        assertThrows(
            SchemaValidationException.class,
            () -> validator.validateSchema("catalog.schema.table", expectedSchema));

    assertTrue(ex.getMessage().contains("DECIMAL"));
    assertTrue(ex.getMessage().contains("precision/scale too small"));
  }

  @Test
  void testAllSupportedTypes() {
    StructType expectedSchema =
        new StructType()
            .add("col_int", IntegerType.INTEGER, false)
            .add("col_long", LongType.LONG, false)
            .add("col_float", FloatType.FLOAT, false)
            .add("col_double", DoubleType.DOUBLE, false)
            .add("col_boolean", BooleanType.BOOLEAN, false)
            .add("col_string", StringType.STRING, false)
            .add("col_binary", BinaryType.BINARY, false)
            .add("col_date", DateType.DATE, false)
            .add("col_timestamp", TimestampType.TIMESTAMP, false);

    TableInfo tableInfo =
        new TableInfo()
            .setColumns(
                List.of(
                    new ColumnInfo().setName("col_int").setTypeJson("integer").setNullable(false),
                    new ColumnInfo().setName("col_long").setTypeJson("long").setNullable(false),
                    new ColumnInfo().setName("col_float").setTypeJson("float").setNullable(false),
                    new ColumnInfo().setName("col_double").setTypeJson("double").setNullable(false),
                    new ColumnInfo()
                        .setName("col_boolean")
                        .setTypeJson("boolean")
                        .setNullable(false),
                    new ColumnInfo().setName("col_string").setTypeJson("string").setNullable(false),
                    new ColumnInfo().setName("col_binary").setTypeJson("binary").setNullable(false),
                    new ColumnInfo().setName("col_date").setTypeJson("date").setNullable(false),
                    new ColumnInfo()
                        .setName("col_timestamp")
                        .setTypeJson("timestamp")
                        .setNullable(false)));

    when(tablesAPI.get("catalog.schema.table")).thenReturn(tableInfo);

    assertDoesNotThrow(() -> validator.validateSchema("catalog.schema.table", expectedSchema));
  }

  @Test
  void testEmptyTableColumns() {
    StructType expectedSchema = new StructType().add("id", IntegerType.INTEGER, false);

    TableInfo tableInfo = new TableInfo().setColumns(List.of());

    when(tablesAPI.get("catalog.schema.table")).thenReturn(tableInfo);

    SchemaValidationException ex =
        assertThrows(
            SchemaValidationException.class,
            () -> validator.validateSchema("catalog.schema.table", expectedSchema));

    assertTrue(ex.getMessage().contains("no columns"));
  }

  @Test
  void testNullTableColumns() {
    StructType expectedSchema = new StructType().add("id", IntegerType.INTEGER, false);

    TableInfo tableInfo = new TableInfo();

    when(tablesAPI.get("catalog.schema.table")).thenReturn(tableInfo);

    SchemaValidationException ex =
        assertThrows(
            SchemaValidationException.class,
            () -> validator.validateSchema("catalog.schema.table", expectedSchema));

    assertTrue(ex.getMessage().contains("no columns"));
  }

  @Test
  void testNestedArrayType_Compatible() {
    StructType expectedSchema =
        new StructType().add("tags", new ArrayType(StringType.STRING, true), true);

    TableInfo tableInfo =
        new TableInfo()
            .setColumns(
                List.of(
                    new ColumnInfo()
                        .setName("tags")
                        .setTypeJson(
                            "{\"type\":\"array\",\"elementType\":\"string\",\"containsNull\":true}")
                        .setNullable(true)));

    when(tablesAPI.get("catalog.schema.table")).thenReturn(tableInfo);

    assertDoesNotThrow(() -> validator.validateSchema("catalog.schema.table", expectedSchema));
  }

  @Test
  void testNestedArrayType_Incompatible() {
    StructType expectedSchema =
        new StructType().add("tags", new ArrayType(StringType.STRING, true), true);

    TableInfo tableInfo =
        new TableInfo()
            .setColumns(
                List.of(
                    new ColumnInfo()
                        .setName("tags")
                        .setTypeJson(
                            "{\"type\":\"array\",\"elementType\":\"integer\",\"containsNull\":true}")
                        .setNullable(true)));

    when(tablesAPI.get("catalog.schema.table")).thenReturn(tableInfo);

    SchemaValidationException ex =
        assertThrows(
            SchemaValidationException.class,
            () -> validator.validateSchema("catalog.schema.table", expectedSchema));

    assertTrue(ex.getMessage().contains("expected STRING"));
    assertTrue(ex.getMessage().contains("actual INTEGER"));
  }

  @Test
  void testNestedMapType_Compatible() {
    StructType expectedSchema =
        new StructType()
            .add("metadata", new MapType(StringType.STRING, IntegerType.INTEGER, true), true);

    TableInfo tableInfo =
        new TableInfo()
            .setColumns(
                List.of(
                    new ColumnInfo()
                        .setName("metadata")
                        .setTypeJson(
                            "{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"integer\",\"valueContainsNull\":true}")
                        .setNullable(true)));

    when(tablesAPI.get("catalog.schema.table")).thenReturn(tableInfo);

    assertDoesNotThrow(() -> validator.validateSchema("catalog.schema.table", expectedSchema));
  }

  @Test
  void testNestedMapType_Incompatible() {
    StructType expectedSchema =
        new StructType()
            .add("metadata", new MapType(StringType.STRING, StringType.STRING, true), true);

    TableInfo tableInfo =
        new TableInfo()
            .setColumns(
                List.of(
                    new ColumnInfo()
                        .setName("metadata")
                        .setTypeJson(
                            "{\"type\":\"map\",\"keyType\":\"string\",\"valueType\":\"integer\",\"valueContainsNull\":true}")
                        .setNullable(true)));

    when(tablesAPI.get("catalog.schema.table")).thenReturn(tableInfo);

    SchemaValidationException ex =
        assertThrows(
            SchemaValidationException.class,
            () -> validator.validateSchema("catalog.schema.table", expectedSchema));

    assertTrue(ex.getMessage().contains("expected STRING"));
    assertTrue(ex.getMessage().contains("actual INTEGER"));
  }

  @Test
  void testNestedStructType_Compatible() {
    StructType nestedStruct =
        new StructType()
            .add("street", StringType.STRING, true)
            .add("city", StringType.STRING, true);
    StructType expectedSchema = new StructType().add("address", nestedStruct, true);

    TableInfo tableInfo =
        new TableInfo()
            .setColumns(
                List.of(
                    new ColumnInfo()
                        .setName("address")
                        .setTypeJson(
                            "{\"type\":\"struct\",\"fields\":["
                                + "{\"name\":\"street\",\"type\":\"string\",\"nullable\":true},"
                                + "{\"name\":\"city\",\"type\":\"string\",\"nullable\":true}"
                                + "]}")
                        .setNullable(true)));

    when(tablesAPI.get("catalog.schema.table")).thenReturn(tableInfo);

    assertDoesNotThrow(() -> validator.validateSchema("catalog.schema.table", expectedSchema));
  }

  @Test
  void testNestedStructType_MissingField() {
    StructType nestedStruct =
        new StructType()
            .add("street", StringType.STRING, true)
            .add("city", StringType.STRING, true)
            .add("zip", StringType.STRING, true);
    StructType expectedSchema = new StructType().add("address", nestedStruct, true);

    TableInfo tableInfo =
        new TableInfo()
            .setColumns(
                List.of(
                    new ColumnInfo()
                        .setName("address")
                        .setTypeJson(
                            "{\"type\":\"struct\",\"fields\":["
                                + "{\"name\":\"street\",\"type\":\"string\",\"nullable\":true},"
                                + "{\"name\":\"city\",\"type\":\"string\",\"nullable\":true}"
                                + "]}")
                        .setNullable(true)));

    when(tablesAPI.get("catalog.schema.table")).thenReturn(tableInfo);

    SchemaValidationException ex =
        assertThrows(
            SchemaValidationException.class,
            () -> validator.validateSchema("catalog.schema.table", expectedSchema));

    assertTrue(ex.getMessage().contains("Missing field"));
    assertTrue(ex.getMessage().contains("zip"));
  }

  @Test
  void testDeeplyNestedType_ArrayOfStruct() {
    StructType nestedStruct =
        new StructType()
            .add("name", StringType.STRING, true)
            .add("value", IntegerType.INTEGER, true);
    StructType expectedSchema =
        new StructType().add("items", new ArrayType(nestedStruct, true), true);

    TableInfo tableInfo =
        new TableInfo()
            .setColumns(
                List.of(
                    new ColumnInfo()
                        .setName("items")
                        .setTypeJson(
                            "{\"type\":\"array\",\"elementType\":{\"type\":\"struct\",\"fields\":["
                                + "{\"name\":\"name\",\"type\":\"string\",\"nullable\":true},"
                                + "{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true}"
                                + "]},\"containsNull\":true}")
                        .setNullable(true)));

    when(tablesAPI.get("catalog.schema.table")).thenReturn(tableInfo);

    assertDoesNotThrow(() -> validator.validateSchema("catalog.schema.table", expectedSchema));
  }
}
