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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.catalog.ColumnInfo;
import com.databricks.sdk.service.catalog.TableInfo;
import io.delta.kernel.types.*;
import java.util.Collection;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Integration test for DeltaTableSchemaValidator that calls real Databricks API.
 *
 * <p>Required environment variables:
 *
 * <ul>
 *   <li>DATABRICKS_HOST - Databricks workspace URL (e.g., https://xxx.cloud.databricks.com)
 *   <li>DATABRICKS_CLIENT_ID - Service principal client ID
 *   <li>DATABRICKS_CLIENT_SECRET - Service principal client secret
 *   <li>DATABRICKS_TEST_TABLE - Full table name to test (e.g., catalog.schema.table)
 * </ul>
 */
@SuppressWarnings("JavadocLinkAsPlainText")
@Tag("integration")
class DeltaTableSchemaValidatorIntegrationTest {

  private static String testTableName;
  private static WorkspaceClient workspaceClient;

  @BeforeAll
  static void setUp() {
    String databricksHost = System.getenv("DATABRICKS_HOST");
    String clientId = System.getenv("DATABRICKS_CLIENT_ID");
    String clientSecret = System.getenv("DATABRICKS_CLIENT_SECRET");
    testTableName = System.getenv("DATABRICKS_TEST_TABLE");

    boolean hasCredentials =
        databricksHost != null
            && !databricksHost.isBlank()
            && clientId != null
            && !clientId.isBlank()
            && clientSecret != null
            && !clientSecret.isBlank()
            && testTableName != null
            && !testTableName.isBlank();

    assumeTrue(
        hasCredentials,
        "Skipping integration test: DATABRICKS_HOST, DATABRICKS_CLIENT_ID, "
            + "DATABRICKS_CLIENT_SECRET, and DATABRICKS_TEST_TABLE environment variables are required");

    DatabricksConfig config =
        new DatabricksConfig()
            .setHost(databricksHost)
            .setClientId(clientId)
            .setClientSecret(clientSecret);

    workspaceClient = new WorkspaceClient(config);
  }

  @Test
  void testFetchTableInfo() {
    TableInfo tableInfo = workspaceClient.tables().get(testTableName);

    assertNotNull(tableInfo, "TableInfo should not be null");
    assertNotNull(tableInfo.getColumns(), "Columns should not be null");
    assertFalse(tableInfo.getColumns().isEmpty(), "Table should have at least one column");

    System.out.println("Table: " + testTableName);
    System.out.println("Columns:");
    for (ColumnInfo column : tableInfo.getColumns()) {
      System.out.printf(
          "  - %s: typeJson=%s, nullable=%s%n",
          column.getName(), column.getTypeJson(), column.getNullable());
    }
  }

  @Test
  void testBuildStructTypeFromRemoteTable() {
    TableInfo tableInfo = workspaceClient.tables().get(testTableName);
    Collection<ColumnInfo> columns = tableInfo.getColumns();

    assertNotNull(columns);
    assertFalse(columns.isEmpty());

    StructType structType = SparkJsonTypeConverter.buildStructTypeFromColumns(columns);

    assertNotNull(structType);
    assertTrue(structType.length() > 0, "StructType should have at least one field");

    System.out.println("Converted StructType:");
    for (StructField field : structType.fields()) {
      System.out.printf(
          "  - %s: %s (nullable=%s)%n",
          field.getName(), field.getDataType().getClass().getSimpleName(), field.isNullable());
    }
  }

  @Test
  void testValidateSchemaMatchesRemoteTable() {
    TableInfo tableInfo = workspaceClient.tables().get(testTableName);
    Collection<ColumnInfo> columns = tableInfo.getColumns();

    StructType remoteSchema = SparkJsonTypeConverter.buildStructTypeFromColumns(columns);

    DeltaTableSchemaValidator validator = new DeltaTableSchemaValidator(workspaceClient);

    assertDoesNotThrow(
        () -> validator.validateSchema(testTableName, remoteSchema),
        "Schema should validate successfully when expected schema matches remote table");
  }

  @Test
  void testValidateSchemaWithMissingColumn() {
    TableInfo tableInfo = workspaceClient.tables().get(testTableName);
    Collection<ColumnInfo> columns = tableInfo.getColumns();

    StructType remoteSchema = SparkJsonTypeConverter.buildStructTypeFromColumns(columns);

    StructType schemaWithExtraColumn =
        remoteSchema.add("nonexistent_test_column_xyz", StringType.STRING, true);

    DeltaTableSchemaValidator validator = new DeltaTableSchemaValidator(workspaceClient);

    DeltaTableSchemaValidator.SchemaValidationException ex =
        assertThrows(
            DeltaTableSchemaValidator.SchemaValidationException.class,
            () -> validator.validateSchema(testTableName, schemaWithExtraColumn));

    assertTrue(ex.getMessage().contains("Missing field"));
    assertTrue(ex.getMessage().contains("nonexistent_test_column_xyz"));

    System.out.println("Expected validation error: " + ex.getMessage());
  }

  @Test
  void testValidateSchemaWithTypeMismatch() {
    TableInfo tableInfo = workspaceClient.tables().get(testTableName);
    Collection<ColumnInfo> columns = tableInfo.getColumns();
    ColumnInfo firstColumn = columns.iterator().next();

    DataType originalType = SparkJsonTypeConverter.convert(firstColumn.getTypeJson());

    DataType mismatchedType;
    if (originalType instanceof StringType) {
      mismatchedType = IntegerType.INTEGER;
    } else if (originalType instanceof IntegerType || originalType instanceof LongType) {
      mismatchedType = BooleanType.BOOLEAN;
    } else {
      mismatchedType = StringType.STRING;
    }

    StructType mismatchedSchema = new StructType().add(firstColumn.getName(), mismatchedType, true);

    DeltaTableSchemaValidator validator = new DeltaTableSchemaValidator(workspaceClient);

    DeltaTableSchemaValidator.SchemaValidationException ex =
        assertThrows(
            DeltaTableSchemaValidator.SchemaValidationException.class,
            () -> validator.validateSchema(testTableName, mismatchedSchema));

    assertTrue(ex.getMessage().contains("expected"));
    assertTrue(ex.getMessage().contains("actual"));

    System.out.println("Expected type mismatch error: " + ex.getMessage());
  }

  @Test
  void testTableNotFound() {
    DeltaTableSchemaValidator validator = new DeltaTableSchemaValidator(workspaceClient);
    StructType dummySchema = new StructType().add("id", IntegerType.INTEGER, false);

    DeltaTableSchemaValidator.SchemaValidationException ex =
        assertThrows(
            DeltaTableSchemaValidator.SchemaValidationException.class,
            () ->
                validator.validateSchema(
                    "nonexistent_catalog.nonexistent_schema.nonexistent_table", dummySchema));

    assertTrue(
        ex.getMessage().contains("does not exist")
            || ex.getMessage().contains("Cannot access table metadata"));

    System.out.println("Expected table not found error: " + ex.getMessage());
  }
}
