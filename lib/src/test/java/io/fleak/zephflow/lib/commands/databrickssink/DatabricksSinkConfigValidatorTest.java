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
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.databrickssink.DatabricksSinkDto.Config;
import io.fleak.zephflow.lib.credentials.DatabricksCredential;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DatabricksSinkConfigValidatorTest {

  private DatabricksSinkConfigValidator validator;
  private JobContext jobContext;
  private DatabricksCredential validCredential;
  private WorkspaceClient workspaceClient;
  private TablesAPI tablesAPI;

  private static final Map<String, Object> VALID_AVRO_SCHEMA =
      Map.of(
          "type", "record",
          "name", "TestRecord",
          "fields",
              List.of(
                  Map.of("name", "id", "type", "int"), Map.of("name", "name", "type", "string")));

  @BeforeEach
  void setUp() {
    jobContext = mock(JobContext.class);
    validCredential =
        DatabricksCredential.builder()
            .host("https://test.databricks.com")
            .clientId("client-id")
            .clientSecret("client-secret")
            .build();

    workspaceClient = mock(WorkspaceClient.class);
    tablesAPI = mock(TablesAPI.class);
    when(workspaceClient.tables()).thenReturn(tablesAPI);

    TableInfo validTableInfo =
        new TableInfo()
            .setColumns(
                List.of(
                    new ColumnInfo().setName("id").setTypeJson("integer").setNullable(false),
                    new ColumnInfo().setName("name").setTypeJson("string").setNullable(true)));
    when(tablesAPI.get(anyString())).thenReturn(validTableInfo);

    validator = new DatabricksSinkConfigValidator(credential -> workspaceClient);
  }

  private Config.ConfigBuilder validConfigBuilder() {
    when(jobContext.getOtherProperties()).thenReturn(Map.of("test-credential", validCredential));
    return Config.builder()
        .databricksCredentialId("test-credential")
        .volumePath("/Volumes/catalog/schema/volume")
        .tableName("catalog.schema.table")
        .warehouseId("warehouse-123")
        .avroSchema(VALID_AVRO_SCHEMA);
  }

  @Test
  void testValidConfig() {
    Config config = validConfigBuilder().build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", jobContext));
  }

  @Test
  void testValidConfigWithTwoPartTableName() {
    Config config = validConfigBuilder().tableName("schema.table").build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", jobContext));
  }

  @Test
  void testValidConfigWithSubpath() {
    Config config =
        validConfigBuilder().volumePath("/Volumes/catalog/schema/volume/subdir").build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", jobContext));
  }

  @Test
  void testNullDatabricksCredentialId() {
    when(jobContext.getOtherProperties()).thenReturn(Map.of());
    Config config =
        Config.builder()
            .databricksCredentialId(null)
            .volumePath("/Volumes/catalog/schema/volume")
            .tableName("catalog.schema.table")
            .warehouseId("warehouse-123")
            .avroSchema(VALID_AVRO_SCHEMA)
            .build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("databricksCredentialId is required"));
  }

  @Test
  void testEmptyDatabricksCredentialId() {
    when(jobContext.getOtherProperties()).thenReturn(Map.of());
    Config config =
        Config.builder()
            .databricksCredentialId("   ")
            .volumePath("/Volumes/catalog/schema/volume")
            .tableName("catalog.schema.table")
            .warehouseId("warehouse-123")
            .avroSchema(VALID_AVRO_SCHEMA)
            .build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("databricksCredentialId is required"));
  }

  @Test
  void testDatabricksCredentialNotFound() {
    when(jobContext.getOtherProperties()).thenReturn(Map.of());
    Config config =
        Config.builder()
            .databricksCredentialId("non-existent")
            .volumePath("/Volumes/catalog/schema/volume")
            .tableName("catalog.schema.table")
            .warehouseId("warehouse-123")
            .avroSchema(VALID_AVRO_SCHEMA)
            .build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("no credential found in jobContext"));
  }

  @Test
  void testNullVolumePath() {
    Config config = validConfigBuilder().volumePath(null).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("volumePath is required"));
  }

  @Test
  void testEmptyVolumePath() {
    Config config = validConfigBuilder().volumePath("").build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("volumePath is required"));
  }

  @Test
  void testInvalidVolumePathMissingVolumesPrefix() {
    Config config = validConfigBuilder().volumePath("/catalog/schema/volume").build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("volumePath must match pattern"));
  }

  @Test
  void testInvalidVolumePathTooFewParts() {
    Config config = validConfigBuilder().volumePath("/Volumes/catalog/schema").build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("volumePath must match pattern"));
  }

  @Test
  void testNullTableName() {
    Config config = validConfigBuilder().tableName(null).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("tableName is required"));
  }

  @Test
  void testEmptyTableName() {
    Config config = validConfigBuilder().tableName("").build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("tableName is required"));
  }

  @Test
  void testInvalidTableNameSinglePart() {
    Config config = validConfigBuilder().tableName("table").build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("tableName must match pattern"));
  }

  @Test
  void testInvalidTableNameTooManyParts() {
    Config config = validConfigBuilder().tableName("a.b.c.d").build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("tableName must match pattern"));
  }

  @Test
  void testNullWarehouseId() {
    Config config = validConfigBuilder().warehouseId(null).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("warehouseId is required"));
  }

  @Test
  void testEmptyWarehouseId() {
    Config config = validConfigBuilder().warehouseId("   ").build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("warehouseId is required"));
  }

  @Test
  void testNullAvroSchema() {
    Config config = validConfigBuilder().avroSchema(null).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("avroSchema is required"));
  }

  @Test
  void testEmptyAvroSchema() {
    Config config = validConfigBuilder().avroSchema(Map.of()).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("avroSchema is required"));
  }

  @Test
  void testInvalidAvroSchema() {
    Config config = validConfigBuilder().avroSchema(Map.of("invalid", "schema")).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("avroSchema is invalid"));
  }

  @Test
  void testZeroBatchSize() {
    Config config = validConfigBuilder().batchSize(0).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("batchSize must be positive"));
  }

  @Test
  void testNegativeBatchSize() {
    Config config = validConfigBuilder().batchSize(-1).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("batchSize must be positive"));
  }

  @Test
  void testExcessiveBatchSize() {
    Config config = validConfigBuilder().batchSize(100001).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("batchSize should not exceed 100,000"));
  }

  @Test
  void testMaxAllowedBatchSize() {
    Config config = validConfigBuilder().batchSize(100000).build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", jobContext));
  }

  @Test
  void testZeroFlushInterval() {
    Config config = validConfigBuilder().flushIntervalMillis(0).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("flushIntervalMillis must be positive"));
  }

  @Test
  void testNegativeFlushInterval() {
    Config config = validConfigBuilder().flushIntervalMillis(-1).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("flushIntervalMillis must be positive"));
  }

  @Test
  void testFlushIntervalTooShort() {
    Config config = validConfigBuilder().flushIntervalMillis(999).build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("flushIntervalMillis should be at least 1000ms"));
  }

  @Test
  void testMinAllowedFlushInterval() {
    Config config = validConfigBuilder().flushIntervalMillis(1000).build();

    assertDoesNotThrow(() -> validator.validateConfig(config, "test-node", jobContext));
  }

  @Test
  void testMultipleErrors() {
    when(jobContext.getOtherProperties()).thenReturn(Map.of());
    Config config =
        Config.builder()
            .databricksCredentialId(null)
            .volumePath(null)
            .tableName(null)
            .warehouseId(null)
            .avroSchema(null)
            .batchSize(0)
            .flushIntervalMillis(0)
            .build();

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    String message = exception.getMessage();
    assertTrue(message.contains("databricksCredentialId is required"));
    assertTrue(message.contains("volumePath is required"));
    assertTrue(message.contains("tableName is required"));
    assertTrue(message.contains("warehouseId is required"));
    assertTrue(message.contains("avroSchema is required"));
    assertTrue(message.contains("batchSize must be positive"));
    assertTrue(message.contains("flushIntervalMillis must be positive"));
  }

  @Test
  void testSchemaValidation_TableNotFound() {
    when(tablesAPI.get("catalog.schema.table"))
        .thenThrow(new RuntimeException("TABLE_NOT_FOUND: does not exist"));

    Config config = validConfigBuilder().build();

    DeltaTableSchemaValidator.SchemaValidationException exception =
        assertThrows(
            DeltaTableSchemaValidator.SchemaValidationException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("does not exist"));
  }

  @Test
  void testSchemaValidation_MissingColumn() {
    TableInfo tableInfoMissingColumn =
        new TableInfo()
            .setColumns(
                List.of(new ColumnInfo().setName("id").setTypeJson("integer").setNullable(false)));
    when(tablesAPI.get("catalog.schema.table")).thenReturn(tableInfoMissingColumn);

    Config config = validConfigBuilder().build();

    DeltaTableSchemaValidator.SchemaValidationException exception =
        assertThrows(
            DeltaTableSchemaValidator.SchemaValidationException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("Missing field"));
    assertTrue(exception.getMessage().contains("name"));
  }

  @Test
  void testSchemaValidation_TypeMismatch() {
    TableInfo tableInfoTypeMismatch =
        new TableInfo()
            .setColumns(
                List.of(
                    new ColumnInfo().setName("id").setTypeJson("string").setNullable(false),
                    new ColumnInfo().setName("name").setTypeJson("string").setNullable(true)));
    when(tablesAPI.get("catalog.schema.table")).thenReturn(tableInfoTypeMismatch);

    Config config = validConfigBuilder().build();

    DeltaTableSchemaValidator.SchemaValidationException exception =
        assertThrows(
            DeltaTableSchemaValidator.SchemaValidationException.class,
            () -> validator.validateConfig(config, "test-node", jobContext));
    assertTrue(exception.getMessage().contains("expected INTEGER"));
    assertTrue(exception.getMessage().contains("actual STRING"));
  }
}
