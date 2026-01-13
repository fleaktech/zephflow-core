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

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.ColumnInfo;
import com.databricks.sdk.service.catalog.TableInfo;
import io.delta.kernel.types.StructType;
import java.util.Collection;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@RequiredArgsConstructor
public class DeltaTableSchemaValidator {

  private final WorkspaceClient workspaceClient;

  public void validateSchema(String tableName, StructType expectedSchema) {
    TableInfo tableInfo = fetchTableInfo(tableName);
    Collection<ColumnInfo> tableColumns = tableInfo.getColumns();

    if (tableColumns == null || tableColumns.isEmpty()) {
      throw new SchemaValidationException(
          "Table '" + tableName + "' has no columns or column metadata is not accessible");
    }

    StructType actualSchema = SparkJsonTypeConverter.buildStructTypeFromColumns(tableColumns);
    List<String> errors = SchemaComparator.compare(expectedSchema, actualSchema);

    if (!errors.isEmpty()) {
      throw new SchemaValidationException(
          "Schema validation failed for table '" + tableName + "': " + errors);
    }

    log.info(
        "Schema validation passed for table '{}' with {} columns", tableName, tableColumns.size());
  }

  private TableInfo fetchTableInfo(String tableName) {
    try {
      return workspaceClient.tables().get(tableName);
    } catch (Exception e) {
      String message = e.getMessage();
      if (message != null
          && (message.contains("NOT_FOUND") || message.contains("does not exist"))) {
        throw new SchemaValidationException(
            "Table '" + tableName + "' does not exist. Please create the table first.");
      }
      throw new SchemaValidationException(
          "Cannot access table metadata for '"
              + tableName
              + "': "
              + message
              + ". Check that the service principal has SELECT permission on the table.",
          e);
    }
  }

  public static class SchemaValidationException extends RuntimeException {
    public SchemaValidationException(String message) {
      super(message);
    }

    public SchemaValidationException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
