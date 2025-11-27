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
import com.databricks.sdk.service.sql.CancelExecutionRequest;
import com.databricks.sdk.service.sql.ColumnInfo;
import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import java.util.*;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public record DatabricksSqlExecutor(
    WorkspaceClient workspaceClient, String warehouseId, long maxWaitMs) {

  private static final int SLEEP_TIME_MS = 5000;
  private static final long DEFAULT_MAX_WAIT_MS = 30 * 60 * 1000L; // 30 minutes

  public DatabricksSqlExecutor(WorkspaceClient workspaceClient, String warehouseId) {
    this(workspaceClient, warehouseId, DEFAULT_MAX_WAIT_MS);
  }

  public CopyIntoStats executeCopyIntoWithStats(
      String tableName,
      String volumePattern,
      Map<String, String> copyOptions,
      Map<String, String> formatOptions) {

    String sql = buildCopyIntoSql(tableName, volumePattern, copyOptions, formatOptions);
    log.info("Executing COPY INTO SQL:\n{}", sql);

    // 1. Submit the query Asynchronously (WaitTimeout = 0s)
    // This returns immediately with a statement ID.
    StatementResponse initialResponse =
        workspaceClient
            .statementExecution()
            .executeStatement(
                new ExecuteStatementRequest()
                    .setWarehouseId(warehouseId)
                    .setStatement(sql)
                    .setWaitTimeout("0s")); // 0s means "don't wait, just give me the ID"

    String statementId = initialResponse.getStatementId();
    log.info("Submitted COPY INTO statementId: {}. Waiting for completion...", statementId);

    // 2. Poll for completion
    StatementResponse finalResponse = waitForStatementCompletion(statementId);

    // 3. Parse and return
    CopyIntoStats stats = parseCopyIntoStats(finalResponse);
    log.info("COPY INTO succeeded: {} rows loaded", stats.rowsLoaded());
    return stats;
  }

  private StatementResponse waitForStatementCompletion(String statementId) {
    long deadline = System.currentTimeMillis() + maxWaitMs;

    while (System.currentTimeMillis() < deadline) {
      StatementResponse response = workspaceClient.statementExecution().getStatement(statementId);
      StatementState state = response.getStatus().getState();

      switch (state) {
        case SUCCEEDED:
          return response;

        case FAILED:
        case CANCELED:
        case CLOSED:
          String errorMsg = "Unknown error";
          if (response.getStatus().getError() != null) {
            errorMsg = response.getStatus().getError().getMessage();
          }
          throw new RuntimeException("COPY INTO failed with state " + state + ": " + errorMsg);

        case RUNNING:
        case PENDING:
        default:
          try {
            //noinspection BusyWait
            Thread.sleep(SLEEP_TIME_MS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("COPY INTO interrupted while waiting for completion", e);
          }
          break;
      }
    }
    try {
      log.warn("Query timed out locally. Attempting to cancel statement: {}", statementId);
      workspaceClient
          .statementExecution()
          .cancelExecution(new CancelExecutionRequest().setStatementId(statementId));
    } catch (Exception e) {
      log.warn("Failed to cancel timed-out statement {}: {}", statementId, e.getMessage());
    }
    throw new RuntimeException("COPY INTO timed out after " + maxWaitMs + "ms");
  }

  private CopyIntoStats parseCopyIntoStats(StatementResponse response) {
    long rowsLoaded = 0;
    int filesProcessed = 0;
    int filesLoaded = 0;
    List<String> errorMessages = new ArrayList<>();

    if (response.getResult() == null || response.getResult().getDataArray() == null) {
      log.warn("COPY INTO returned no result data");
      return new CopyIntoStats(rowsLoaded, filesProcessed, filesLoaded, errorMessages);
    }

    // Build column index mapping from manifest schema
    Map<String, Integer> colIndex = new HashMap<>();
    if (response.getManifest() != null && response.getManifest().getSchema() != null) {
      Collection<ColumnInfo> columns = response.getManifest().getSchema().getColumns();
      if (columns != null) {
        int i = 0;
        for (ColumnInfo col : columns) {
          colIndex.put(col.getName(), i++);
        }
      }
    }

    // Parse the first row of data (SDK returns Collection<Collection<String>>)
    if (!response.getResult().getDataArray().isEmpty()) {
      List<String> firstRow =
          new ArrayList<>(response.getResult().getDataArray().iterator().next());

      // Priority 1: num_inserted_rows (specific COPY INTO metric)
      rowsLoaded = extractLongValue(firstRow, colIndex, "num_inserted_rows");

      // Priority 2: num_copied_rows (legacy/specific versions)
      if (rowsLoaded == 0) {
        rowsLoaded = extractLongValue(firstRow, colIndex, "num_copied_rows");
      }

      // Priority 3: num_affected_rows (generic fallback)
      if (rowsLoaded == 0) {
        rowsLoaded = extractLongValue(firstRow, colIndex, "num_affected_rows");
      }
    }

    return new CopyIntoStats(rowsLoaded, filesProcessed, filesLoaded, errorMessages);
  }

  private long extractLongValue(List<String> row, Map<String, Integer> colIndex, String colName) {
    if (!colIndex.containsKey(colName)) return 0;
    int idx = colIndex.get(colName);
    if (idx >= row.size() || row.get(idx) == null) return 0;

    try {
      return Long.parseLong(row.get(idx));
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  private String buildCopyIntoSql(
      String tableName,
      String volumePattern,
      Map<String, String> copyOptions,
      Map<String, String> formatOptions) {

    StringBuilder sql = new StringBuilder();
    sql.append("COPY INTO ").append(tableName).append("\n");
    sql.append("FROM '").append(volumePattern).append("'\n");
    sql.append("FILEFORMAT = PARQUET\n");

    if (!formatOptions.isEmpty()) {
      sql.append("FORMAT_OPTIONS (\n");
      formatOptions.forEach(
          (k, v) -> sql.append("  '").append(k).append("' = '").append(v).append("',\n"));
      sql.setLength(sql.length() - 2);
      sql.append("\n)\n");
    }

    if (!copyOptions.isEmpty()) {
      sql.append("COPY_OPTIONS (\n");
      copyOptions.forEach(
          (k, v) -> sql.append("  '").append(k).append("' = '").append(v).append("',\n"));
      sql.setLength(sql.length() - 2);
      sql.append("\n)");
    } else {
      sql.append("COPY_OPTIONS ('mergeSchema' = 'true')");
    }

    return sql.toString();
  }

  public record CopyIntoStats(
      long rowsLoaded, int filesProcessed, int filesLoaded, List<String> errorMessages) {

    public boolean hasErrors() {
      return !errorMessages.isEmpty();
    }
  }
}
