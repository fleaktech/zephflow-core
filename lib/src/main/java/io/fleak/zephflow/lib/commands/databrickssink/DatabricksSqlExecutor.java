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
import com.databricks.sdk.service.sql.Disposition;
import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import java.util.*;
import java.util.function.LongSupplier;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public record DatabricksSqlExecutor(
    WorkspaceClient workspaceClient, String warehouseId, long maxWaitMs) {

  private static final int SLEEP_TIME_MS = 5000;
  private static final long DEFAULT_MAX_WAIT_MS = 30 * 60 * 1000L;

  public DatabricksSqlExecutor(WorkspaceClient workspaceClient, String warehouseId) {
    this(workspaceClient, warehouseId, DEFAULT_MAX_WAIT_MS);
  }

  public void validateCopyInto(
      String tableName,
      String volumePattern,
      Map<String, String> copyOptions,
      Map<String, String> formatOptions) {
    validateCopyInto(
        tableName,
        volumePattern,
        copyOptions,
        formatOptions,
        System::currentTimeMillis,
        Thread::sleep);
  }

  void validateCopyInto(
      String tableName,
      String volumePattern,
      Map<String, String> copyOptions,
      Map<String, String> formatOptions,
      LongSupplier clock,
      Sleeper sleeper) {
    executeStatement(
        buildCopyIntoSql(tableName, volumePattern, copyOptions, formatOptions, true),
        true,
        clock,
        sleeper);
  }

  public CopyIntoStats executeCopyIntoWithStats(
      String tableName,
      String volumePattern,
      Map<String, String> copyOptions,
      Map<String, String> formatOptions) {
    return executeCopyIntoWithStats(
        tableName,
        volumePattern,
        copyOptions,
        formatOptions,
        System::currentTimeMillis,
        Thread::sleep);
  }

  CopyIntoStats executeCopyIntoWithStats(
      String tableName,
      String volumePattern,
      Map<String, String> copyOptions,
      Map<String, String> formatOptions,
      LongSupplier clock,
      Sleeper sleeper) {
    StatementResponse finalResponse =
        executeStatement(
            buildCopyIntoSql(tableName, volumePattern, copyOptions, formatOptions, false),
            false,
            clock,
            sleeper);
    CopyIntoStats stats = parseCopyIntoStats(finalResponse);
    if (stats.rowsLoadedKnown()) {
      log.info(
          "COPY INTO statement {} succeeded: {} rows loaded",
          finalResponse.getStatementId(),
          stats.rowsLoaded());
    } else {
      log.warn(
          "COPY INTO statement {} succeeded; rows loaded count is unavailable",
          finalResponse.getStatementId());
    }
    return stats;
  }

  private StatementResponse executeStatement(
      String sql, boolean validation, LongSupplier clock, Sleeper sleeper) {
    log.info("Executing COPY INTO SQL:\n{}", sql);
    ExecuteStatementRequest request =
        new ExecuteStatementRequest()
            .setWarehouseId(warehouseId)
            .setStatement(sql)
            .setWaitTimeout("0s");
    if (validation) {
      request.setDisposition(Disposition.EXTERNAL_LINKS);
    }

    StatementResponse initialResponse;
    try {
      initialResponse = workspaceClient.statementExecution().executeStatement(request);
    } catch (RuntimeException e) {
      throw unknown(null, "COPY INTO submission outcome unknown", e);
    }

    String statementId = initialResponse == null ? null : initialResponse.getStatementId();
    if (statementId == null || statementId.isBlank()) {
      throw unknown(null, "COPY INTO response has no statement ID; outcome unknown", null);
    }
    log.info("Submitted COPY INTO statementId: {}. Waiting for completion...", statementId);
    return waitForStatementCompletion(statementId, initialResponse, validation, clock, sleeper);
  }

  private StatementResponse waitForStatementCompletion(
      String statementId,
      StatementResponse initialResponse,
      boolean validation,
      LongSupplier clock,
      Sleeper sleeper) {
    long deadline = clock.getAsLong() + maxWaitMs;
    StatementResponse response = initialResponse;
    while (true) {
      if (isTerminal(statementId, response, validation)) {
        return response;
      }
      if (clock.getAsLong() >= deadline) {
        return resolveTimeout(statementId, validation);
      }
      try {
        response = workspaceClient.statementExecution().getStatement(statementId);
      } catch (RuntimeException e) {
        throw unknown(statementId, "COPY INTO polling failed; delivery outcome unknown", e);
      }
      if (isTerminal(statementId, response, validation)) {
        return response;
      }
      long remaining = deadline - clock.getAsLong();
      if (remaining > 0) {
        try {
          sleeper.sleep(Math.min(SLEEP_TIME_MS, remaining));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw unknown(statementId, "COPY INTO interrupted; delivery outcome unknown", e);
        }
      }
    }
  }

  private boolean isTerminal(String statementId, StatementResponse response, boolean validation) {
    StatementStatus status = response == null ? null : response.getStatus();
    StatementState state = status == null ? null : status.getState();
    if (state == null) {
      throw unknown(
          statementId, "COPY INTO response has no status; delivery outcome unknown", null);
    }
    return switch (state) {
      case SUCCEEDED, CLOSED -> true;
      case FAILED, CANCELED ->
          throw new StatementExecutionException(
              statementId,
              state,
              status.getSqlState(),
              status.getError() == null || status.getError().getErrorCode() == null
                  ? null
                  : status.getError().getErrorCode().toString(),
              status.getError() == null ? null : status.getError().getMessage(),
              !validation,
              null);
      case PENDING, RUNNING -> false;
    };
  }

  private StatementResponse resolveTimeout(String statementId, boolean validation) {
    RuntimeException cancelFailure = null;
    try {
      log.warn("Query timed out locally. Attempting to cancel statement: {}", statementId);
      workspaceClient
          .statementExecution()
          .cancelExecution(new CancelExecutionRequest().setStatementId(statementId));
    } catch (RuntimeException e) {
      cancelFailure = e;
      preserveInterruption(e);
      log.warn("Failed to cancel timed-out statement {}", statementId);
    }

    try {
      StatementResponse response = workspaceClient.statementExecution().getStatement(statementId);
      if (isTerminal(statementId, response, validation)) {
        return response;
      }
    } catch (StatementExecutionException e) {
      if (cancelFailure != null) e.addSuppressed(cancelFailure);
      throw e;
    } catch (RuntimeException e) {
      StatementExecutionException failure =
          unknown(statementId, "COPY INTO timed out and final status is unknown", e);
      if (cancelFailure != null) failure.addSuppressed(cancelFailure);
      throw failure;
    }
    throw unknown(
        statementId,
        "COPY INTO timed out after " + maxWaitMs + "ms; delivery outcome unknown",
        cancelFailure);
  }

  private static StatementExecutionException unknown(
      String statementId, String message, Throwable cause) {
    preserveInterruption(cause);
    return new StatementExecutionException(statementId, null, null, null, message, true, cause);
  }

  private static void preserveInterruption(Throwable cause) {
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    while (cause != null && seen.add(cause)) {
      if (cause instanceof InterruptedException) {
        Thread.currentThread().interrupt();
        return;
      }
      cause = cause.getCause();
    }
  }

  private CopyIntoStats parseCopyIntoStats(StatementResponse response) {
    if (response.getStatus().getState() == StatementState.CLOSED
        || response.getResult() == null
        || response.getResult().getDataArray() == null
        || response.getResult().getDataArray().isEmpty()) {
      return new CopyIntoStats(0, 0, 0, List.of(), false);
    }

    Map<String, Integer> colIndex = new HashMap<>();
    if (response.getManifest() != null && response.getManifest().getSchema() != null) {
      Collection<ColumnInfo> columns = response.getManifest().getSchema().getColumns();
      if (columns != null) {
        int i = 0;
        for (ColumnInfo col : columns) {
          if (col != null) colIndex.put(col.getName(), i);
          i++;
        }
      }
    }

    Collection<String> row = response.getResult().getDataArray().iterator().next();
    if (row != null) {
      List<String> firstRow = new ArrayList<>(row);
      for (String column : List.of("num_inserted_rows", "num_copied_rows", "num_affected_rows")) {
        Long value = extractLongValue(firstRow, colIndex, column);
        if (value != null) {
          return new CopyIntoStats(value, 0, 0, List.of(), true);
        }
      }
    }
    return new CopyIntoStats(0, 0, 0, List.of(), false);
  }

  private Long extractLongValue(List<String> row, Map<String, Integer> colIndex, String colName) {
    Integer idx = colIndex.get(colName);
    if (idx == null || idx >= row.size() || row.get(idx) == null) return null;
    try {
      long value = Long.parseLong(row.get(idx));
      return value < 0 ? null : value;
    } catch (NumberFormatException e) {
      return null;
    }
  }

  @FunctionalInterface
  interface Sleeper {
    void sleep(long millis) throws InterruptedException;
  }

  private String buildCopyIntoSql(
      String tableName,
      String volumePattern,
      Map<String, String> copyOptions,
      Map<String, String> formatOptions,
      boolean validation) {

    StringBuilder sql = new StringBuilder();
    sql.append("COPY INTO ").append(tableName).append("\n");
    sql.append("FROM '").append(volumePattern).append("'\n");
    sql.append("FILEFORMAT = PARQUET\n");
    if (validation) {
      sql.append("VALIDATE ALL\n");
    }

    if (!formatOptions.isEmpty()) {
      sql.append("FORMAT_OPTIONS (\n");
      formatOptions.forEach(
          (k, v) ->
              sql.append("  '")
                  .append(escapeSqlString(k))
                  .append("' = '")
                  .append(escapeSqlString(v))
                  .append("',\n"));
      sql.setLength(sql.length() - 2);
      sql.append("\n)\n");
    }

    if (!copyOptions.isEmpty()) {
      sql.append("COPY_OPTIONS (\n");
      copyOptions.forEach(
          (k, v) ->
              sql.append("  '")
                  .append(escapeSqlString(k))
                  .append("' = '")
                  .append(escapeSqlString(v))
                  .append("',\n"));
      sql.setLength(sql.length() - 2);
      sql.append("\n)");
    } else {
      sql.append("COPY_OPTIONS ('mergeSchema' = 'true')");
    }

    return sql.toString();
  }

  private static String escapeSqlString(String value) {
    return value == null ? "" : value.replace("'", "''");
  }

  public record CopyIntoStats(
      long rowsLoaded,
      int filesProcessed,
      int filesLoaded,
      List<String> errorMessages,
      boolean rowsLoadedKnown) {

    public CopyIntoStats(
        long rowsLoaded, int filesProcessed, int filesLoaded, List<String> errorMessages) {
      this(rowsLoaded, filesProcessed, filesLoaded, errorMessages, true);
    }

    public boolean hasErrors() {
      return !errorMessages.isEmpty();
    }
  }

  public static class StatementExecutionException extends RuntimeException {
    private final String statementId;
    private final StatementState state;
    private final String sqlState;
    private final String errorCode;
    private final String serviceMessage;
    private final boolean outcomeUnknown;

    public StatementExecutionException(
        String statementId,
        StatementState state,
        String sqlState,
        String errorCode,
        String message,
        boolean outcomeUnknown,
        Throwable cause) {
      super(
          outcomeUnknown ? message : "COPY INTO failed with state " + state + ": " + message,
          cause);
      this.statementId = statementId;
      this.state = state;
      this.sqlState = sqlState;
      this.errorCode = errorCode;
      this.serviceMessage = message;
      this.outcomeUnknown = outcomeUnknown;
    }

    public String statementId() {
      return statementId;
    }

    public StatementState state() {
      return state;
    }

    public String sqlState() {
      return sqlState;
    }

    public String errorCode() {
      return errorCode;
    }

    public String serviceMessage() {
      return serviceMessage;
    }

    public boolean outcomeUnknown() {
      return outcomeUnknown;
    }
  }
}
