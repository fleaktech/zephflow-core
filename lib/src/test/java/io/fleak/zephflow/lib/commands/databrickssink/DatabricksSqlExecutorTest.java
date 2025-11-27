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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.sql.*;
import io.fleak.zephflow.lib.commands.databrickssink.DatabricksSqlExecutor.CopyIntoStats;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class DatabricksSqlExecutorTest {

  private WorkspaceClient workspaceClient;
  private StatementExecutionAPI statementExecutionAPI;
  private DatabricksSqlExecutor executor;

  private static final String WAREHOUSE_ID = "test-warehouse-id";
  private static final String TABLE_NAME = "catalog.schema.test_table";
  private static final String VOLUME_PATTERN = "/Volumes/catalog/schema/volume/*.parquet";

  @BeforeEach
  void setUp() {
    workspaceClient = mock(WorkspaceClient.class);
    statementExecutionAPI = mock(StatementExecutionAPI.class);
    when(workspaceClient.statementExecution()).thenReturn(statementExecutionAPI);

    executor = new DatabricksSqlExecutor(workspaceClient, WAREHOUSE_ID, 100);
  }

  @Test
  void testExecuteCopyIntoWithStatsSuccess() {
    StatementResponse initialResponse = createInitialResponse("stmt-123");
    StatementResponse finalResponse = createSuccessResponse(100L);

    when(statementExecutionAPI.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(initialResponse);
    when(statementExecutionAPI.getStatement("stmt-123")).thenReturn(finalResponse);

    CopyIntoStats stats =
        executor.executeCopyIntoWithStats(TABLE_NAME, VOLUME_PATTERN, Map.of(), Map.of());

    assertEquals(100L, stats.rowsLoaded());
    assertFalse(stats.hasErrors());

    ArgumentCaptor<ExecuteStatementRequest> captor =
        ArgumentCaptor.forClass(ExecuteStatementRequest.class);
    verify(statementExecutionAPI).executeStatement(captor.capture());

    ExecuteStatementRequest request = captor.getValue();
    assertEquals(WAREHOUSE_ID, request.getWarehouseId());
    assertTrue(request.getStatement().contains("COPY INTO " + TABLE_NAME));
    assertTrue(request.getStatement().contains(VOLUME_PATTERN));
    assertEquals("0s", request.getWaitTimeout());
  }

  @Test
  void testExecuteCopyIntoWithFormatOptions() {
    StatementResponse initialResponse = createInitialResponse("stmt-123");
    StatementResponse finalResponse = createSuccessResponse(50L);

    when(statementExecutionAPI.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(initialResponse);
    when(statementExecutionAPI.getStatement("stmt-123")).thenReturn(finalResponse);

    Map<String, String> formatOptions = Map.of("mergeSchema", "true", "ignoreCorruptFiles", "true");

    CopyIntoStats stats =
        executor.executeCopyIntoWithStats(TABLE_NAME, VOLUME_PATTERN, Map.of(), formatOptions);

    assertEquals(50L, stats.rowsLoaded());

    ArgumentCaptor<ExecuteStatementRequest> captor =
        ArgumentCaptor.forClass(ExecuteStatementRequest.class);
    verify(statementExecutionAPI).executeStatement(captor.capture());

    String sql = captor.getValue().getStatement();
    assertTrue(sql.contains("FORMAT_OPTIONS"));
    assertTrue(sql.contains("'mergeSchema' = 'true'"));
    assertTrue(sql.contains("'ignoreCorruptFiles' = 'true'"));
  }

  @Test
  void testExecuteCopyIntoWithCopyOptions() {
    StatementResponse initialResponse = createInitialResponse("stmt-123");
    StatementResponse finalResponse = createSuccessResponse(75L);

    when(statementExecutionAPI.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(initialResponse);
    when(statementExecutionAPI.getStatement("stmt-123")).thenReturn(finalResponse);

    Map<String, String> copyOptions = Map.of("force", "true");

    CopyIntoStats stats =
        executor.executeCopyIntoWithStats(TABLE_NAME, VOLUME_PATTERN, copyOptions, Map.of());

    assertEquals(75L, stats.rowsLoaded());

    ArgumentCaptor<ExecuteStatementRequest> captor =
        ArgumentCaptor.forClass(ExecuteStatementRequest.class);
    verify(statementExecutionAPI).executeStatement(captor.capture());

    String sql = captor.getValue().getStatement();
    assertTrue(sql.contains("COPY_OPTIONS"));
    assertTrue(sql.contains("'force' = 'true'"));
  }

  @Test
  void testExecuteCopyIntoDefaultCopyOptions() {
    StatementResponse initialResponse = createInitialResponse("stmt-123");
    StatementResponse finalResponse = createSuccessResponse(10L);

    when(statementExecutionAPI.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(initialResponse);
    when(statementExecutionAPI.getStatement("stmt-123")).thenReturn(finalResponse);

    executor.executeCopyIntoWithStats(TABLE_NAME, VOLUME_PATTERN, Map.of(), Map.of());

    ArgumentCaptor<ExecuteStatementRequest> captor =
        ArgumentCaptor.forClass(ExecuteStatementRequest.class);
    verify(statementExecutionAPI).executeStatement(captor.capture());

    String sql = captor.getValue().getStatement();
    assertTrue(sql.contains("COPY_OPTIONS ('mergeSchema' = 'true')"));
  }

  @Test
  void testExecuteCopyIntoStateFailed() {
    StatementResponse initialResponse = createInitialResponse("stmt-123");
    StatementResponse failedResponse = createFailedResponse("Table not found");

    when(statementExecutionAPI.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(initialResponse);
    when(statementExecutionAPI.getStatement("stmt-123")).thenReturn(failedResponse);

    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                executor.executeCopyIntoWithStats(TABLE_NAME, VOLUME_PATTERN, Map.of(), Map.of()));

    assertTrue(exception.getMessage().contains("COPY INTO failed"));
    assertTrue(exception.getMessage().contains("FAILED"));
    assertTrue(exception.getMessage().contains("Table not found"));
  }

  @Test
  void testExecuteCopyIntoStateCanceled() {
    StatementResponse initialResponse = createInitialResponse("stmt-123");
    StatementResponse canceledResponse = createCanceledResponse();

    when(statementExecutionAPI.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(initialResponse);
    when(statementExecutionAPI.getStatement("stmt-123")).thenReturn(canceledResponse);

    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                executor.executeCopyIntoWithStats(TABLE_NAME, VOLUME_PATTERN, Map.of(), Map.of()));

    assertTrue(exception.getMessage().contains("COPY INTO failed"));
    assertTrue(exception.getMessage().contains("CANCELED"));
  }

  @Test
  void testExecuteCopyIntoStateClosed() {
    StatementResponse initialResponse = createInitialResponse("stmt-123");
    StatementResponse closedResponse = createClosedResponse();

    when(statementExecutionAPI.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(initialResponse);
    when(statementExecutionAPI.getStatement("stmt-123")).thenReturn(closedResponse);

    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                executor.executeCopyIntoWithStats(TABLE_NAME, VOLUME_PATTERN, Map.of(), Map.of()));

    assertTrue(exception.getMessage().contains("COPY INTO failed"));
    assertTrue(exception.getMessage().contains("CLOSED"));
  }

  @Test
  void testExecuteCopyIntoPollsUntilSuccess() {
    StatementResponse initialResponse = createInitialResponse("stmt-123");
    StatementResponse pendingResponse = createPendingResponse();
    StatementResponse runningResponse = createRunningResponse();
    StatementResponse successResponse = createSuccessResponse(200L);

    when(statementExecutionAPI.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(initialResponse);
    when(statementExecutionAPI.getStatement("stmt-123"))
        .thenReturn(pendingResponse)
        .thenReturn(runningResponse)
        .thenReturn(successResponse);

    DatabricksSqlExecutor shortWaitExecutor =
        new DatabricksSqlExecutor(workspaceClient, WAREHOUSE_ID, 30000);

    CopyIntoStats stats =
        shortWaitExecutor.executeCopyIntoWithStats(TABLE_NAME, VOLUME_PATTERN, Map.of(), Map.of());

    assertEquals(200L, stats.rowsLoaded());
    verify(statementExecutionAPI, times(3)).getStatement("stmt-123");
  }

  @Test
  void testExecuteCopyIntoTimeout() {
    StatementResponse initialResponse = createInitialResponse("stmt-123");
    StatementResponse pendingResponse = createPendingResponse();

    when(statementExecutionAPI.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(initialResponse);
    when(statementExecutionAPI.getStatement("stmt-123")).thenReturn(pendingResponse);

    DatabricksSqlExecutor shortTimeoutExecutor =
        new DatabricksSqlExecutor(workspaceClient, WAREHOUSE_ID, 1);

    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                shortTimeoutExecutor.executeCopyIntoWithStats(
                    TABLE_NAME, VOLUME_PATTERN, Map.of(), Map.of()));

    assertTrue(exception.getMessage().contains("timed out"));
    verify(statementExecutionAPI).cancelExecution(any(CancelExecutionRequest.class));
  }

  @Test
  void testExecuteCopyIntoTimeoutCancelFails() {
    StatementResponse initialResponse = createInitialResponse("stmt-123");
    StatementResponse pendingResponse = createPendingResponse();

    when(statementExecutionAPI.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(initialResponse);
    when(statementExecutionAPI.getStatement("stmt-123")).thenReturn(pendingResponse);
    doThrow(new RuntimeException("Cancel failed"))
        .when(statementExecutionAPI)
        .cancelExecution(any(CancelExecutionRequest.class));

    DatabricksSqlExecutor shortTimeoutExecutor =
        new DatabricksSqlExecutor(workspaceClient, WAREHOUSE_ID, 1);

    RuntimeException exception =
        assertThrows(
            RuntimeException.class,
            () ->
                shortTimeoutExecutor.executeCopyIntoWithStats(
                    TABLE_NAME, VOLUME_PATTERN, Map.of(), Map.of()));

    assertTrue(exception.getMessage().contains("timed out"));
  }

  @Test
  void testParseStatsWithNumInsertedRows() {
    StatementResponse initialResponse = createInitialResponse("stmt-123");
    StatementResponse successResponse = createSuccessResponseWithColumn("num_inserted_rows", "150");

    when(statementExecutionAPI.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(initialResponse);
    when(statementExecutionAPI.getStatement("stmt-123")).thenReturn(successResponse);

    CopyIntoStats stats =
        executor.executeCopyIntoWithStats(TABLE_NAME, VOLUME_PATTERN, Map.of(), Map.of());

    assertEquals(150L, stats.rowsLoaded());
  }

  @Test
  void testParseStatsWithNumCopiedRows() {
    StatementResponse initialResponse = createInitialResponse("stmt-123");
    StatementResponse successResponse = createSuccessResponseWithColumn("num_copied_rows", "250");

    when(statementExecutionAPI.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(initialResponse);
    when(statementExecutionAPI.getStatement("stmt-123")).thenReturn(successResponse);

    CopyIntoStats stats =
        executor.executeCopyIntoWithStats(TABLE_NAME, VOLUME_PATTERN, Map.of(), Map.of());

    assertEquals(250L, stats.rowsLoaded());
  }

  @Test
  void testParseStatsWithNumAffectedRows() {
    StatementResponse initialResponse = createInitialResponse("stmt-123");
    StatementResponse successResponse = createSuccessResponseWithColumn("num_affected_rows", "350");

    when(statementExecutionAPI.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(initialResponse);
    when(statementExecutionAPI.getStatement("stmt-123")).thenReturn(successResponse);

    CopyIntoStats stats =
        executor.executeCopyIntoWithStats(TABLE_NAME, VOLUME_PATTERN, Map.of(), Map.of());

    assertEquals(350L, stats.rowsLoaded());
  }

  @Test
  void testParseStatsNoResultData() {
    StatementResponse initialResponse = createInitialResponse("stmt-123");
    StatementResponse successResponse = createSuccessResponseNoData();

    when(statementExecutionAPI.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(initialResponse);
    when(statementExecutionAPI.getStatement("stmt-123")).thenReturn(successResponse);

    CopyIntoStats stats =
        executor.executeCopyIntoWithStats(TABLE_NAME, VOLUME_PATTERN, Map.of(), Map.of());

    assertEquals(0L, stats.rowsLoaded());
    assertFalse(stats.hasErrors());
  }

  @Test
  void testParseStatsInvalidNumberFormat() {
    StatementResponse initialResponse = createInitialResponse("stmt-123");
    StatementResponse successResponse =
        createSuccessResponseWithColumn("num_inserted_rows", "not-a-number");

    when(statementExecutionAPI.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(initialResponse);
    when(statementExecutionAPI.getStatement("stmt-123")).thenReturn(successResponse);

    CopyIntoStats stats =
        executor.executeCopyIntoWithStats(TABLE_NAME, VOLUME_PATTERN, Map.of(), Map.of());

    assertEquals(0L, stats.rowsLoaded());
  }

  @Test
  void testParseStatsNullValue() {
    StatementResponse initialResponse = createInitialResponse("stmt-123");
    StatementResponse successResponse = createSuccessResponseWithNullValue();

    when(statementExecutionAPI.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(initialResponse);
    when(statementExecutionAPI.getStatement("stmt-123")).thenReturn(successResponse);

    CopyIntoStats stats =
        executor.executeCopyIntoWithStats(TABLE_NAME, VOLUME_PATTERN, Map.of(), Map.of());

    assertEquals(0L, stats.rowsLoaded());
  }

  @Test
  void testDefaultConstructor() {
    DatabricksSqlExecutor defaultExecutor =
        new DatabricksSqlExecutor(workspaceClient, WAREHOUSE_ID);

    assertEquals(30 * 60 * 1000L, defaultExecutor.maxWaitMs());
    assertEquals(WAREHOUSE_ID, defaultExecutor.warehouseId());
  }

  @Test
  void testCopyIntoStatsHasErrors() {
    CopyIntoStats withErrors = new CopyIntoStats(100L, 5, 5, List.of("Error 1", "Error 2"));
    CopyIntoStats withoutErrors = new CopyIntoStats(100L, 5, 5, List.of());

    assertTrue(withErrors.hasErrors());
    assertFalse(withoutErrors.hasErrors());
  }

  private StatementResponse createInitialResponse(String statementId) {
    StatementResponse response = mock(StatementResponse.class);
    when(response.getStatementId()).thenReturn(statementId);
    return response;
  }

  private StatementResponse createSuccessResponse(long rowsLoaded) {
    return createSuccessResponseWithColumn("num_inserted_rows", String.valueOf(rowsLoaded));
  }

  private StatementResponse createSuccessResponseWithColumn(String columnName, String value) {
    StatementResponse response = mock(StatementResponse.class);
    StatementStatus status = mock(StatementStatus.class);
    when(status.getState()).thenReturn(StatementState.SUCCEEDED);
    when(response.getStatus()).thenReturn(status);

    ResultData resultData = mock(ResultData.class);
    @SuppressWarnings("unchecked")
    Collection<Collection<String>> dataArray = mock(Collection.class);
    Collection<String> row = List.of(value);
    when(dataArray.isEmpty()).thenReturn(false);
    when(dataArray.iterator()).thenReturn(List.of(row).iterator());
    when(resultData.getDataArray()).thenReturn(dataArray);
    when(response.getResult()).thenReturn(resultData);

    ResultManifest manifest = mock(ResultManifest.class);
    ResultSchema schema = mock(ResultSchema.class);
    ColumnInfo columnInfo = mock(ColumnInfo.class);
    when(columnInfo.getName()).thenReturn(columnName);
    when(schema.getColumns()).thenReturn(List.of(columnInfo));
    when(manifest.getSchema()).thenReturn(schema);
    when(response.getManifest()).thenReturn(manifest);

    return response;
  }

  private StatementResponse createSuccessResponseNoData() {
    StatementResponse response = mock(StatementResponse.class);
    StatementStatus status = mock(StatementStatus.class);
    when(status.getState()).thenReturn(StatementState.SUCCEEDED);
    when(response.getStatus()).thenReturn(status);
    when(response.getResult()).thenReturn(null);
    return response;
  }

  private StatementResponse createSuccessResponseWithNullValue() {
    StatementResponse response = mock(StatementResponse.class);
    StatementStatus status = mock(StatementStatus.class);
    when(status.getState()).thenReturn(StatementState.SUCCEEDED);
    when(response.getStatus()).thenReturn(status);

    ResultData resultData = mock(ResultData.class);
    @SuppressWarnings("unchecked")
    Collection<Collection<String>> dataArray = mock(Collection.class);
    List<String> row = new java.util.ArrayList<>();
    row.add(null);
    Collection<String> rowAsCollection = row;
    when(dataArray.isEmpty()).thenReturn(false);
    doReturn(List.<Collection<String>>of(rowAsCollection).iterator()).when(dataArray).iterator();
    when(resultData.getDataArray()).thenReturn(dataArray);
    when(response.getResult()).thenReturn(resultData);

    ResultManifest manifest = mock(ResultManifest.class);
    ResultSchema schema = mock(ResultSchema.class);
    ColumnInfo columnInfo = mock(ColumnInfo.class);
    when(columnInfo.getName()).thenReturn("num_inserted_rows");
    when(schema.getColumns()).thenReturn(List.of(columnInfo));
    when(manifest.getSchema()).thenReturn(schema);
    when(response.getManifest()).thenReturn(manifest);

    return response;
  }

  private StatementResponse createFailedResponse(String errorMessage) {
    StatementResponse response = mock(StatementResponse.class);
    StatementStatus status = mock(StatementStatus.class);
    when(status.getState()).thenReturn(StatementState.FAILED);
    ServiceError error = mock(ServiceError.class);
    when(error.getMessage()).thenReturn(errorMessage);
    when(status.getError()).thenReturn(error);
    when(response.getStatus()).thenReturn(status);
    return response;
  }

  private StatementResponse createCanceledResponse() {
    StatementResponse response = mock(StatementResponse.class);
    StatementStatus status = mock(StatementStatus.class);
    when(status.getState()).thenReturn(StatementState.CANCELED);
    when(status.getError()).thenReturn(null);
    when(response.getStatus()).thenReturn(status);
    return response;
  }

  private StatementResponse createClosedResponse() {
    StatementResponse response = mock(StatementResponse.class);
    StatementStatus status = mock(StatementStatus.class);
    when(status.getState()).thenReturn(StatementState.CLOSED);
    when(status.getError()).thenReturn(null);
    when(response.getStatus()).thenReturn(status);
    return response;
  }

  private StatementResponse createPendingResponse() {
    StatementResponse response = mock(StatementResponse.class);
    StatementStatus status = mock(StatementStatus.class);
    when(status.getState()).thenReturn(StatementState.PENDING);
    when(response.getStatus()).thenReturn(status);
    return response;
  }

  private StatementResponse createRunningResponse() {
    StatementResponse response = mock(StatementResponse.class);
    StatementStatus status = mock(StatementStatus.class);
    when(status.getState()).thenReturn(StatementState.RUNNING);
    when(response.getStatus()).thenReturn(status);
    return response;
  }
}
