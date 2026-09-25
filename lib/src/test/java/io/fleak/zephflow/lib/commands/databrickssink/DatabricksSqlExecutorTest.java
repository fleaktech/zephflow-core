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
import io.fleak.zephflow.lib.commands.databrickssink.DatabricksSqlExecutor.StatementExecutionException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

class DatabricksSqlExecutorTest {
  private WorkspaceClient workspaceClient;
  private StatementExecutionAPI statementExecutionAPI;
  private DatabricksSqlExecutor executor;

  private static final String WAREHOUSE_ID = "test-warehouse-id";
  private static final String TABLE_NAME = "catalog.schema.test_table";
  private static final String VOLUME_PATTERN = "/Volumes/catalog/schema/volume/*.parquet";
  private static final String STATEMENT_ID = "stmt-123";

  @BeforeEach
  void setUp() {
    workspaceClient = mock(WorkspaceClient.class);
    statementExecutionAPI = mock(StatementExecutionAPI.class);
    when(workspaceClient.statementExecution()).thenReturn(statementExecutionAPI);
    executor = new DatabricksSqlExecutor(workspaceClient, WAREHOUSE_ID, 100);
  }

  @Test
  void executesCopyAndPreservesDefaultOptions() {
    submit(response(StatementState.PENDING));
    when(statementExecutionAPI.getStatement(STATEMENT_ID))
        .thenReturn(success("num_inserted_rows", "100"));

    CopyIntoStats stats = execute();
    assertEquals(100, stats.rowsLoaded());
    assertTrue(stats.rowsLoadedKnown());
    assertFalse(stats.hasErrors());
    ExecuteStatementRequest request = submittedRequest();
    assertEquals(WAREHOUSE_ID, request.getWarehouseId());
    assertEquals("0s", request.getWaitTimeout());
    assertTrue(request.getStatement().contains("COPY INTO " + TABLE_NAME));
    assertTrue(request.getStatement().contains(VOLUME_PATTERN));
    assertTrue(request.getStatement().contains("COPY_OPTIONS ('mergeSchema' = 'true')"));
  }

  @Test
  void preservesExplicitCopyAndFormatOptionsAndEscaping() {
    submit(success("num_inserted_rows", "50"));
    CopyIntoStats stats =
        executor.executeCopyIntoWithStats(
            TABLE_NAME,
            VOLUME_PATTERN,
            Map.of("force", "true"),
            Map.of("mergeSchema", "true", "key'quote", "value'quote"));
    assertEquals(50, stats.rowsLoaded());
    String sql = submittedRequest().getStatement();
    assertTrue(sql.contains("FORMAT_OPTIONS"));
    assertTrue(sql.contains("'mergeSchema' = 'true'"));
    assertTrue(sql.contains("'key''quote' = 'value''quote'"));
    assertTrue(sql.contains("'force' = 'true'"));
    verify(statementExecutionAPI, never()).getStatement(anyString());
  }

  @Test
  void validatesAllRecordsWithExternalPreviewAndUnchangedDefaultOptions() {
    submit(response(StatementState.SUCCEEDED));

    assertDoesNotThrow(this::validate);

    ExecuteStatementRequest request = submittedRequest();
    assertEquals(
        "COPY INTO "
            + TABLE_NAME
            + "\n"
            + "FROM '"
            + VOLUME_PATTERN
            + "'\n"
            + "FILEFORMAT = PARQUET\n"
            + "VALIDATE ALL\n"
            + "COPY_OPTIONS ('mergeSchema' = 'true')",
        request.getStatement());
    assertEquals(WAREHOUSE_ID, request.getWarehouseId());
    assertEquals("0s", request.getWaitTimeout());
    assertEquals(Disposition.EXTERNAL_LINKS, request.getDisposition());
    assertNull(request.getByteLimit());
    assertNull(request.getRowLimit());
    verifyNoMoreInteractions(statementExecutionAPI);
  }

  @Test
  void validationPreservesExplicitOptionsAndDoesNotAffectMutatingCopyDisposition() {
    submit(success("num_inserted_rows", "7"));
    Map<String, String> copyOptions = new LinkedHashMap<>();
    copyOptions.put("force", "true");
    copyOptions.put("mergeSchema", "false");
    Map<String, String> formatOptions = new LinkedHashMap<>();
    formatOptions.put("key'quote", "value'quote");

    executor.validateCopyInto(TABLE_NAME, VOLUME_PATTERN, copyOptions, formatOptions);
    assertEquals(
        7,
        executor
            .executeCopyIntoWithStats(TABLE_NAME, VOLUME_PATTERN, copyOptions, formatOptions)
            .rowsLoaded());

    ArgumentCaptor<ExecuteStatementRequest> requests =
        ArgumentCaptor.forClass(ExecuteStatementRequest.class);
    verify(statementExecutionAPI, times(2)).executeStatement(requests.capture());
    ExecuteStatementRequest validation = requests.getAllValues().get(0);
    ExecuteStatementRequest copy = requests.getAllValues().get(1);
    assertEquals(
        "COPY INTO "
            + TABLE_NAME
            + "\n"
            + "FROM '"
            + VOLUME_PATTERN
            + "'\n"
            + "FILEFORMAT = PARQUET\n"
            + "VALIDATE ALL\n"
            + "FORMAT_OPTIONS (\n  'key''quote' = 'value''quote'\n)\n"
            + "COPY_OPTIONS (\n  'force' = 'true',\n  'mergeSchema' = 'false'\n)",
        validation.getStatement());
    assertEquals(validation.getStatement().replace("VALIDATE ALL\n", ""), copy.getStatement());
    assertEquals(Disposition.EXTERNAL_LINKS, validation.getDisposition());
    assertNull(validation.getByteLimit());
    assertNull(validation.getRowLimit());
    assertNull(copy.getDisposition());
    assertNull(copy.getByteLimit());
    assertNull(copy.getRowLimit());
    verifyNoMoreInteractions(statementExecutionAPI);
  }

  @Test
  void successfulValidationIgnoresPreviewColumnNamesValuesTruncationAndLinks() {
    StatementResponse preview =
        response(StatementState.SUCCEEDED)
            .setManifest(
                new ResultManifest()
                    .setTruncated(true)
                    .setSchema(
                        new ResultSchema()
                            .setColumns(
                                List.of(
                                    new ColumnInfo().setName("error"),
                                    new ColumnInfo().setName("status"),
                                    new ColumnInfo().setName("num_inserted_rows")))))
            .setResult(
                new ResultData()
                    .setDataArray(List.of(List.of("user data", "FAILED", "not-a-count")))
                    .setNextChunkIndex(1L)
                    .setNextChunkInternalLink("/preview/next")
                    .setExternalLinks(
                        List.of(
                            new ExternalLink()
                                .setExternalLink("https://preview.invalid/ignored")
                                .setNextChunkInternalLink("/external/next"))));
    submit(preview);

    assertDoesNotThrow(this::validate);

    verify(statementExecutionAPI).executeStatement(any(ExecuteStatementRequest.class));
    verifyNoMoreInteractions(statementExecutionAPI);
    verify(workspaceClient).statementExecution();
    verifyNoMoreInteractions(workspaceClient);
  }

  @Test
  void closedValidationIsSuccessfulWithoutPreview() {
    submit(response(StatementState.CLOSED));
    assertDoesNotThrow(this::validate);
    verify(statementExecutionAPI).executeStatement(any(ExecuteStatementRequest.class));
    verifyNoMoreInteractions(statementExecutionAPI);
  }

  @Test
  void validationPollingAndTimeoutReconciliationPreserveItsReadOnlyFailureStatus() {
    prepareTimeout(failure("22023", "[INVALID_PARAMETER_VALUE] CHECK expression failed"));
    StatementExecutionException error =
        assertThrows(StatementExecutionException.class, this::validateWithClock);
    assertFalse(error.outcomeUnknown());
    assertEquals(StatementState.FAILED, error.state());
    assertEquals("22023", error.sqlState());
    verifyCancelThenFinalRead();
  }

  @Test
  void validationTimeoutFollowedBySuccessOrClosedAllowsCopyWithoutReadingPreview() {
    for (StatementState state : List.of(StatementState.SUCCEEDED, StatementState.CLOSED)) {
      reset(statementExecutionAPI);
      prepareTimeout(response(state));
      assertDoesNotThrow(this::validateWithClock);
      verifyCancelThenFinalRead();
    }
  }

  @Test
  void validationCanceledStopsWithoutBecomingFailedValidation() {
    for (boolean afterTimeout : List.of(false, true)) {
      reset(statementExecutionAPI);
      if (afterTimeout) {
        prepareTimeout(response(StatementState.CANCELED));
      } else {
        submit(response(StatementState.CANCELED));
      }
      StatementExecutionException error =
          assertThrows(
              StatementExecutionException.class,
              () -> {
                if (afterTimeout) validateWithClock();
                else validate();
              });
      assertEquals(StatementState.CANCELED, error.state());
      assertFalse(error.outcomeUnknown());
    }
  }

  @Test
  void validationTransportAndUnresolvedTimeoutRemainUnknownAndNeverResubmit() {
    RuntimeException failure = new RuntimeException("lost validation response");
    when(statementExecutionAPI.executeStatement(any(ExecuteStatementRequest.class)))
        .thenThrow(failure);
    assertUnknown(assertThrows(StatementExecutionException.class, this::validate));
    verify(statementExecutionAPI).executeStatement(any(ExecuteStatementRequest.class));
    verifyNoMoreInteractions(statementExecutionAPI);

    reset(statementExecutionAPI);
    submit(response(StatementState.PENDING));
    when(statementExecutionAPI.getStatement(STATEMENT_ID)).thenThrow(failure);
    assertUnknown(assertThrows(StatementExecutionException.class, this::validate));
    verify(statementExecutionAPI).executeStatement(any(ExecuteStatementRequest.class));
    verify(statementExecutionAPI).getStatement(STATEMENT_ID);
    verifyNoMoreInteractions(statementExecutionAPI);

    reset(statementExecutionAPI);
    prepareTimeout(response(StatementState.RUNNING));
    assertUnknown(assertThrows(StatementExecutionException.class, this::validateWithClock));
    verifyCancelThenFinalRead();
  }

  @Test
  void validationMissingStatementIdentityOrStatusIsUnknown() {
    for (StatementResponse malformed :
        Arrays.asList(
            null,
            response(StatementState.SUCCEEDED).setStatementId(null),
            response(StatementState.PENDING).setStatementId(" "),
            new StatementResponse().setStatementId(STATEMENT_ID),
            response(null))) {
      submit(malformed);
      assertUnknown(assertThrows(StatementExecutionException.class, this::validate));
    }
    verify(statementExecutionAPI, never()).getStatement(anyString());
  }

  @Test
  void validationPollingCanFinishWithoutReadingPreview() {
    submit(response(StatementState.PENDING));
    when(statementExecutionAPI.getStatement(STATEMENT_ID))
        .thenReturn(response(StatementState.RUNNING), response(StatementState.SUCCEEDED));
    AtomicLong clock = new AtomicLong();

    assertDoesNotThrow(
        () ->
            new DatabricksSqlExecutor(workspaceClient, WAREHOUSE_ID, 30000)
                .validateCopyInto(
                    TABLE_NAME, VOLUME_PATTERN, Map.of(), Map.of(), clock::get, clock::addAndGet));

    assertEquals(5000, clock.get());
    verify(statementExecutionAPI).executeStatement(any(ExecuteStatementRequest.class));
    verify(statementExecutionAPI, times(2)).getStatement(STATEMENT_ID);
    verifyNoMoreInteractions(statementExecutionAPI);
  }

  @Test
  void handlesSuccessInInitialResponseWithoutPolling() {
    submit(success("num_inserted_rows", "200"));
    assertEquals(200, execute().rowsLoaded());
    verify(statementExecutionAPI, never()).getStatement(anyString());
  }

  @Test
  void closedInInitialResponseConfirmsCommitWithoutStatistics() {
    submit(response(StatementState.CLOSED));
    assertUnknownStatistics(execute());
    verify(statementExecutionAPI, never()).getStatement(anyString());
  }

  @Test
  void closedAfterPollingConfirmsCommitEvenIfOldResultStillExists() {
    submit(response(StatementState.PENDING));
    StatementResponse closed = success("num_inserted_rows", "200");
    closed.getStatus().setState(StatementState.CLOSED);
    when(statementExecutionAPI.getStatement(STATEMENT_ID)).thenReturn(closed);
    assertUnknownStatistics(execute());
  }

  @Test
  void pollsPendingAndRunningDeterministically() {
    submit(response(StatementState.PENDING));
    when(statementExecutionAPI.getStatement(STATEMENT_ID))
        .thenReturn(
            response(StatementState.PENDING),
            response(StatementState.RUNNING),
            success("num_inserted_rows", "200"));
    AtomicLong clock = new AtomicLong();
    CopyIntoStats stats =
        new DatabricksSqlExecutor(workspaceClient, WAREHOUSE_ID, 30000)
            .executeCopyIntoWithStats(
                TABLE_NAME, VOLUME_PATTERN, Map.of(), Map.of(), clock::get, clock::addAndGet);
    assertEquals(200, stats.rowsLoaded());
    assertEquals(10000, clock.get());
    verify(statementExecutionAPI, times(3)).getStatement(STATEMENT_ID);
    verify(statementExecutionAPI, never()).cancelExecution(any(CancelExecutionRequest.class));
  }

  @Test
  void mutatingFailureNeverProvesRollbackAndPreservesDiagnostics() {
    String[][] cases = {
      {"23001", "[CHECK_CONSTRAINT_VIOLATION] check failed"},
      {"23502", "[DELTA_NOT_NULL_CONSTRAINT_VIOLATED] null value"},
      {"22023", "[INVALID_PARAMETER_VALUE] CHECK expression failed"},
      {"2DKD0", "[DELTA_POST_COMMIT_HOOK_FAILED] transaction already committed"},
      {null, "Opaque provider failure"},
      {"42501", "Permission denied"}
    };
    for (String[] testCase : cases) {
      submit(failure(testCase[0], testCase[1]));
      StatementExecutionException error =
          assertThrows(StatementExecutionException.class, this::execute);
      assertTrue(error.outcomeUnknown(), Arrays.toString(testCase));
      assertEquals(STATEMENT_ID, error.statementId());
      assertEquals(StatementState.FAILED, error.state());
      assertEquals(testCase[0], error.sqlState());
      assertEquals("BAD_REQUEST", error.errorCode());
      assertEquals(testCase[1], error.serviceMessage());
    }
    verify(statementExecutionAPI, never()).getStatement(anyString());
  }

  @Test
  void validationFailureIsKnownRegardlessOfSqlStateOrErrorMessage() {
    String[][] cases = {
      {"23001", "[CHECK_CONSTRAINT_VIOLATION] check failed"},
      {"22023", "[INVALID_PARAMETER_VALUE] CHECK expression failed"},
      {null, "Opaque provider failure"},
      {null, null},
      {"42501", "Permission denied"}
    };
    for (String[] testCase : cases) {
      submit(failure(testCase[0], testCase[1]));
      StatementExecutionException error =
          assertThrows(StatementExecutionException.class, this::validate);
      assertFalse(error.outcomeUnknown(), Arrays.toString(testCase));
      assertEquals(STATEMENT_ID, error.statementId());
      assertEquals(StatementState.FAILED, error.state());
      assertEquals(testCase[0], error.sqlState());
      assertEquals("BAD_REQUEST", error.errorCode());
      assertEquals(testCase[1], error.serviceMessage());
    }
    submit(response(StatementState.FAILED));
    StatementExecutionException withoutError =
        assertThrows(StatementExecutionException.class, this::validate);
    assertFalse(withoutError.outcomeUnknown());
    assertEquals(StatementState.FAILED, withoutError.state());
    assertNull(withoutError.errorCode());
    assertNull(withoutError.serviceMessage());
    verify(statementExecutionAPI, never()).getStatement(anyString());
  }

  @Test
  void mutatingCanceledDoesNotProveRollbackEvenWithConstraintSqlState() {
    StatementResponse canceled = failure("23001", "[CHECK_CONSTRAINT_VIOLATION] stale error");
    canceled.getStatus().setState(StatementState.CANCELED);
    submit(canceled);
    StatementExecutionException error =
        assertThrows(StatementExecutionException.class, this::execute);
    assertTrue(error.outcomeUnknown());
    assertEquals(StatementState.CANCELED, error.state());
  }

  @Test
  void mutatingFailureWithoutServiceErrorRemainsUnknown() {
    submit(response(StatementState.FAILED));
    StatementExecutionException error =
        assertThrows(StatementExecutionException.class, this::execute);
    assertTrue(error.outcomeUnknown());
    assertNull(error.errorCode());
    assertNull(error.serviceMessage());
  }

  @Test
  void timeoutCancelAcknowledgmentDoesNotProveFailure() {
    prepareTimeout(response(StatementState.RUNNING));
    StatementExecutionException error =
        assertThrows(StatementExecutionException.class, this::executeWithClock);
    assertUnknown(error);
    assertTrue(error.getMessage().contains("timed out"));
    assertEquals(STATEMENT_ID, error.statementId());
    verifyCancelThenFinalRead();
  }

  @Test
  void timeoutFollowedBySuccessConfirmsCommit() {
    prepareTimeout(success("num_inserted_rows", "42"));
    assertEquals(42, executeWithClock().rowsLoaded());
    verifyCancelThenFinalRead();
  }

  @Test
  void timeoutFollowedByClosedConfirmsCommitWithoutStatistics() {
    prepareTimeout(response(StatementState.CLOSED));
    assertUnknownStatistics(executeWithClock());
    verifyCancelThenFinalRead();
  }

  @Test
  void mutatingTimeoutFollowedByDataFailureRemainsUnknown() {
    prepareTimeout(failure("23001", "[CHECK_CONSTRAINT_VIOLATION] failed"));
    StatementExecutionException error =
        assertThrows(StatementExecutionException.class, this::executeWithClock);
    assertTrue(error.outcomeUnknown());
    verifyCancelThenFinalRead();
  }

  @Test
  void mutatingTimeoutFollowedByCanceledRemainsUnknown() {
    prepareTimeout(response(StatementState.CANCELED));
    StatementExecutionException error =
        assertThrows(StatementExecutionException.class, this::executeWithClock);
    assertTrue(error.outcomeUnknown());
    verifyCancelThenFinalRead();
  }

  @Test
  void cancelFailureDoesNotHideCommittedFinalStatus() {
    prepareTimeout(success("num_inserted_rows", "42"));
    doThrow(new RuntimeException("cancel unavailable"))
        .when(statementExecutionAPI)
        .cancelExecution(any(CancelExecutionRequest.class));
    assertEquals(42, executeWithClock().rowsLoaded());
    verifyCancelThenFinalRead();
  }

  @Test
  void cancelAndFinalPollFailurePreserveUnknownAndBothCauses() {
    submit(response(StatementState.PENDING));
    RuntimeException cancelError = new RuntimeException("cancel unavailable");
    RuntimeException pollError = new RuntimeException("poll unavailable");
    when(statementExecutionAPI.getStatement(STATEMENT_ID))
        .thenReturn(response(StatementState.RUNNING))
        .thenThrow(pollError);
    doThrow(cancelError)
        .when(statementExecutionAPI)
        .cancelExecution(any(CancelExecutionRequest.class));
    StatementExecutionException error =
        assertThrows(StatementExecutionException.class, this::executeWithClock);
    assertUnknown(error);
    assertSame(pollError, error.getCause());
    assertArrayEquals(new Throwable[] {cancelError}, error.getSuppressed());
    verifyCancelThenFinalRead();
  }

  @Test
  void submitNetworkFailureIsUnknownAndNeverResubmits() {
    RuntimeException cause = new RuntimeException("lost submit response");
    when(statementExecutionAPI.executeStatement(any(ExecuteStatementRequest.class)))
        .thenThrow(cause);
    StatementExecutionException error =
        assertThrows(StatementExecutionException.class, this::execute);
    assertUnknown(error);
    assertNull(error.statementId());
    assertSame(cause, error.getCause());
    verify(statementExecutionAPI).executeStatement(any(ExecuteStatementRequest.class));
    verify(statementExecutionAPI, never()).getStatement(anyString());
  }

  @Test
  void pollNetworkFailureIsUnknownAndNeverResubmits() {
    submit(response(StatementState.PENDING));
    when(statementExecutionAPI.getStatement(STATEMENT_ID))
        .thenThrow(new RuntimeException("lost poll response"));
    StatementExecutionException error =
        assertThrows(StatementExecutionException.class, this::execute);
    assertUnknown(error);
    assertEquals(STATEMENT_ID, error.statementId());
    verify(statementExecutionAPI).executeStatement(any(ExecuteStatementRequest.class));
    verify(statementExecutionAPI, never()).cancelExecution(any(CancelExecutionRequest.class));
  }

  @Test
  void missingInitialResponseIdOrStatusIsUnknown() {
    List<StatementResponse> malformed =
        Arrays.asList(
            null,
            response(StatementState.PENDING).setStatementId(null),
            response(StatementState.PENDING).setStatementId(" "),
            new StatementResponse().setStatementId(STATEMENT_ID),
            response(null));
    for (StatementResponse response : malformed) {
      submit(response);
      assertUnknown(assertThrows(StatementExecutionException.class, this::execute));
    }
    verify(statementExecutionAPI, never()).getStatement(anyString());
  }

  @Test
  void missingPollStatusIsUnknown() {
    submit(response(StatementState.PENDING));
    when(statementExecutionAPI.getStatement(STATEMENT_ID)).thenReturn(new StatementResponse());
    assertUnknown(assertThrows(StatementExecutionException.class, this::execute));
  }

  @Test
  void missingFinalStatusAfterCancelRemainsUnknown() {
    prepareTimeout(new StatementResponse());
    assertUnknown(assertThrows(StatementExecutionException.class, this::executeWithClock));
    verifyCancelThenFinalRead();
  }

  @Test
  void interruptionWhileWaitingIsUnknownAndPreservesInterruptFlag() {
    submit(response(StatementState.PENDING));
    when(statementExecutionAPI.getStatement(STATEMENT_ID))
        .thenReturn(response(StatementState.RUNNING));
    try {
      StatementExecutionException error =
          assertThrows(
              StatementExecutionException.class,
              () ->
                  executor.executeCopyIntoWithStats(
                      TABLE_NAME,
                      VOLUME_PATTERN,
                      Map.of(),
                      Map.of(),
                      () -> 0L,
                      millis -> {
                        throw new InterruptedException("interrupted wait");
                      }));
      assertUnknown(error);
      assertTrue(Thread.currentThread().isInterrupted());
      assertInstanceOf(InterruptedException.class, error.getCause());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void interruptionWrappedBySdkPreservesInterruptFlag() {
    when(statementExecutionAPI.executeStatement(any(ExecuteStatementRequest.class)))
        .thenThrow(new RuntimeException(new InterruptedException("SDK interrupted")));
    try {
      assertUnknown(assertThrows(StatementExecutionException.class, this::execute));
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void supportsAllExistingRowCountColumnsAndKnownZero() {
    for (String column : List.of("num_inserted_rows", "num_copied_rows", "num_affected_rows")) {
      for (String count : List.of("0", "150")) {
        submit(success(column, count));
        CopyIntoStats stats = execute();
        assertTrue(stats.rowsLoadedKnown());
        assertEquals(Long.parseLong(count), stats.rowsLoaded());
      }
    }
  }

  @Test
  void zeroInPrimaryCountDoesNotFallThroughToDifferentMetric() {
    StatementResponse response = success("num_inserted_rows", "0");
    response
        .getManifest()
        .getSchema()
        .setColumns(
            List.of(
                new ColumnInfo().setName("num_inserted_rows"),
                new ColumnInfo().setName("num_affected_rows")));
    response.getResult().setDataArray(List.of(List.of("0", "999")));
    submit(response);
    CopyIntoStats stats = execute();
    assertTrue(stats.rowsLoadedKnown());
    assertEquals(0, stats.rowsLoaded());
  }

  @Test
  void successfulCommitWithMissingOrMalformedStatisticsIsStillSuccess() {
    List<StatementResponse> cases =
        Arrays.asList(
            response(StatementState.SUCCEEDED),
            success("num_inserted_rows", "bad-number"),
            success("num_inserted_rows", "-1"),
            success("num_inserted_rows", "9999999999999999999999999"),
            success("unknown_metric", "12"),
            success("num_inserted_rows", null),
            success("num_inserted_rows", "12").setManifest(null),
            success("num_inserted_rows", "12").setResult(new ResultData().setDataArray(List.of())),
            success("num_inserted_rows", "12")
                .setResult(new ResultData().setDataArray(List.of(List.of()))),
            success("num_inserted_rows", "12")
                .setResult(new ResultData().setDataArray(Collections.singletonList(null))));
    for (StatementResponse response : cases) {
      submit(response);
      assertUnknownStatistics(execute());
    }
    verify(statementExecutionAPI, never()).getStatement(anyString());
  }

  @Test
  void preservesDefaultConstructorAndStatsConstructorCompatibility() {
    DatabricksSqlExecutor defaults = new DatabricksSqlExecutor(workspaceClient, WAREHOUSE_ID);
    assertEquals(30 * 60 * 1000L, defaults.maxWaitMs());
    assertEquals(WAREHOUSE_ID, defaults.warehouseId());
    CopyIntoStats withErrors = new CopyIntoStats(100, 5, 5, List.of("Error"));
    assertTrue(withErrors.hasErrors());
    assertTrue(withErrors.rowsLoadedKnown());
    assertFalse(new CopyIntoStats(100, 5, 5, List.of()).hasErrors());
  }

  private void validate() {
    executor.validateCopyInto(TABLE_NAME, VOLUME_PATTERN, Map.of(), Map.of());
  }

  private void validateWithClock() {
    AtomicLong clock = new AtomicLong();
    executor.validateCopyInto(
        TABLE_NAME, VOLUME_PATTERN, Map.of(), Map.of(), clock::get, clock::addAndGet);
  }

  private CopyIntoStats execute() {
    return executor.executeCopyIntoWithStats(TABLE_NAME, VOLUME_PATTERN, Map.of(), Map.of());
  }

  private CopyIntoStats executeWithClock() {
    AtomicLong clock = new AtomicLong();
    return executor.executeCopyIntoWithStats(
        TABLE_NAME, VOLUME_PATTERN, Map.of(), Map.of(), clock::get, clock::addAndGet);
  }

  private void prepareTimeout(StatementResponse finalResponse) {
    submit(response(StatementState.PENDING));
    when(statementExecutionAPI.getStatement(STATEMENT_ID))
        .thenReturn(response(StatementState.RUNNING), finalResponse);
  }

  private void verifyCancelThenFinalRead() {
    InOrder order = inOrder(statementExecutionAPI);
    order.verify(statementExecutionAPI).executeStatement(any(ExecuteStatementRequest.class));
    order.verify(statementExecutionAPI).getStatement(STATEMENT_ID);
    order.verify(statementExecutionAPI).cancelExecution(any(CancelExecutionRequest.class));
    order.verify(statementExecutionAPI).getStatement(STATEMENT_ID);
    order.verifyNoMoreInteractions();
  }

  private void submit(StatementResponse response) {
    when(statementExecutionAPI.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(response);
  }

  private ExecuteStatementRequest submittedRequest() {
    ArgumentCaptor<ExecuteStatementRequest> captor =
        ArgumentCaptor.forClass(ExecuteStatementRequest.class);
    verify(statementExecutionAPI).executeStatement(captor.capture());
    return captor.getValue();
  }

  private static StatementResponse response(StatementState state) {
    return new StatementResponse()
        .setStatementId(STATEMENT_ID)
        .setStatus(new StatementStatus().setState(state));
  }

  private static StatementResponse success(String column, String value) {
    return response(StatementState.SUCCEEDED)
        .setManifest(
            new ResultManifest()
                .setSchema(
                    new ResultSchema().setColumns(List.of(new ColumnInfo().setName(column)))))
        .setResult(new ResultData().setDataArray(List.of(Collections.singletonList(value))));
  }

  private static StatementResponse failure(String sqlState, String message) {
    return new StatementResponse()
        .setStatementId(STATEMENT_ID)
        .setStatus(
            new StatementStatus()
                .setState(StatementState.FAILED)
                .setSqlState(sqlState)
                .setError(
                    new ServiceError()
                        .setErrorCode(ServiceErrorCode.BAD_REQUEST)
                        .setMessage(message)));
  }

  private static void assertUnknown(StatementExecutionException error) {
    assertTrue(error.outcomeUnknown());
  }

  private static void assertUnknownStatistics(CopyIntoStats stats) {
    assertFalse(stats.rowsLoadedKnown());
    assertFalse(stats.hasErrors());
    assertEquals(0, stats.rowsLoaded());
  }
}
