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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.databrickssink.DatabricksSqlExecutor.CopyIntoStats;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BatchDatabricksFlusherTest {

  @TempDir Path tempDir;

  private DatabricksSinkDto.Config config;
  private DatabricksParquetWriter parquetWriter;
  private DatabricksVolumeUploader volumeUploader;
  private DatabricksSqlExecutor sqlExecutor;
  private DlqWriter dlqWriter;
  private StructType schema;
  private FleakCounter sinkOutputCounter;
  private FleakCounter outputSizeCounter;
  private FleakCounter sinkErrorCounter;

  @BeforeEach
  void setUp() {
    config =
        DatabricksSinkDto.Config.builder()
            .volumePath("/Volumes/catalog/schema/volume")
            .tableName("catalog.schema.test_table")
            .warehouseId("test-warehouse-id")
            .batchSize(100)
            .flushIntervalMillis(60000)
            .cleanupAfterCopy(true)
            .build();

    parquetWriter = mock(DatabricksParquetWriter.class);
    volumeUploader = mock(DatabricksVolumeUploader.class);
    sqlExecutor = mock(DatabricksSqlExecutor.class);
    dlqWriter = mock(DlqWriter.class);
    sinkOutputCounter = mock(FleakCounter.class);
    outputSizeCounter = mock(FleakCounter.class);
    sinkErrorCounter = mock(FleakCounter.class);

    schema =
        new StructType(
            List.of(
                new StructField("id", IntegerType.INTEGER, false),
                new StructField("name", StringType.STRING, true)));
  }

  private BatchDatabricksFlusher createFlusher() {
    return createFlusher(null);
  }

  private BatchDatabricksFlusher createFlusher(String nodeId) {
    return new BatchDatabricksFlusher(
        config,
        parquetWriter,
        volumeUploader,
        sqlExecutor,
        tempDir,
        dlqWriter,
        schema,
        sinkOutputCounter,
        outputSizeCounter,
        sinkErrorCounter,
        nodeId);
  }

  private File createMockFile(String name) throws Exception {
    File file = new File(tempDir.toFile(), name);
    assertTrue(file.createNewFile(), "Failed to create mock file: " + name);
    return file;
  }

  @Test
  void boundedUploadFailureDoesNotRetryCopyOrFlushOnAbort() throws Exception {
    var hooks = boundedHooks();
    var file = createMockFile("bounded.parquet");
    when(parquetWriter.writeParquetFiles(anyList(), any(Path.class))).thenReturn(List.of(file));
    var accepted = new java.util.concurrent.atomic.AtomicInteger();
    doAnswer(
            call -> {
              accepted.incrementAndGet();
              throw new IOException("accepted but response lost");
            })
        .when(volumeUploader)
        .uploadFile(eq(file), anyString());
    var sink =
        new BatchDatabricksFlusher(
            config,
            parquetWriter,
            volumeUploader,
            sqlExecutor,
            tempDir,
            null,
            schema,
            sinkOutputCounter,
            outputSizeCounter,
            sinkErrorCounter,
            "db",
            io.fleak.zephflow.api.JobContext.builder().executionHooks(hooks).build());
    var events = new SimpleSinkCommand.PreparedInputEvents<Map<String, Object>>();
    var data = Map.<String, Object>of("id", 1, "name", "one");
    events.add((RecordFleakData) FleakData.wrap(data), data);
    try {
      var result = sink.flushBounded(events, Map.of(), hooks);
      assertEquals(0, result.successCount());
      assertEquals(1, result.errorOutputList().size());
      assertEquals(
          io.fleak.zephflow.api.execution.EffectOutcome.Delivery.UNKNOWN,
          result.boundedOutcome(1).delivery());
    } finally {
      sink.abort();
    }
    assertEquals(1, accepted.get());
    verifyNoInteractions(sqlExecutor);
  }

  @Test
  void boundedUnknownCopyIsNotRepeatedOrReinterpretedAsSuccess() throws Exception {
    var hooks = boundedHooks();
    var file = createMockFile("bounded-copy.parquet");
    when(parquetWriter.writeParquetFiles(anyList(), any(Path.class))).thenReturn(List.of(file));
    when(sqlExecutor.executeCopyIntoWithStats(anyString(), anyString(), anyMap(), anyMap()))
        .thenThrow(new RuntimeException("COPY accepted; result lost"));
    var sink =
        new BatchDatabricksFlusher(
            config,
            parquetWriter,
            volumeUploader,
            sqlExecutor,
            tempDir,
            null,
            schema,
            sinkOutputCounter,
            outputSizeCounter,
            sinkErrorCounter,
            "db",
            io.fleak.zephflow.api.JobContext.builder().executionHooks(hooks).build());
    var events = new SimpleSinkCommand.PreparedInputEvents<Map<String, Object>>();
    var data = Map.<String, Object>of("id", 1, "name", "one");
    events.add((RecordFleakData) FleakData.wrap(data), data);
    try {
      var result = sink.flushBounded(events, Map.of(), hooks);
      assertEquals(
          io.fleak.zephflow.api.execution.EffectOutcome.Delivery.UNKNOWN,
          result.boundedOutcome(1).delivery());
      assertEquals(1, result.errorOutputList().size());
    } finally {
      sink.abort();
    }
    verify(sqlExecutor, times(1))
        .executeCopyIntoWithStats(anyString(), anyString(), anyMap(), anyMap());
    verify(volumeUploader, never()).deleteDirectory(anyString());
  }

  @org.junit.jupiter.params.ParameterizedTest
  @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
  void boundedLocalRejectionRemainsDefiniteWithOrWithoutSuccessfulRecords(boolean mixed)
      throws Exception {
    var hooks = boundedHooks();
    when(parquetWriter.writeParquetFiles(anyList(), any(Path.class)))
        .thenAnswer(
            call -> {
              List<Map<String, Object>> values = call.getArgument(0);
              if (values.stream().anyMatch(value -> value.get("id") instanceof String))
                throw new io.fleak.zephflow.lib.commands.deltalakesink.InvalidRecordException(
                    "private-row-id");
              Path file =
                  java.nio.file.Files.createTempFile(
                      call.getArgument(1, Path.class), "valid-", ".parquet");
              return List.of(file.toFile());
            });
    when(sqlExecutor.executeCopyIntoWithStats(anyString(), anyString(), anyMap(), anyMap()))
        .thenReturn(new CopyIntoStats(1, 1, 1, List.of(), true));
    var events = new SimpleSinkCommand.PreparedInputEvents<Map<String, Object>>();
    var invalid = Map.<String, Object>of("id", "private-row-id", "name", "invalid");
    events.add((RecordFleakData) FleakData.wrap(invalid), invalid);
    if (mixed) {
      var valid = Map.<String, Object>of("id", 1, "name", "valid");
      events.add((RecordFleakData) FleakData.wrap(valid), valid);
    }
    try (var sink = boundedFlusher(hooks);
        var logs =
            new io.fleak.zephflow.lib.utils.BoundedLogCapture(BatchDatabricksFlusher.class)) {
      var result = sink.flushBounded(events, Map.of(), hooks);
      var outcome = result.effectOutcome();
      assertEquals(mixed ? 1L : 0L, outcome.attemptedCount());
      assertEquals(mixed ? 1L : 0L, outcome.acknowledgedCount());
      assertEquals(1L, outcome.definiteFailureCount());
      assertEquals(1L, outcome.notAttemptedCount());
      assertEquals(0L, outcome.unknownCount());
      assertEquals(
          mixed
              ? io.fleak.zephflow.api.execution.EffectOutcome.Delivery.PARTIAL
              : io.fleak.zephflow.api.execution.EffectOutcome.Delivery.FAILED,
          outcome.delivery());
      assertEquals(1, result.errorOutputList().size());
      assertEquals(invalid, result.errorOutputList().getFirst().inputEvent().unwrap());
      assertTrue(logs.events().stream().allMatch(event -> event.getThrown() == null));
      assertTrue(
          logs.events().stream()
              .noneMatch(
                  event -> event.getMessage().getFormattedMessage().contains("private-row-id")));
    }
    if (mixed) verify(volumeUploader).uploadFile(any(), anyString());
    else verifyNoInteractions(volumeUploader, sqlExecutor);
  }

  @Test
  void boundedGenerationUploadAndCleanupFailuresLogOnlySafeContext() throws Exception {
    var hooks = boundedHooks();
    var failure =
        new IOException(
            "token=databricks-private-token", new IllegalStateException("nested-private-row"));
    var events = new SimpleSinkCommand.PreparedInputEvents<Map<String, Object>>();
    var data = Map.<String, Object>of("id", 1, "name", "one");
    events.add((RecordFleakData) FleakData.wrap(data), data);
    try (var logs =
        new io.fleak.zephflow.lib.utils.BoundedLogCapture(BatchDatabricksFlusher.class)) {
      when(parquetWriter.writeParquetFiles(anyList(), any(Path.class))).thenThrow(failure);
      try (var sink = boundedFlusher(hooks)) {
        assertEquals(1, sink.flushBounded(events, Map.of(), hooks).errorOutputList().size());
      }
      java.nio.file.Files.createDirectories(tempDir);
      var file = createMockFile("upload-failure.parquet");
      when(parquetWriter.writeParquetFiles(anyList(), any(Path.class))).thenReturn(List.of(file));
      doThrow(failure).when(volumeUploader).uploadFile(any(), anyString());
      doThrow(new IllegalStateException("token=databricks-private-token", failure))
          .when(volumeUploader)
          .deleteDirectory(anyString());
      try (var sink = boundedFlusher(hooks)) {
        assertEquals(1L, sink.flushBounded(events, Map.of(), hooks).effectOutcome().unknownCount());
      }
      assertTrue(
          logs.events().stream()
              .anyMatch(
                  event ->
                      event
                          .getMessage()
                          .getFormattedMessage()
                          .contains("Bounded Databricks remote cleanup failed")));
      assertTrue(logs.events().stream().allMatch(event -> event.getThrown() == null));
      assertTrue(
          logs.events().stream()
              .noneMatch(
                  event ->
                      event.getMessage().getFormattedMessage().contains("private-token")
                          || event.getMessage().getFormattedMessage().contains("private-row")));
    }
  }

  @Test
  void boundedStopImmediatelyBeforeUploadPreservesKnownUnsentReceipt() throws Exception {
    var checks = new java.util.concurrent.atomic.AtomicInteger();
    var hooks =
        new io.fleak.zephflow.api.execution.ExecutionHooks(
            () -> {
              if (checks.incrementAndGet() == 3)
                throw new io.fleak.zephflow.api.execution.ExecutionStoppedException(
                    "cancel before first upload");
            },
            mock(io.fleak.zephflow.api.execution.ExecutionHooks.Effects.class),
            Runnable::run);
    var file = createMockFile("cancel.parquet");
    when(parquetWriter.writeParquetFiles(anyList(), any(Path.class))).thenReturn(List.of(file));
    var events = new SimpleSinkCommand.PreparedInputEvents<Map<String, Object>>();
    var data = Map.<String, Object>of("id", 1, "name", "one");
    events.add((RecordFleakData) FleakData.wrap(data), data);
    try (var sink = boundedFlusher(hooks)) {
      var stopped =
          assertThrows(
              io.fleak.zephflow.lib.commands.sink.BoundedFlushException.class,
              () -> sink.flushBounded(events, Map.of(), hooks));
      var outcome = stopped.result().effectOutcome();
      assertEquals(0L, outcome.attemptedCount());
      assertEquals(0L, outcome.unknownCount());
      assertEquals(1L, outcome.notAttemptedCount());
      assertEquals(
          io.fleak.zephflow.api.execution.EffectOutcome.Delivery.NOT_ATTEMPTED, outcome.delivery());
      assertInstanceOf(
          io.fleak.zephflow.api.execution.ExecutionStoppedException.class, stopped.getCause());
    }
    verify(volumeUploader, never()).uploadFile(any(), anyString());
    verifyNoInteractions(sqlExecutor);
  }

  private BatchDatabricksFlusher boundedFlusher(
      io.fleak.zephflow.api.execution.ExecutionHooks hooks) {
    return new BatchDatabricksFlusher(
        config,
        parquetWriter,
        volumeUploader,
        sqlExecutor,
        tempDir,
        null,
        schema,
        sinkOutputCounter,
        outputSizeCounter,
        sinkErrorCounter,
        "db",
        io.fleak.zephflow.api.JobContext.builder().executionHooks(hooks).build());
  }

  private static io.fleak.zephflow.api.execution.ExecutionHooks boundedHooks() {
    return new io.fleak.zephflow.api.execution.ExecutionHooks(
        () -> {},
        new io.fleak.zephflow.api.execution.ExecutionHooks.Effects() {
          public long started() {
            return 1;
          }

          public void finished(long id, io.fleak.zephflow.api.execution.EffectOutcome outcome) {}
        },
        Runnable::run);
  }

  @Test
  void testFlushEmptyEvents() throws Exception {
    try (BatchDatabricksFlusher flusher = createFlusher()) {
      SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> emptyEvents =
          new SimpleSinkCommand.PreparedInputEvents<>();

      SimpleSinkCommand.FlushResult result = flusher.flush(emptyEvents, Map.of());
      assertEquals(0, result.successCount());
      assertEquals(0, result.flushedDataSize());
      assertTrue(result.errorOutputList().isEmpty());
    }

    verifyNoInteractions(parquetWriter, volumeUploader, sqlExecutor);
  }

  @Test
  void testFlushBuffersUntilBatchSize() throws Exception {
    try (BatchDatabricksFlusher flusher = createFlusher()) {

      Map<String, Object> testData = Map.of("id", 1, "name", "test");
      RecordFleakData testRecord = (RecordFleakData) FleakData.wrap(testData);

      SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      events.add(testRecord, testData);

      SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

      assertEquals(0, result.successCount());
      verifyNoInteractions(parquetWriter, volumeUploader, sqlExecutor);
    }
  }

  @Test
  void testFlushTriggersWhenBatchSizeReached() throws Exception {
    DatabricksSinkDto.Config smallBatchConfig =
        DatabricksSinkDto.Config.builder()
            .volumePath("/Volumes/catalog/schema/volume")
            .tableName("catalog.schema.test_table")
            .warehouseId("test-warehouse-id")
            .batchSize(2)
            .flushIntervalMillis(60000)
            .cleanupAfterCopy(true)
            .build();

    try (BatchDatabricksFlusher flusher =
        new BatchDatabricksFlusher(
            smallBatchConfig,
            parquetWriter,
            volumeUploader,
            sqlExecutor,
            tempDir,
            dlqWriter,
            schema,
            sinkOutputCounter,
            outputSizeCounter,
            sinkErrorCounter,
            null)) {

      File mockFile = createMockFile("test.parquet");

      when(parquetWriter.writeParquetFiles(anyList(), any(Path.class)))
          .thenReturn(List.of(mockFile));

      CopyIntoStats stats = new CopyIntoStats(2, 1, 1, List.of());
      when(sqlExecutor.executeCopyIntoWithStats(anyString(), anyString(), anyMap(), anyMap()))
          .thenReturn(stats);

      SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      events.add(
          (RecordFleakData) FleakData.wrap(Map.of("id", 1, "name", "test1")),
          Map.of("id", 1, "name", "test1"));
      events.add(
          (RecordFleakData) FleakData.wrap(Map.of("id", 2, "name", "test2")),
          Map.of("id", 2, "name", "test2"));

      SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

      assertEquals(2, result.successCount());
      assertTrue(result.errorOutputList().isEmpty());

      verify(parquetWriter)
          .writeParquetFiles(anyList(), argThat(path -> path.getParent().equals(tempDir)));
      verify(volumeUploader).uploadFile(eq(mockFile), contains("test.parquet"));
      verify(sqlExecutor).executeCopyIntoWithStats(anyString(), anyString(), anyMap(), anyMap());
    }
  }

  @Test
  void testFlushHandlesParquetWriteFailure() throws Exception {
    DatabricksSinkDto.Config smallBatchConfig =
        DatabricksSinkDto.Config.builder()
            .volumePath("/Volumes/catalog/schema/volume")
            .tableName("catalog.schema.test_table")
            .warehouseId("test-warehouse-id")
            .batchSize(1)
            .flushIntervalMillis(60000)
            .cleanupAfterCopy(true)
            .build();

    try (BatchDatabricksFlusher flusher =
        new BatchDatabricksFlusher(
            smallBatchConfig,
            parquetWriter,
            volumeUploader,
            sqlExecutor,
            tempDir,
            dlqWriter,
            schema,
            sinkOutputCounter,
            outputSizeCounter,
            sinkErrorCounter,
            null)) {

      when(parquetWriter.writeParquetFiles(anyList(), any(Path.class)))
          .thenThrow(new RuntimeException("Parquet write failed"));

      SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      events.add(
          (RecordFleakData) FleakData.wrap(Map.of("id", 1, "name", "test")),
          Map.of("id", 1, "name", "test"));

      SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

      assertEquals(0, result.successCount());
      assertEquals(1, result.errorOutputList().size());
      assertTrue(result.errorOutputList().get(0).errorMessage().contains("Parquet"));

      verifyNoInteractions(volumeUploader, sqlExecutor);
    }
  }

  @Test
  void testFlushHandlesUploadFailure() throws Exception {
    DatabricksSinkDto.Config smallBatchConfig =
        DatabricksSinkDto.Config.builder()
            .volumePath("/Volumes/catalog/schema/volume")
            .tableName("catalog.schema.test_table")
            .warehouseId("test-warehouse-id")
            .batchSize(1)
            .flushIntervalMillis(60000)
            .cleanupAfterCopy(true)
            .build();

    try (BatchDatabricksFlusher flusher =
        new BatchDatabricksFlusher(
            smallBatchConfig,
            parquetWriter,
            volumeUploader,
            sqlExecutor,
            tempDir,
            dlqWriter,
            schema,
            sinkOutputCounter,
            outputSizeCounter,
            sinkErrorCounter,
            null)) {

      File mockFile = createMockFile("test.parquet");

      when(parquetWriter.writeParquetFiles(anyList(), any(Path.class)))
          .thenReturn(List.of(mockFile));

      doThrow(new RuntimeException("Upload failed"))
          .when(volumeUploader)
          .uploadFile(any(File.class), anyString());

      SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      events.add(
          (RecordFleakData) FleakData.wrap(Map.of("id", 1, "name", "test")),
          Map.of("id", 1, "name", "test"));

      SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

      assertEquals(0, result.successCount());
      assertEquals(1, result.errorOutputList().size());
      String errorMessage = result.errorOutputList().get(0).errorMessage();
      assertTrue(errorMessage.startsWith("Databricks upload failed: "));
      assertTrue(errorMessage.contains("Upload failed"));

      verifyNoInteractions(sqlExecutor);
    }
  }

  @Test
  void testFlushHandlesUploadFailureWithPermissionDenied() throws Exception {
    DatabricksSinkDto.Config smallBatchConfig =
        DatabricksSinkDto.Config.builder()
            .volumePath("/Volumes/test/catalog/vol")
            .tableName("test.catalog.vol_table")
            .warehouseId("test-warehouse-id")
            .batchSize(1)
            .flushIntervalMillis(60000)
            .cleanupAfterCopy(true)
            .build();

    try (BatchDatabricksFlusher flusher =
        new BatchDatabricksFlusher(
            smallBatchConfig,
            parquetWriter,
            volumeUploader,
            sqlExecutor,
            tempDir,
            dlqWriter,
            schema,
            sinkOutputCounter,
            outputSizeCounter,
            sinkErrorCounter,
            null)) {

      File mockFile = createMockFile("test.parquet");

      when(parquetWriter.writeParquetFiles(anyList(), any(Path.class)))
          .thenReturn(List.of(mockFile));

      // Mimics what DatabricksVolumeUploader.uploadFile throws when the
      // Databricks SDK reports a PERMISSION_DENIED on the target volume.
      doThrow(
              new IOException(
                  "Upload failed: User does not have WRITE VOLUME privilege on VOLUME"
                      + " 'test.catalog.vol'."))
          .when(volumeUploader)
          .uploadFile(any(File.class), anyString());

      SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      events.add(
          (RecordFleakData) FleakData.wrap(Map.of("id", 1, "name", "test")),
          Map.of("id", 1, "name", "test"));

      SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

      assertEquals(0, result.successCount());
      assertEquals(1, result.errorOutputList().size());
      String errorMessage = result.errorOutputList().get(0).errorMessage();
      assertTrue(
          errorMessage.startsWith("Databricks upload failed: "),
          "expected prefix 'Databricks upload failed: ' but got: " + errorMessage);
      assertTrue(
          errorMessage.contains("WRITE VOLUME"),
          "expected 'WRITE VOLUME' in message but got: " + errorMessage);
      assertTrue(
          errorMessage.contains("test.catalog.vol"),
          "expected volume name in message but got: " + errorMessage);

      verifyNoInteractions(sqlExecutor);
    }
  }

  @Test
  void testFlushHandlesCopyIntoFailure() throws Exception {
    DatabricksSinkDto.Config smallBatchConfig =
        DatabricksSinkDto.Config.builder()
            .volumePath("/Volumes/catalog/schema/volume")
            .tableName("catalog.schema.test_table")
            .warehouseId("test-warehouse-id")
            .batchSize(1)
            .flushIntervalMillis(60000)
            .cleanupAfterCopy(true)
            .build();

    try (BatchDatabricksFlusher flusher =
        new BatchDatabricksFlusher(
            smallBatchConfig,
            parquetWriter,
            volumeUploader,
            sqlExecutor,
            tempDir,
            dlqWriter,
            schema,
            sinkOutputCounter,
            outputSizeCounter,
            sinkErrorCounter,
            null)) {

      File mockFile = createMockFile("test.parquet");

      when(parquetWriter.writeParquetFiles(anyList(), any(Path.class)))
          .thenReturn(List.of(mockFile));

      when(sqlExecutor.executeCopyIntoWithStats(anyString(), anyString(), anyMap(), anyMap()))
          .thenThrow(
              new DatabricksSqlExecutor.StatementExecutionException(
                  "statement",
                  com.databricks.sdk.service.sql.StatementState.FAILED,
                  "42501",
                  "PERMISSION_DENIED",
                  "COPY INTO failed",
                  false,
                  null));

      SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      events.add(
          (RecordFleakData) FleakData.wrap(Map.of("id", 1, "name", "test")),
          Map.of("id", 1, "name", "test"));

      SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

      assertEquals(0, result.successCount());
      assertEquals(1, result.errorOutputList().size());
      assertTrue(result.errorOutputList().get(0).errorMessage().contains("COPY INTO failed"));

      verify(volumeUploader).uploadFile(any(File.class), anyString());
      verify(volumeUploader, never()).deleteDirectory(anyString());
      assertTrue(result.errorOutputList().get(0).errorMessage().contains("outcome unknown"));
      verify(sqlExecutor).validateCopyInto(anyString(), anyString(), anyMap(), anyMap());
    }
  }

  @Test
  void testFlushAfterCloseThrowsException() throws Exception {
    BatchDatabricksFlusher flusher = createFlusher();
    flusher.close();

    SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    events.add(
        (RecordFleakData) FleakData.wrap(Map.of("id", 1, "name", "test")),
        Map.of("id", 1, "name", "test"));

    assertThrows(IllegalStateException.class, () -> flusher.flush(events, Map.of()));
  }

  @Test
  void testCloseFlushesRemainingBuffer() throws Exception {
    DatabricksSinkDto.Config largerBatchConfig =
        DatabricksSinkDto.Config.builder()
            .volumePath("/Volumes/catalog/schema/volume")
            .tableName("catalog.schema.test_table")
            .warehouseId("test-warehouse-id")
            .batchSize(100)
            .flushIntervalMillis(60000)
            .cleanupAfterCopy(true)
            .build();

    BatchDatabricksFlusher flusher =
        new BatchDatabricksFlusher(
            largerBatchConfig,
            parquetWriter,
            volumeUploader,
            sqlExecutor,
            tempDir,
            dlqWriter,
            schema,
            sinkOutputCounter,
            outputSizeCounter,
            sinkErrorCounter,
            null);

    File mockFile = createMockFile("test.parquet");

    when(parquetWriter.writeParquetFiles(anyList(), any(Path.class))).thenReturn(List.of(mockFile));

    CopyIntoStats stats = new CopyIntoStats(1, 1, 1, List.of());
    when(sqlExecutor.executeCopyIntoWithStats(anyString(), anyString(), anyMap(), anyMap()))
        .thenReturn(stats);

    SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    events.add(
        (RecordFleakData) FleakData.wrap(Map.of("id", 1, "name", "test")),
        Map.of("id", 1, "name", "test"));

    flusher.flush(events, Map.of());

    verifyNoInteractions(parquetWriter);

    flusher.close();

    verify(parquetWriter)
        .writeParquetFiles(anyList(), argThat(path -> path.getParent().equals(tempDir)));
    verify(volumeUploader).uploadFile(any(File.class), anyString());
    verify(sqlExecutor).executeCopyIntoWithStats(anyString(), anyString(), anyMap(), anyMap());
  }

  @Test
  void testCloseIsIdempotent() throws Exception {
    BatchDatabricksFlusher flusher = createFlusher();

    flusher.close();
    flusher.close();

    // No exception thrown on second close
  }

  @Test
  void testVolumePathFormattingWithTrailingSlash() throws Exception {
    DatabricksSinkDto.Config configWithSlash =
        DatabricksSinkDto.Config.builder()
            .volumePath("/Volumes/catalog/schema/volume/")
            .tableName("catalog.schema.test_table")
            .warehouseId("test-warehouse-id")
            .batchSize(1)
            .flushIntervalMillis(60000)
            .cleanupAfterCopy(true)
            .build();

    BatchDatabricksFlusher flusher =
        new BatchDatabricksFlusher(
            configWithSlash,
            parquetWriter,
            volumeUploader,
            sqlExecutor,
            tempDir,
            dlqWriter,
            schema,
            sinkOutputCounter,
            outputSizeCounter,
            sinkErrorCounter,
            null);

    File mockFile = createMockFile("test.parquet");

    when(parquetWriter.writeParquetFiles(anyList(), any(Path.class))).thenReturn(List.of(mockFile));

    CopyIntoStats stats = new CopyIntoStats(1, 1, 1, List.of());
    when(sqlExecutor.executeCopyIntoWithStats(anyString(), anyString(), anyMap(), anyMap()))
        .thenReturn(stats);

    SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    events.add(
        (RecordFleakData) FleakData.wrap(Map.of("id", 1, "name", "test")),
        Map.of("id", 1, "name", "test"));

    flusher.flush(events, Map.of());

    verify(volumeUploader)
        .uploadFile(
            eq(mockFile), argThat(path -> !path.contains("//") && path.contains("/test.parquet")));
  }

  @Test
  void testScheduledFlushWritesToDlqOnError() throws Exception {
    BatchDatabricksFlusher flusher = createFlusher();

    SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    events.add(
        (RecordFleakData) FleakData.wrap(Map.of("id", 1, "name", "test")),
        Map.of("id", 1, "name", "test"));
    flusher.flush(events, Map.of());

    when(parquetWriter.writeParquetFiles(anyList(), any(Path.class)))
        .thenThrow(new RuntimeException("Scheduled flush error"));

    flusher.executeScheduledFlush();

    verify(dlqWriter).writeToDlq(anyLong(), any(), contains("Scheduled flush error"), isNull());
  }

  @Test
  void testScheduledFlushWritesToDlqWithNodeId() throws Exception {
    BatchDatabricksFlusher flusher = createFlusher("test-node");

    SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    events.add(
        (RecordFleakData) FleakData.wrap(Map.of("id", 1, "name", "test")),
        Map.of("id", 1, "name", "test"));
    flusher.flush(events, Map.of());

    when(parquetWriter.writeParquetFiles(anyList(), any(Path.class)))
        .thenThrow(new RuntimeException("Scheduled flush error"));

    flusher.executeScheduledFlush();

    verify(dlqWriter)
        .writeToDlq(anyLong(), any(), contains("Scheduled flush error"), eq("test-node"));
  }

  @Test
  void testScheduledFlushNoDlqConfigured() throws Exception {
    try (BatchDatabricksFlusher flusher =
        new BatchDatabricksFlusher(
            config,
            parquetWriter,
            volumeUploader,
            sqlExecutor,
            tempDir,
            null,
            schema,
            sinkOutputCounter,
            outputSizeCounter,
            sinkErrorCounter,
            null)) {
      SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      events.add(
          (RecordFleakData) FleakData.wrap(Map.of("id", 1, "name", "test")),
          Map.of("id", 1, "name", "test"));
      flusher.flush(events, Map.of());

      when(parquetWriter.writeParquetFiles(anyList(), any(Path.class)))
          .thenThrow(new RuntimeException("Scheduled flush error"));

      assertDoesNotThrow(() -> flusher.executeScheduledFlush());

      verifyNoInteractions(dlqWriter);
    }
  }

  @Test
  void testFlushRecoversFromBatchWriteFailure() throws Exception {
    DatabricksSinkDto.Config smallBatchConfig =
        DatabricksSinkDto.Config.builder()
            .volumePath("/Volumes/catalog/schema/volume")
            .tableName("catalog.schema.test_table")
            .warehouseId("test-warehouse-id")
            .batchSize(2)
            .flushIntervalMillis(60000)
            .cleanupAfterCopy(true)
            .build();

    BatchDatabricksFlusher flusher =
        new BatchDatabricksFlusher(
            smallBatchConfig,
            parquetWriter,
            volumeUploader,
            sqlExecutor,
            tempDir,
            dlqWriter,
            schema,
            sinkOutputCounter,
            outputSizeCounter,
            sinkErrorCounter,
            null);

    // Valid: id is integer, name is string
    Map<String, Object> validData = Map.of("id", 1, "name", "valid");
    // Invalid: id field has a non-parseable string "not-a-number" for INTEGER type
    Map<String, Object> invalidData = Map.of("id", "not-a-number", "name", "test");

    SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    events.add((RecordFleakData) FleakData.wrap(validData), validData);
    events.add((RecordFleakData) FleakData.wrap(invalidData), invalidData);

    when(parquetWriter.writeParquetFiles(anyList(), any()))
        .thenAnswer(
            invocation ->
                new DatabricksParquetWriter(schema)
                    .writeParquetFiles(invocation.getArgument(0), invocation.getArgument(1)));

    CopyIntoStats stats = new CopyIntoStats(1, 1, 1, List.of());
    when(sqlExecutor.executeCopyIntoWithStats(any(), any(), any(), any())).thenReturn(stats);

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(1, result.successCount(), "Should successfully recover the valid record");
    assertEquals(1, result.errorOutputList().size(), "Should report the invalid record as error");
    assertTrue(
        result.errorOutputList().get(0).errorMessage().contains("Parquet conversion failed"),
        "Error message should indicate validation failure in recovery mode");
  }
}
