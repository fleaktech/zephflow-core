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
        sinkErrorCounter);
  }

  private File createMockFile(String name) throws Exception {
    File file = new File(tempDir.toFile(), name);
    assertTrue(file.createNewFile(), "Failed to create mock file: " + name);
    return file;
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
            sinkErrorCounter)) {

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

      verify(parquetWriter).writeParquetFiles(anyList(), eq(tempDir));
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
            sinkErrorCounter)) {

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
            sinkErrorCounter)) {

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
      assertTrue(result.errorOutputList().get(0).errorMessage().contains("Partial upload failure"));

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
            sinkErrorCounter)) {

      File mockFile = createMockFile("test.parquet");

      when(parquetWriter.writeParquetFiles(anyList(), any(Path.class)))
          .thenReturn(List.of(mockFile));

      when(sqlExecutor.executeCopyIntoWithStats(anyString(), anyString(), anyMap(), anyMap()))
          .thenThrow(new RuntimeException("COPY INTO failed"));

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
      verify(volumeUploader).deleteDirectory(anyString());
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
            sinkErrorCounter);

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

    verify(parquetWriter).writeParquetFiles(anyList(), eq(tempDir));
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
            sinkErrorCounter);

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

    java.lang.reflect.Method method =
        BatchDatabricksFlusher.class.getSuperclass().getDeclaredMethod("executeScheduledFlush");
    method.setAccessible(true);
    method.invoke(flusher);

    verify(dlqWriter).writeToDlq(anyLong(), any(), contains("Scheduled flush error"));
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
            sinkErrorCounter)) {
      SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
          new SimpleSinkCommand.PreparedInputEvents<>();
      events.add(
          (RecordFleakData) FleakData.wrap(Map.of("id", 1, "name", "test")),
          Map.of("id", 1, "name", "test"));
      flusher.flush(events, Map.of());

      when(parquetWriter.writeParquetFiles(anyList(), any(Path.class)))
          .thenThrow(new RuntimeException("Scheduled flush error"));

      java.lang.reflect.Method method =
          BatchDatabricksFlusher.class.getSuperclass().getDeclaredMethod("executeScheduledFlush");
      method.setAccessible(true);

      assertDoesNotThrow(() -> method.invoke(flusher));

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
            sinkErrorCounter);

    // Valid: id is integer, name is string
    Map<String, Object> validData = Map.of("id", 1, "name", "valid");
    // Invalid: id field has a non-parseable string "not-a-number" for INTEGER type
    Map<String, Object> invalidData = Map.of("id", "not-a-number", "name", "test");

    SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    events.add((RecordFleakData) FleakData.wrap(validData), validData);
    events.add((RecordFleakData) FleakData.wrap(invalidData), invalidData);

    when(parquetWriter.writeParquetFiles(argThat(l -> l != null && l.size() == 2), any()))
        .thenThrow(new RuntimeException("Batch write failed"));

    File mockFile = createMockFile("recovery.parquet");
    when(parquetWriter.writeParquetFiles(argThat(l -> l != null && l.size() == 1), any()))
        .thenReturn(List.of(mockFile));

    CopyIntoStats stats = new CopyIntoStats(1, 1, 1, List.of());
    when(sqlExecutor.executeCopyIntoWithStats(any(), any(), any(), any())).thenReturn(stats);

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(1, result.successCount(), "Should successfully recover the valid record");
    assertEquals(1, result.errorOutputList().size(), "Should report the invalid record as error");
    assertTrue(
        result.errorOutputList().get(0).errorMessage().contains("validation error"),
        "Error message should indicate validation failure in recovery mode");
  }
}
