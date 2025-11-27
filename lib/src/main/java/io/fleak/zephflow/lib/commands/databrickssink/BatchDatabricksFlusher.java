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
import io.delta.kernel.types.StructType;
import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.databrickssink.DatabricksSqlExecutor.CopyIntoStats;
import io.fleak.zephflow.lib.commands.deltalakesink.DeltaLakeDataConverter;
import io.fleak.zephflow.lib.commands.sink.RecordFleakDataEncoder;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class BatchDatabricksFlusher implements SimpleSinkCommand.Flusher<Map<String, Object>> {

  private static final int MAX_UPLOAD_RETRIES = 3;
  private static final long INITIAL_RETRY_DELAY_MS = 1000;

  private final DatabricksSinkDto.Config config;
  private final DatabricksParquetWriter parquetWriter;
  private final DatabricksVolumeUploader volumeUploader;
  private final DatabricksSqlExecutor sqlExecutor;
  private final Path tempDirectory;
  private final DlqWriter dlqWriter;
  private final RecordFleakDataEncoder recordEncoder = new RecordFleakDataEncoder();
  private final StructType schema;

  private final Object bufferLock = new Object();
  private List<Pair<RecordFleakData, Map<String, Object>>> buffer = new ArrayList<>();

  private final ReentrantLock flushLock = new ReentrantLock();
  private final ScheduledFuture<?> scheduledFuture;
  private volatile boolean closed = false;

  public BatchDatabricksFlusher(
      DatabricksSinkDto.Config config,
      WorkspaceClient workspaceClient,
      Path tempDirectory,
      ScheduledExecutorService scheduler,
      DlqWriter dlqWriter) {

    this.config = config;
    schema = AvroToDeltaSchemaConverter.parse(config.getAvroSchema());
    this.parquetWriter = new DatabricksParquetWriter(schema);
    this.volumeUploader = new DatabricksVolumeUploader(workspaceClient);
    this.sqlExecutor = new DatabricksSqlExecutor(workspaceClient, config.getWarehouseId());
    this.tempDirectory = tempDirectory;
    this.dlqWriter = dlqWriter;

    this.scheduledFuture =
        scheduler.scheduleAtFixedRate(
            this::flushBufferScheduled,
            config.getFlushIntervalMillis(),
            config.getFlushIntervalMillis(),
            TimeUnit.MILLISECONDS);

    log.info(
        "BatchDatabricksFlusher initialized: batchSize={}, flushInterval={}ms",
        config.getBatchSize(),
        config.getFlushIntervalMillis());
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events) throws Exception {

    if (closed) {
      throw new IllegalStateException("BatchDatabricksFlusher is closed");
    }

    if (events.preparedList().isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    List<Pair<RecordFleakData, Map<String, Object>>> snapshot = null;

    synchronized (bufferLock) {
      buffer.addAll(events.rawAndPreparedList());

      log.debug(
          "Added {} records to buffer. Buffer: {} records",
          events.rawAndPreparedList().size(),
          buffer.size());

      if (buffer.size() >= config.getBatchSize()) {
        log.info("Buffer threshold exceeded, triggering immediate flush");
        snapshot = swapBuffer();
      }
    }

    if (snapshot != null) {
      return flushBatchToDatabricks(snapshot);
    }

    return new SimpleSinkCommand.FlushResult(0, 0, List.of());
  }

  private void flushBufferScheduled() {
    List<Pair<RecordFleakData, Map<String, Object>>> snapshot = null;

    synchronized (bufferLock) {
      if (!buffer.isEmpty()) {
        log.info("Timer-triggered flush: {} records buffered", buffer.size());
        snapshot = swapBuffer();
      }
    }

    if (snapshot != null) {
      try {
        SimpleSinkCommand.FlushResult result = flushBatchToDatabricks(snapshot);
        if (!result.errorOutputList().isEmpty()) {
          handleScheduledFlushErrors(result.errorOutputList());
        }
      } catch (Exception e) {
        log.error("Error during scheduled flush", e);
        handleScheduledFlushErrors(snapshot, e.getMessage());
      }
    }
  }

  private void handleScheduledFlushErrors(List<ErrorOutput> errors) {
    if (dlqWriter == null) {
      log.error(
          "Scheduled flush failed with {} errors but no DLQ configured. Records lost.",
          errors.size());
      return;
    }

    long ts = System.currentTimeMillis();
    int dlqWriteFailures = 0;
    for (ErrorOutput error : errors) {
      try {
        SerializedEvent serialized = recordEncoder.serialize(error.inputEvent());
        dlqWriter.writeToDlq(ts, serialized, error.errorMessage());
      } catch (Exception e) {
        dlqWriteFailures++;
        log.debug("Failed to write to DLQ: {}", e.getMessage());
      }
    }

    if (dlqWriteFailures > 0) {
      log.warn(
          "Wrote {} failed records to DLQ, {} DLQ write failures",
          errors.size() - dlqWriteFailures,
          dlqWriteFailures);
    } else {
      log.info("Wrote {} failed records to DLQ", errors.size());
    }
  }

  private void handleScheduledFlushErrors(
      List<Pair<RecordFleakData, Map<String, Object>>> snapshot, String errorMessage) {
    List<ErrorOutput> errors =
        snapshot.stream().map(pair -> new ErrorOutput(pair.getLeft(), errorMessage)).toList();
    handleScheduledFlushErrors(errors);
  }

  private List<Pair<RecordFleakData, Map<String, Object>>> swapBuffer() {
    List<Pair<RecordFleakData, Map<String, Object>>> snapshot = buffer;
    buffer = new ArrayList<>();
    return snapshot;
  }

  private SimpleSinkCommand.FlushResult flushBatchToDatabricks(
      List<Pair<RecordFleakData, Map<String, Object>>> batch) {
    if (batch.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    flushLock.lock();
    try {
      return doFlushBatch(batch);
    } finally {
      flushLock.unlock();
    }
  }

  private SimpleSinkCommand.FlushResult doFlushBatch(
      List<Pair<RecordFleakData, Map<String, Object>>> batch) {
    String batchId = UUID.randomUUID().toString();
    log.info("Starting flush of {} records with batchId: {}", batch.size(), batchId);

    List<ErrorOutput> allErrors;

    ParquetGenerationResult parquetResult;
    try {
      parquetResult = generateParquetWithErrorTracking(batch);
      allErrors = new ArrayList<>(parquetResult.errors);

      if (parquetResult.generatedFiles.isEmpty()) {
        log.error("No Parquet files generated, all {} records failed", batch.size());
        return new SimpleSinkCommand.FlushResult(0, 0, allErrors);
      }

      log.info(
          "Parquet generation: {} files created, {} records succeeded, {} failed",
          parquetResult.generatedFiles.size(),
          parquetResult.successfulRecordCount,
          parquetResult.errors.size());

    } catch (Exception e) {
      log.error("Critical error during Parquet generation", e);
      return createCompleteFailureResult(batch, "Parquet generation failed: " + e.getMessage());
    }

    UploadResult uploadResult;
    try {
      uploadResult = uploadFilesWithRetry(parquetResult.generatedFiles, batchId);

      if (!uploadResult.failedFiles.isEmpty()) {
        log.error(
            "Partial upload failure: {} of {} files failed. Aborting batch to prevent data gaps.",
            uploadResult.failedFiles.size(),
            parquetResult.generatedFiles.size());
        cleanupTempFiles(parquetResult.generatedFiles);
        cleanupRemoteBatchDirectory(batchId);
        return createCompleteFailureResult(batch, "Partial upload failure - aborting batch");
      }

      log.info(
          "Upload: {} files succeeded to batch subdirectory: {}",
          uploadResult.uploadedPaths.size(),
          batchId);

    } catch (Exception e) {
      log.error("Critical error during file upload", e);
      cleanupTempFiles(parquetResult.generatedFiles);
      cleanupRemoteBatchDirectory(batchId);
      return createCompleteFailureResult(batch, "Upload failed: " + e.getMessage());
    }

    CopyIntoResult copyResult;
    try {
      copyResult = executeCopyIntoWithErrorParsing(batchId);

      if (!copyResult.success) {
        log.error("COPY INTO failed: {}", copyResult.errorMessage);
        if (copyResult.recordsLoaded > 0) {
          log.warn("Partial success: {} records loaded before error", copyResult.recordsLoaded);
        }
      }

      log.info(
          "COPY INTO: {} records loaded into {}", copyResult.recordsLoaded, config.getTableName());

    } catch (Exception e) {
      log.error("Critical error during COPY INTO", e);
      return new SimpleSinkCommand.FlushResult(0, 0, allErrors);
    } finally {
      cleanupTempFiles(parquetResult.generatedFiles);
      cleanupRemoteBatchDirectory(batchId);
    }

    long totalFlushedBytes = parquetResult.generatedFiles.stream().mapToLong(File::length).sum();

    int successCount = (int) copyResult.recordsLoaded;

    if (copyResult.recordsLoaded != parquetResult.successfulRecordCount) {
      log.warn(
          "Metrics discrepancy: {} records written to Parquet, {} records loaded by COPY INTO",
          parquetResult.successfulRecordCount,
          copyResult.recordsLoaded);
    }

    log.info(
        "Flush completed: {} records succeeded, {} errors, {} bytes, batchId: {}",
        successCount,
        allErrors.size(),
        totalFlushedBytes,
        batchId);

    return new SimpleSinkCommand.FlushResult(successCount, totalFlushedBytes, allErrors);
  }

  private ParquetGenerationResult generateParquetWithErrorTracking(
      List<Pair<RecordFleakData, Map<String, Object>>> batch) {

    if (batch.isEmpty()) {
      return new ParquetGenerationResult(List.of(), List.of(), 0);
    }

    // Optimistic path: try writing entire batch
    List<Map<String, Object>> records = batch.stream().map(Pair::getRight).toList();
    try {
      List<File> files = parquetWriter.writeParquetFiles(records, tempDirectory);
      log.info("Generated {} Parquet files from {} records", files.size(), batch.size());
      return new ParquetGenerationResult(files, List.of(), batch.size());
    } catch (Exception e) {
      log.warn("Batch write failed, entering recovery mode: {}", e.getMessage());
      return filterAndBulkWrite(batch);
    }
  }

  private ParquetGenerationResult filterAndBulkWrite(
      List<Pair<RecordFleakData, Map<String, Object>>> batch) {

    List<ErrorOutput> errors = new ArrayList<>();
    List<Pair<RecordFleakData, Map<String, Object>>> goodRecords = new ArrayList<>();

    // Phase 1: Filter - identify good vs bad records by testing each individually
    for (int i = 0; i < batch.size(); i++) {
      Pair<RecordFleakData, Map<String, Object>> pair = batch.get(i);
      if (canWriteRecord(pair.getRight())) {
        goodRecords.add(pair);
      } else {
        log.warn("Record {} failed validation in recovery mode", i);
        errors.add(
            new ErrorOutput(pair.getLeft(), "Parquet conversion failed: record validation error"));
      }
    }

    if (goodRecords.isEmpty()) {
      log.warn("Recovery mode: all {} records failed validation", batch.size());
      return new ParquetGenerationResult(List.of(), errors, 0);
    }

    // Phase 2: Bulk-write all good records to a single Parquet file
    List<Map<String, Object>> goodData = goodRecords.stream().map(Pair::getRight).toList();
    try {
      List<File> files = parquetWriter.writeParquetFiles(goodData, tempDirectory);
      log.info(
          "Recovery mode completed: {} records succeeded, {} failed, {} files created",
          goodRecords.size(),
          errors.size(),
          files.size());
      return new ParquetGenerationResult(files, errors, goodRecords.size());
    } catch (Exception e) {
      log.error("Bulk write of validated records failed unexpectedly: {}", e.getMessage());
      // All records that passed validation still failed - add them to errors
      for (Pair<RecordFleakData, Map<String, Object>> pair : goodRecords) {
        errors.add(new ErrorOutput(pair.getLeft(), "Parquet bulk write failed: " + e.getMessage()));
      }
      return new ParquetGenerationResult(List.of(), errors, 0);
    }
  }

  private boolean canWriteRecord(Map<String, Object> record) {
    try (var ignored = DeltaLakeDataConverter.convertToColumnarBatch(List.of(record), schema)) {
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private UploadResult uploadFilesWithRetry(List<File> files, String batchId) {
    List<String> uploadedPaths = new ArrayList<>();
    List<File> failedFiles = new ArrayList<>();

    for (File file : files) {
      boolean uploaded = false;
      Exception lastException = null;

      for (int attempt = 0; attempt < MAX_UPLOAD_RETRIES; attempt++) {
        try {
          String remotePath = buildRemotePath(file, batchId);
          volumeUploader.uploadFile(file, remotePath);
          uploadedPaths.add(remotePath);
          uploaded = true;

          if (attempt > 0) {
            log.info("Upload succeeded on attempt {} for file: {}", attempt + 1, file.getName());
          }
          break;

        } catch (Exception e) {
          lastException = e;
          long delay = INITIAL_RETRY_DELAY_MS * (long) Math.pow(2, attempt);

          if (attempt < MAX_UPLOAD_RETRIES - 1) {
            log.warn(
                "Upload attempt {} failed for {}, retrying in {}ms: {}",
                attempt + 1,
                file.getName(),
                delay,
                e.getMessage());
            try {
              Thread.sleep(delay);
            } catch (InterruptedException ie) {
              Thread.currentThread().interrupt();
              break;
            }
          }
        }
      }

      if (!uploaded) {
        log.error(
            "Failed to upload {} after {} attempts: {}",
            file.getName(),
            MAX_UPLOAD_RETRIES,
            lastException.getMessage());
        failedFiles.add(file);
      }
    }

    return new UploadResult(uploadedPaths, failedFiles);
  }

  private String buildRemotePath(File file, String batchId) {
    String volumePath = config.getVolumePath();
    if (!volumePath.endsWith("/")) {
      volumePath += "/";
    }
    return volumePath + batchId + "/" + file.getName();
  }

  private String buildBatchDirectoryPath(String batchId) {
    String volumePath = config.getVolumePath();
    if (!volumePath.endsWith("/")) {
      volumePath += "/";
    }
    return volumePath + batchId;
  }

  private CopyIntoResult executeCopyIntoWithErrorParsing(String batchId) {
    String batchDirectory = buildBatchDirectoryPath(batchId);
    String volumePattern = batchDirectory + "/*.parquet";

    try {
      CopyIntoStats stats =
          sqlExecutor.executeCopyIntoWithStats(
              config.getTableName(),
              volumePattern,
              config.getCopyOptions(),
              config.getFormatOptions());

      if (stats.hasErrors()) {
        log.warn(
            "COPY INTO completed with errors: {} files processed, {} files loaded, {} errors",
            stats.filesProcessed(),
            stats.filesLoaded(),
            stats.errorMessages().size());

        stats.errorMessages().forEach(msg -> log.warn("COPY INTO error: {}", msg));
      }

      return new CopyIntoResult(true, stats.rowsLoaded(), null);

    } catch (Exception e) {
      log.error("COPY INTO failed", e);
      return new CopyIntoResult(false, 0, e.getMessage());
    }
  }

  private void cleanupRemoteBatchDirectory(String batchId) {
    if (!config.isCleanupAfterCopy()) {
      log.debug("Cleanup disabled, keeping remote batch directory: {}", batchId);
      return;
    }

    try {
      String batchDirectory = buildBatchDirectoryPath(batchId);
      volumeUploader.deleteDirectory(batchDirectory);
      log.debug("Cleaned up remote batch directory: {}", batchDirectory);
    } catch (Exception e) {
      log.warn("Failed to cleanup remote batch directory {}: {}", batchId, e.getMessage());
    }
  }

  private void cleanupTempFiles(List<File> files) {
    if (!config.isCleanupAfterCopy()) {
      log.debug("Cleanup disabled, keeping temp files");
      return;
    }

    int deletedCount = 0;
    int failedCount = 0;

    for (File file : files) {
      try {
        if (file.delete()) {
          deletedCount++;
        } else {
          log.warn("Failed to delete temp file (file.delete() returned false): {}", file);
          failedCount++;
        }
      } catch (Exception e) {
        log.warn("Error deleting temp file {}: {}", file, e.getMessage());
        failedCount++;
      }
    }

    log.debug("Cleanup: {} files deleted, {} failed", deletedCount, failedCount);
  }

  private SimpleSinkCommand.FlushResult createCompleteFailureResult(
      List<Pair<RecordFleakData, Map<String, Object>>> rawBatch, String errorMessage) {

    List<ErrorOutput> errors =
        rawBatch.stream().map(pair -> new ErrorOutput(pair.getLeft(), errorMessage)).toList();

    return new SimpleSinkCommand.FlushResult(0, 0, errors);
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }

    log.info("Closing BatchDatabricksFlusher...");
    closed = true;

    if (scheduledFuture != null) {
      scheduledFuture.cancel(false);
    }

    List<Pair<RecordFleakData, Map<String, Object>>> snapshot = null;
    synchronized (bufferLock) {
      if (!buffer.isEmpty()) {
        log.info("Flushing {} remaining records during close", buffer.size());
        snapshot = swapBuffer();
      }
    }

    if (snapshot != null) {
      flushBatchToDatabricks(snapshot);
    }

    if (Files.exists(tempDirectory)) {
      try (var paths = Files.walk(tempDirectory)) {
        paths
            .sorted(Comparator.reverseOrder())
            .forEach(
                path -> {
                  try {
                    Files.delete(path);
                  } catch (IOException e) {
                    log.warn("Failed to delete {}", path, e);
                  }
                });
      }
    }

    if (dlqWriter != null) {
      dlqWriter.close();
    }

    log.info("BatchDatabricksFlusher closed successfully");
  }

  private record ParquetGenerationResult(
      List<File> generatedFiles, List<ErrorOutput> errors, int successfulRecordCount) {}

  private record UploadResult(List<String> uploadedPaths, List<File> failedFiles) {}

  private record CopyIntoResult(boolean success, long recordsLoaded, String errorMessage) {}
}
