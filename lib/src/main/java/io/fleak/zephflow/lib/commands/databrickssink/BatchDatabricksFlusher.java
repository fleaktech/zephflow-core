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
import com.databricks.sdk.service.sql.StatementState;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.delta.kernel.types.StructType;
import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.databrickssink.DatabricksSqlExecutor.CopyIntoStats;
import io.fleak.zephflow.lib.commands.deltalakesink.DeltaLakeDataConverter;
import io.fleak.zephflow.lib.commands.deltalakesink.InvalidRecordException;
import io.fleak.zephflow.lib.commands.sink.AbstractBufferedFlusher;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class BatchDatabricksFlusher extends AbstractBufferedFlusher<Map<String, Object>> {

  private static final int MAX_UPLOAD_RETRIES = 3;

  private final DatabricksSinkDto.Config config;
  private final DatabricksParquetWriter parquetWriter;
  private final DatabricksVolumeUploader volumeUploader;
  private final DatabricksSqlExecutor sqlExecutor;
  private final Path tempDirectory;
  private final StructType schema;

  private final ReentrantLock flushLock = new ReentrantLock();
  private volatile boolean closed = false;

  public BatchDatabricksFlusher(
      DatabricksSinkDto.Config config,
      WorkspaceClient workspaceClient,
      Path tempDirectory,
      DlqWriter dlqWriter,
      JobContext jobContext,
      @NonNull FleakCounter sinkOutputCounter,
      @NonNull FleakCounter outputSizeCounter,
      @NonNull FleakCounter sinkErrorCounter,
      String nodeId) {
    super(dlqWriter, jobContext, nodeId, sinkOutputCounter, outputSizeCounter, sinkErrorCounter);
    this.config = config;
    schema = AvroToDeltaSchemaConverter.parse(config.getAvroSchema());
    this.parquetWriter = new DatabricksParquetWriter(schema);
    this.volumeUploader = new DatabricksVolumeUploader(workspaceClient);
    this.sqlExecutor = new DatabricksSqlExecutor(workspaceClient, config.getWarehouseId());
    this.tempDirectory = tempDirectory;

    log.info(
        "BatchDatabricksFlusher initialized: batchSize={}, flushInterval={}ms, table={}",
        config.getBatchSize(),
        config.getFlushIntervalMillis(),
        config.getTableName());
  }

  // Package-private constructor for testing with injected dependencies (no timer, no test mode)
  @VisibleForTesting
  BatchDatabricksFlusher(
      DatabricksSinkDto.Config config,
      DatabricksParquetWriter parquetWriter,
      DatabricksVolumeUploader volumeUploader,
      DatabricksSqlExecutor sqlExecutor,
      Path tempDirectory,
      DlqWriter dlqWriter,
      StructType schema,
      @NonNull FleakCounter sinkOutputCounter,
      @NonNull FleakCounter outputSizeCounter,
      @NonNull FleakCounter sinkErrorCounter,
      String nodeId) {
    super(dlqWriter, null, nodeId, sinkOutputCounter, outputSizeCounter, sinkErrorCounter);
    this.config = config;
    this.parquetWriter = parquetWriter;
    this.volumeUploader = volumeUploader;
    this.sqlExecutor = sqlExecutor;
    this.tempDirectory = tempDirectory;
    this.schema = schema;
    // Note: Timer not started for test constructor - tests control flushing manually
  }

  // ===== ABSTRACT METHOD IMPLEMENTATIONS =====

  @Override
  protected int getBatchSize() {
    return config.getBatchSize();
  }

  @Override
  protected SimpleSinkCommand.FlushResult doFlush(
      List<Pair<RecordFleakData, Map<String, Object>>> batch) {
    return doFlushWithRecovery(batch);
  }

  @Override
  protected void ensureCanWriteRecord(Map<String, Object> record) throws Exception {
    Preconditions.checkNotNull(record, "Record is null");
    //noinspection EmptyTryBlock
    try (var ignored = DeltaLakeDataConverter.convertToColumnarBatch(List.of(record), schema)) {
      // noop
    }
  }

  // ===== HOOK METHOD IMPLEMENTATIONS =====

  @Override
  protected void beforeFlush() {
    if (closed) {
      throw new IllegalStateException("Flusher is closed");
    }
  }

  @Override
  protected void beforeWrite() {
    flushLock.lock();
  }

  @Override
  protected void afterWrite() {
    flushLock.unlock();
  }

  // ===== TIMER CONFIGURATION =====

  @Override
  protected long getFlushIntervalMs() {
    return config.getFlushIntervalMillis();
  }

  @Override
  protected String getSchedulerThreadName() {
    return "BatchDatabricksFlusher-Flush";
  }

  @Override
  protected SimpleSinkCommand.FlushResult doFlushWithRecovery(
      List<Pair<RecordFleakData, Map<String, Object>>> batch) {
    List<IndexedRecord> records = new ArrayList<>(batch.size());
    for (int index = 0; index < batch.size(); index++) {
      records.add(new IndexedRecord(index, batch.get(index)));
    }
    FlushAccumulator accumulator = new FlushAccumulator(batch.size());
    if (!writeAndDeliver(records, UUID.randomUUID().toString(), accumulator)) {
      accumulator.fail(records, "Delivery not attempted after an operational failure");
    }
    return accumulator.result();
  }

  private boolean writeAndDeliver(
      List<IndexedRecord> records, String batchId, FlushAccumulator accumulator) {
    if (records.isEmpty()) {
      return true;
    }
    List<PreparedFiles> prepared = new ArrayList<>();
    try {
      generateParquet(records, prepared, accumulator);
      List<IndexedRecord> validRecords =
          prepared.stream().flatMap(files -> files.records().stream()).toList();
      return validRecords.isEmpty()
          || deliverPrepared(validRecords, prepared, batchId, accumulator);
    } catch (Exception failure) {
      log.error("Parquet generation failed for batch {}", batchId, failure);
      accumulator.fail(records, "Parquet generation failed: " + failure.getMessage());
      return false;
    } finally {
      cleanupPreparedFiles(prepared);
    }
  }

  private void generateParquet(
      List<IndexedRecord> records, List<PreparedFiles> prepared, FlushAccumulator accumulator)
      throws Exception {
    Files.createDirectories(tempDirectory);
    Path attemptDirectory = Files.createTempDirectory(tempDirectory, "attempt-");
    try {
      List<Map<String, Object>> values =
          records.stream().map(record -> record.pair().getRight()).toList();
      List<File> files = List.copyOf(parquetWriter.writeParquetFiles(values, attemptDirectory));
      if (files.isEmpty()) {
        throw new IOException("Parquet writer produced no files for a nonempty group");
      }
      long bytes = 0;
      for (File file : files) {
        bytes += Files.size(file.toPath());
      }
      prepared.add(new PreparedFiles(attemptDirectory, files, records, bytes));
      return;
    } catch (Exception failure) {
      try {
        deleteLocalDirectory(attemptDirectory);
      } catch (Exception cleanupFailure) {
        failure.addSuppressed(cleanupFailure);
      }
      if (!isRecordFailure(failure)) {
        throw failure;
      }
      if (records.size() == 1) {
        accumulator.fail(records, "Parquet conversion failed: " + failure.getMessage());
        return;
      }
    }
    int midpoint = records.size() / 2;
    generateParquet(records.subList(0, midpoint), prepared, accumulator);
    generateParquet(records.subList(midpoint, records.size()), prepared, accumulator);
  }

  private static boolean isRecordFailure(Throwable failure) {
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    Deque<Throwable> pending = new ArrayDeque<>();
    pending.add(failure);
    boolean invalidRecord = false;
    while (!pending.isEmpty()) {
      Throwable current = pending.removeFirst();
      if (!seen.add(current)) {
        continue;
      }
      if (current instanceof IOException || current instanceof java.io.UncheckedIOException) {
        return false;
      }
      invalidRecord |= current instanceof InvalidRecordException;
      if (current.getCause() != null) {
        pending.add(current.getCause());
      }
      Collections.addAll(pending, current.getSuppressed());
    }
    return invalidRecord;
  }

  private boolean deliverPrepared(
      List<IndexedRecord> records,
      List<PreparedFiles> prepared,
      String batchId,
      FlushAccumulator accumulator) {
    String attemptId = batchId + "-" + UUID.randomUUID();
    AttemptPhase phase = AttemptPhase.UPLOAD;
    boolean preserveRemoteFiles = false;
    String rejection;
    try {
      List<File> files = prepared.stream().flatMap(group -> group.files().stream()).toList();
      UploadResult upload = uploadFilesWithRetry(files, attemptId);
      if (!upload.failedFiles().isEmpty()) {
        accumulator.fail(records, "Databricks upload failed: " + upload.lastErrorMessage());
        return false;
      }
      phase = AttemptPhase.VALIDATE;
      sqlExecutor.validateCopyInto(
          config.getTableName(),
          buildBatchDirectoryPath(attemptId) + "/*.parquet",
          config.getCopyOptions(),
          config.getFormatOptions());
      phase = AttemptPhase.COPY;
      CopyIntoStats stats =
          sqlExecutor.executeCopyIntoWithStats(
              config.getTableName(),
              buildBatchDirectoryPath(attemptId) + "/*.parquet",
              config.getCopyOptions(),
              config.getFormatOptions());
      long bytes = prepared.stream().mapToLong(PreparedFiles::bytes).sum();
      accumulator.commit(records, stats, bytes);
      if (!stats.rowsLoadedKnown()) {
        log.warn(
            "COPY INTO committed for batch {}, attempt {}, but row count is unavailable",
            batchId,
            attemptId);
      }
      return true;
    } catch (DatabricksSqlExecutor.StatementExecutionException failure) {
      if (phase != AttemptPhase.VALIDATE
          || failure.state() != StatementState.FAILED
          || failure.outcomeUnknown()) {
        preserveRemoteFiles = phase == AttemptPhase.COPY;
        accumulator.fail(records, failureMessagePrefix(phase) + failure.getMessage());
        if (preserveRemoteFiles) {
          log.warn(
              "Preserving COPY source: batch={}, attempt={}, statement={}, path={}",
              batchId,
              attemptId,
              failure.statementId(),
              buildBatchDirectoryPath(attemptId));
        }
        return false;
      }
      rejection = failure.getMessage();
    } catch (Exception failure) {
      preserveRemoteFiles = phase == AttemptPhase.COPY;
      accumulator.fail(records, failureMessagePrefix(phase) + failure.getMessage());
      if (preserveRemoteFiles) {
        log.warn(
            "Preserving COPY source after unknown outcome: batch={}, attempt={}, path={}",
            batchId,
            attemptId,
            buildBatchDirectoryPath(attemptId));
      }
      return false;
    } finally {
      cleanupPreparedFiles(prepared);
      if (!preserveRemoteFiles) {
        cleanupRemoteBatchDirectory(attemptId);
      }
    }

    if (records.size() == 1) {
      accumulator.fail(records, "COPY INTO validation rejected record: " + rejection);
      return true;
    }
    int midpoint = records.size() / 2;
    return writeAndDeliver(records.subList(0, midpoint), batchId, accumulator)
        && writeAndDeliver(records.subList(midpoint, records.size()), batchId, accumulator);
  }

  private enum AttemptPhase {
    UPLOAD,
    VALIDATE,
    COPY
  }

  private static String failureMessagePrefix(AttemptPhase phase) {
    return switch (phase) {
      case UPLOAD -> "Databricks upload failed: ";
      case VALIDATE -> "COPY INTO validation failed: ";
      case COPY -> "COPY INTO delivery outcome unknown: ";
    };
  }

  private void cleanupPreparedFiles(List<PreparedFiles> prepared) {
    if (!config.isCleanupAfterCopy()) {
      return;
    }
    for (PreparedFiles files : prepared) {
      try {
        deleteLocalDirectory(files.directory());
      } catch (Exception failure) {
        log.warn("Failed to clean local attempt directory {}", files.directory(), failure);
      }
    }
  }

  private static void deleteLocalDirectory(Path directory) throws IOException {
    if (!Files.exists(directory)) {
      return;
    }
    try (var paths = Files.walk(directory)) {
      for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
        Files.deleteIfExists(path);
      }
    }
  }

  private UploadResult uploadFilesWithRetry(List<File> files, String batchId) {
    List<String> uploadedPaths = new ArrayList<>();
    List<File> failedFiles = new ArrayList<>();
    String lastErrorMessage = null;

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
        lastErrorMessage = lastException.getMessage();
      }
    }

    return new UploadResult(uploadedPaths, failedFiles, lastErrorMessage);
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

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;

    log.info("Closing BatchDatabricksFlusher...");
    stopFlushTimer();

    List<Pair<RecordFleakData, Map<String, Object>>> snapshot = swapBufferIfNotEmpty();

    if (snapshot != null) {
      log.info("Flushing {} remaining records during close", snapshot.size());
      SimpleSinkCommand.FlushResult result = executeFlushOutOfBand(snapshot, Map.of());
      if (!result.errorOutputList().isEmpty()) {
        reportErrorMetrics(result.errorOutputList().size(), Map.of());
        handleScheduledFlushErrors(result.errorOutputList());
      }
    }

    // Acquire flushLock to ensure any in-progress scheduled flush completes
    // before we clean up temp files it may be using
    flushLock.lock();
    try {
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
    } finally {
      flushLock.unlock();
    }

    if (dlqWriter != null) {
      dlqWriter.close();
    }

    log.info("BatchDatabricksFlusher closed successfully");
  }

  private record IndexedRecord(int index, Pair<RecordFleakData, Map<String, Object>> pair) {}

  private record PreparedFiles(
      Path directory, List<File> files, List<IndexedRecord> records, long bytes) {}

  private static final class FlushAccumulator {
    private final boolean[] completed;
    private final ErrorOutput[] errors;
    private long rowsLoaded;
    private long bytes;

    private FlushAccumulator(int size) {
      completed = new boolean[size];
      errors = new ErrorOutput[size];
    }

    private void fail(List<IndexedRecord> records, String message) {
      for (IndexedRecord record : records) {
        if (!completed[record.index()]) {
          completed[record.index()] = true;
          errors[record.index()] = new ErrorOutput(record.pair().getLeft(), message);
        }
      }
    }

    private void commit(List<IndexedRecord> records, CopyIntoStats stats, long writtenBytes) {
      for (IndexedRecord record : records) {
        completed[record.index()] = true;
      }
      if (stats.rowsLoadedKnown()) {
        rowsLoaded += stats.rowsLoaded();
      }
      bytes += writtenBytes;
    }

    private SimpleSinkCommand.FlushResult result() {
      return new SimpleSinkCommand.FlushResult(
          (int) rowsLoaded, bytes, Arrays.stream(errors).filter(Objects::nonNull).toList());
    }
  }

  private record UploadResult(
      List<String> uploadedPaths, List<File> failedFiles, String lastErrorMessage) {}
}
