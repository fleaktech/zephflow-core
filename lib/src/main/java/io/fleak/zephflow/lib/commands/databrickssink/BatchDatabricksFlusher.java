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
import io.fleak.zephflow.api.execution.EffectOutcome;
import io.fleak.zephflow.api.execution.ExecutionStoppedException;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.databrickssink.DatabricksSqlExecutor.CopyIntoStats;
import io.fleak.zephflow.lib.commands.deltalakesink.DeltaLakeDataConverter;
import io.fleak.zephflow.lib.commands.deltalakesink.InvalidRecordException;
import io.fleak.zephflow.lib.commands.sink.AbstractBufferedFlusher;
import io.fleak.zephflow.lib.commands.sink.BoundedFlushException;
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
    this.volumeUploader = new DatabricksVolumeUploader(workspaceClient, boundedHooks != null);
    this.sqlExecutor =
        new DatabricksSqlExecutor(workspaceClient, config.getWarehouseId(), boundedHooks != null);
    this.tempDirectory = tempDirectory;

    if (boundedHooks == null)
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
    this(
        config,
        parquetWriter,
        volumeUploader,
        sqlExecutor,
        tempDirectory,
        dlqWriter,
        schema,
        sinkOutputCounter,
        outputSizeCounter,
        sinkErrorCounter,
        nodeId,
        null);
  }

  @VisibleForTesting
  BatchDatabricksFlusher(
      DatabricksSinkDto.Config config,
      DatabricksParquetWriter parquetWriter,
      DatabricksVolumeUploader volumeUploader,
      DatabricksSqlExecutor sqlExecutor,
      Path tempDirectory,
      DlqWriter dlqWriter,
      StructType schema,
      FleakCounter sinkOutputCounter,
      FleakCounter outputSizeCounter,
      FleakCounter sinkErrorCounter,
      String nodeId,
      JobContext jobContext) {
    super(dlqWriter, jobContext, nodeId, sinkOutputCounter, outputSizeCounter, sinkErrorCounter);
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
    FlushAccumulator accumulator = new FlushAccumulator(batch.size(), boundedHooks != null);
    try {
      if (!writeAndDeliver(records, UUID.randomUUID().toString(), accumulator)) {
        accumulator.fail(records, "Delivery not attempted after an operational failure");
      }
    } catch (ExecutionStoppedException stopped) {
      throw new BoundedFlushException(accumulator.result(), stopped);
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
    } catch (ExecutionStoppedException stopped) {
      throw stopped;
    } catch (Exception failure) {
      if (boundedHooks != null) log.error("Bounded Databricks Parquet generation failed");
      else log.error("Parquet generation failed for batch {}", batchId, failure);
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
        accumulator.reject(records, "Parquet conversion failed: " + failure.getMessage());
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
    if (boundedHooks != null) boundedHooks.control().checkpoint();
    String attemptId = batchId + "-" + UUID.randomUUID();
    AttemptPhase phase = AttemptPhase.UPLOAD;
    boolean preserveRemoteFiles = false;
    String rejection;
    try {
      List<File> files = prepared.stream().flatMap(group -> group.files().stream()).toList();
      UploadResult upload =
          uploadFilesWithRetry(files, attemptId, () -> accumulator.attempt(records));
      if (!upload.failedFiles().isEmpty()) {
        accumulator.fail(records, "Databricks upload failed: " + upload.lastErrorMessage());
        return false;
      }
      phase = AttemptPhase.VALIDATE;
      if (boundedHooks != null) boundedHooks.control().checkpoint();
      sqlExecutor.validateCopyInto(
          config.getTableName(),
          buildBatchDirectoryPath(attemptId) + "/*.parquet",
          config.getCopyOptions(),
          config.getFormatOptions());
      phase = AttemptPhase.COPY;
      if (boundedHooks != null) boundedHooks.control().checkpoint();
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
          if (boundedHooks != null)
            log.warn("Bounded Databricks COPY source retained after uncertain outcome");
          else
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
    } catch (ExecutionStoppedException stopped) {
      preserveRemoteFiles = phase == AttemptPhase.COPY;
      throw stopped;
    } catch (Exception failure) {
      preserveRemoteFiles = phase == AttemptPhase.COPY;
      accumulator.fail(records, failureMessagePrefix(phase) + failure.getMessage());
      if (preserveRemoteFiles) {
        if (boundedHooks != null)
          log.warn("Bounded Databricks COPY source retained after uncertain outcome");
        else
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
      accumulator.reject(records, "COPY INTO validation rejected record: " + rejection);
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
        if (boundedHooks != null) log.warn("Bounded Databricks local cleanup failed");
        else log.warn("Failed to clean local attempt directory {}", files.directory(), failure);
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

  private UploadResult uploadFilesWithRetry(
      List<File> files, String batchId, Runnable beforeUpload) {
    List<String> uploadedPaths = new ArrayList<>();
    List<File> failedFiles = new ArrayList<>();
    String lastErrorMessage = null;

    for (File file : files) {
      boolean uploaded = false;
      Exception lastException = null;
      int attemptsMade = 0;

      for (int attempt = 0; attempt < MAX_UPLOAD_RETRIES; attempt++) {
        if (boundedHooks != null) boundedHooks.control().checkpoint();
        attemptsMade++;
        try {
          String remotePath = buildRemotePath(file, batchId);
          beforeUpload.run();
          volumeUploader.uploadFile(file, remotePath);
          uploadedPaths.add(remotePath);
          uploaded = true;

          if (attempt > 0) {
            log.info("Upload succeeded on attempt {} for file: {}", attempt + 1, file.getName());
          }
          break;

        } catch (Exception e) {
          lastException = e;
          if (boundedHooks != null) break;
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
        if (boundedHooks != null) log.error("Bounded Databricks upload failed");
        else
          log.error(
              "Failed to upload {} after {} attempts: {}",
              file.getName(),
              attemptsMade,
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
      if (boundedHooks == null) log.debug("Cleaned up remote batch directory: {}", batchDirectory);
    } catch (Exception e) {
      if (boundedHooks != null) log.warn("Bounded Databricks remote cleanup failed");
      else log.warn("Failed to cleanup remote batch directory {}: {}", batchId, e.getMessage());
    }
  }

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;

    log.info("Closing BatchDatabricksFlusher...");
    if (boundedHooks != null) discardPendingRecords();
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
                      if (boundedHooks != null) log.warn("Bounded Databricks local cleanup failed");
                      else log.warn("Failed to delete {}", path, e);
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
    private final boolean bounded;
    private final boolean[] attempted;
    private long rejected;
    private long attemptedRejected;
    private boolean rowsLoadedKnown = true;
    private long rowsLoaded;
    private long bytes;

    private FlushAccumulator(int size, boolean bounded) {
      completed = new boolean[size];
      errors = new ErrorOutput[size];
      attempted = new boolean[size];
      this.bounded = bounded;
    }

    private void attempt(List<IndexedRecord> records) {
      records.forEach(record -> attempted[record.index()] = true);
    }

    private void reject(List<IndexedRecord> records, String message) {
      for (var record : records) {
        if (!completed[record.index()]) {
          rejected++;
          if (attempted[record.index()]) attemptedRejected++;
        }
      }
      fail(records, message);
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
      } else {
        rowsLoadedKnown = false;
      }
      bytes += writtenBytes;
    }

    private SimpleSinkCommand.FlushResult result() {
      var retainedErrors = Arrays.stream(errors).filter(Objects::nonNull).toList();
      if (!bounded)
        return new SimpleSinkCommand.FlushResult((int) rowsLoaded, bytes, retainedErrors);
      long attemptedCount = 0;
      for (int index = 0; index < attempted.length; index++) {
        if (attempted[index]) {
          attemptedCount++;
        }
      }
      long notAttempted = attempted.length - attemptedCount;
      // Conversion failures are definite and unsent. Other failed sends remain uncertain.
      EffectOutcome outcome =
          rowsLoadedKnown
              ? EffectOutcome.counted(
                  attemptedCount,
                  rowsLoaded,
                  rejected,
                  Math.max(0, attemptedCount - rowsLoaded - attemptedRejected),
                  notAttempted,
                  "copy_into_rows")
              : new EffectOutcome(
                  attemptedCount,
                  null,
                  rejected,
                  null,
                  notAttempted,
                  "copy_into_rows_unavailable",
                  rowsLoaded > 0 ? EffectOutcome.Delivery.PARTIAL : EffectOutcome.Delivery.UNKNOWN);
      return new SimpleSinkCommand.FlushResult((int) rowsLoaded, bytes, retainedErrors, outcome);
    }
  }

  private record UploadResult(
      List<String> uploadedPaths, List<File> failedFiles, String lastErrorMessage) {}
}
