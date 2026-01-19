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
package io.fleak.zephflow.lib.commands.sink;

import static io.fleak.zephflow.lib.utils.MiscUtils.threadSleep;

import com.google.common.annotations.VisibleForTesting;
import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.utils.BufferedWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Base class for buffered sink flushers that provides common functionality for buffer management,
 * record-level recovery, retry logic, and DLQ handling.
 *
 * <p>This class implements the Template Method pattern where {@link #flush} is the template method
 * that orchestrates buffering and flushing, and subclasses provide the actual write logic via
 * {@link #doFlush}.
 *
 * @param <T> The type of prepared data (e.g., Map&lt;String, Object&gt;)
 */
@Slf4j
public abstract class AbstractBufferedFlusher<T> implements SimpleSinkCommand.Flusher<T> {

  protected static final int MAX_WRITE_RETRIES = 3;
  protected static final long INITIAL_RETRY_DELAY_MS = 1000;

  protected final DlqWriter dlqWriter;
  protected final RecordFleakDataEncoder recordEncoder = new RecordFleakDataEncoder();
  protected final boolean testMode;

  private BufferedWriter<Pair<RecordFleakData, T>> bufferedWriter;

  protected AbstractBufferedFlusher(DlqWriter dlqWriter, JobContext jobContext) {
    this.dlqWriter = dlqWriter;
    this.testMode =
        jobContext != null
            && Boolean.TRUE.equals(jobContext.getOtherProperties().get(JobContext.FLAG_TEST_MODE));
    // BufferedWriter will be initialized lazily on first use to allow subclass to set batchSize
  }

  protected final boolean isSyncMode() {
    return testMode;
  }

  private synchronized void ensureBufferedWriterInitialized() {
    if (bufferedWriter == null) {
      bufferedWriter =
          new BufferedWriter<>(
              getBatchSize(),
              0, // No timer for lazy initialization; timer is started in initialize()
              this::handleScheduledFlushCallback,
              getSchedulerThreadName(),
              testMode);
    }
  }

  // ===== ABSTRACT METHODS (Subclasses must implement) =====

  /**
   * Returns the batch size threshold for triggering a flush.
   *
   * @return the batch size
   */
  protected abstract int getBatchSize();

  /**
   * Performs the actual write operation for a batch of records. Called outside buffer lock but
   * inside write lock (if beforeWrite/afterWrite acquire one).
   *
   * @param batch The batch to write (includes original records for error reporting)
   * @return The flush result
   * @throws Exception if write fails
   */
  protected abstract SimpleSinkCommand.FlushResult doFlush(List<Pair<RecordFleakData, T>> batch)
      throws Exception;

  /**
   * Validates if a record can be written.
   *
   * @param record The record to validate
   */
  protected abstract void ensureCanWriteRecord(T record) throws Exception;

  // ===== HOOK METHODS (Subclasses can override) =====

  /** Called before flush processing. Override to add state checks (e.g., initialized, closed). */
  protected void beforeFlush() {}

  /** Called before actual write operation. Override to acquire additional locks. */
  protected void beforeWrite() {}

  /** Called after write operation completes (success or failure). Override to release locks. */
  protected void afterWrite() {}

  /**
   * Called to report success metrics after flush. Override for direct metrics reporting.
   *
   * @param result The flush result
   * @param metricTags Tags for metrics
   */
  protected void reportMetrics(
      SimpleSinkCommand.FlushResult result, Map<String, String> metricTags) {}

  /**
   * Called to report error metrics (for scheduled/close flushes where SimpleSinkCommand isn't
   * involved).
   *
   * @param errorCount Number of errors
   * @param metricTags Tags for metrics
   */
  protected void reportErrorMetrics(int errorCount, Map<String, String> metricTags) {}

  /**
   * Returns the flush interval in milliseconds. Override to enable timer-based flushing. Return 0
   * or negative to disable.
   */
  protected long getFlushIntervalMs() {
    return 0;
  }

  /** Returns the thread name for the flush scheduler. Override to customize. */
  protected String getSchedulerThreadName() {
    return "BufferedFlusher-Flush";
  }

  // ===== LIFECYCLE =====

  /**
   * Initializes the flusher, including starting the timer-based flush scheduler if configured.
   * Subclasses should call super.initialize() at the end of their own initialization.
   */
  public void initialize() {
    // Create BufferedWriter with timer support
    this.bufferedWriter =
        new BufferedWriter<>(
            getBatchSize(),
            getFlushIntervalMs(),
            this::handleScheduledFlushCallback,
            getSchedulerThreadName(),
            testMode);
    bufferedWriter.start();
  }

  private void handleScheduledFlushCallback(List<Pair<RecordFleakData, T>> batch) {
    executeScheduledFlushInternal(batch);
  }

  private void executeScheduledFlushInternal(List<Pair<RecordFleakData, T>> batch) {
    try {
      SimpleSinkCommand.FlushResult result = executeFlush(batch, Map.of());
      if (!result.errorOutputList().isEmpty()) {
        reportErrorMetrics(result.errorOutputList().size(), Map.of());
        handleScheduledFlushErrors(result.errorOutputList());
      }
    } catch (Exception e) {
      log.error("Error during scheduled flush", e);
      reportErrorMetrics(batch.size(), Map.of());
      handleScheduledFlushErrors(batch, e.getMessage());
    }
  }

  /**
   * Executes a scheduled flush manually. Used for testing. This method flushes the current buffer
   * using the scheduled flush logic.
   */
  @VisibleForTesting
  public void executeScheduledFlush() {
    ensureBufferedWriterInitialized();
    List<Pair<RecordFleakData, T>> batch = bufferedWriter.flushAndGet();
    if (!batch.isEmpty()) {
      executeScheduledFlushInternal(batch);
    }
  }

  /** Stops the timer-based flush scheduler. Call from subclass close(). */
  protected final synchronized void stopFlushTimer() {
    if (bufferedWriter != null) {
      bufferedWriter.close();
    }
  }

  // ===== TEMPLATE METHOD =====

  /**
   * Template method that handles buffering and triggers flush when batch size is reached. This is
   * the main entry point called by SimpleSinkCommand.
   *
   * @param preparedInputEvents The events to buffer/flush
   * @param metricTags Tags for metrics reporting
   * @return FlushResult with success count, size, and errors
   * @throws Exception if flush fails completely
   */
  @Override
  public final SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<T> preparedInputEvents, Map<String, String> metricTags)
      throws Exception {

    beforeFlush();

    if (preparedInputEvents.preparedList().isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    // Ensure BufferedWriter is initialized (lazy initialization for tests that don't call init)
    ensureBufferedWriterInitialized();

    // Add to buffer without auto-flush (we control when to flush)
    bufferedWriter.write(preparedInputEvents.rawAndPreparedList(), false);

    log.debug(
        "Added {} records to buffer. Buffer: {} / {}",
        preparedInputEvents.rawAndPreparedList().size(),
        bufferedWriter.getBufferSize(),
        getBatchSize());

    // Check if we should flush (sync mode or batch size reached)
    if (bufferedWriter.shouldFlush()) {
      log.info(
          "Triggering flush: syncMode={}, bufferSize={}, batchSize={}",
          isSyncMode(),
          bufferedWriter.getBufferSize(),
          getBatchSize());
      List<Pair<RecordFleakData, T>> snapshot = bufferedWriter.flushAndGet();
      if (!snapshot.isEmpty()) {
        return executeFlush(snapshot, metricTags);
      }
    }

    return new SimpleSinkCommand.FlushResult(0, 0, List.of());
  }

  /**
   * Executes a flush with proper lock handling and metrics reporting.
   *
   * @param batch The batch to flush
   * @param metricTags Tags for metrics
   * @return The flush result
   */
  protected final SimpleSinkCommand.FlushResult executeFlush(
      List<Pair<RecordFleakData, T>> batch, Map<String, String> metricTags) {
    if (batch.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    log.info("Flushing {} records", batch.size());

    beforeWrite();
    try {
      SimpleSinkCommand.FlushResult result = doFlushWithRecovery(batch);
      reportMetrics(result, metricTags);
      return result;
    } finally {
      afterWrite();
    }
  }

  /**
   * Swaps buffer and returns snapshot if not empty. Use for close-time flushing.
   *
   * @return The buffer snapshot, or empty list if buffer is empty
   */
  protected final List<Pair<RecordFleakData, T>> swapBufferIfNotEmpty() {
    synchronized (this) {
      if (bufferedWriter == null) {
        return List.of();
      }
    }
    return bufferedWriter.flushAndGet();
  }

  /**
   * Returns the current buffer size. Useful for testing.
   *
   * @return The number of items in the buffer
   */
  @VisibleForTesting
  protected final int getBufferSize() {
    synchronized (this) {
      if (bufferedWriter == null) {
        return 0;
      }
    }
    return bufferedWriter.getBufferSize();
  }

  // ===== RECOVERY LOGIC =====

  /**
   * Attempts to flush with recovery. First tries direct flush; if that fails, filters out bad
   * records and flushes the good ones. Subclasses can override for custom recovery logic.
   *
   * @param batch The batch to flush
   * @return The flush result
   */
  protected SimpleSinkCommand.FlushResult doFlushWithRecovery(
      List<Pair<RecordFleakData, T>> batch) {
    try {
      return doFlushWithRetry(batch);
    } catch (Exception e) {
      log.warn("Batch write failed, entering recovery mode: {}", e.getMessage());
      return filterAndFlush(batch);
    }
  }

  /**
   * Flushes data with retry logic using exponential backoff.
   *
   * @param batch The batch to flush
   * @return The flush result
   * @throws Exception if all retries are exhausted
   */
  protected SimpleSinkCommand.FlushResult doFlushWithRetry(List<Pair<RecordFleakData, T>> batch)
      throws Exception {
    long retryDelayMs = INITIAL_RETRY_DELAY_MS;
    int attempt = 0;

    while (true) {
      attempt++;
      try {
        return doFlush(batch);
      } catch (Exception e) {
        if (attempt >= MAX_WRITE_RETRIES) {
          throw e;
        }
        log.warn(
            "Error (attempt {}/{}), retrying in {}ms: {}",
            attempt,
            MAX_WRITE_RETRIES,
            retryDelayMs,
            e.getMessage());
        threadSleep(retryDelayMs);
        retryDelayMs *= 2;
      }
    }
  }

  /**
   * Filters out bad records and flushes the good ones.
   *
   * @param batch The batch to filter and flush
   * @return The flush result including errors for bad records
   */
  protected SimpleSinkCommand.FlushResult filterAndFlush(List<Pair<RecordFleakData, T>> batch) {
    List<ErrorOutput> errors = new ArrayList<>();
    List<Pair<RecordFleakData, T>> goodRecords = new ArrayList<>();

    for (int i = 0; i < batch.size(); i++) {
      Pair<RecordFleakData, T> pair = batch.get(i);
      try {
        ensureCanWriteRecord(pair.getRight());
        goodRecords.add(pair);
      } catch (Exception e) {
        log.warn("Record {} failed validation in recovery mode", i, e);
        errors.add(new ErrorOutput(pair.getLeft(), "Record validation failed: " + e.getMessage()));
      }
    }

    if (goodRecords.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, errors);
    }

    try {
      SimpleSinkCommand.FlushResult result = doFlushWithRetry(goodRecords);
      List<ErrorOutput> allErrors = new ArrayList<>(result.errorOutputList());
      allErrors.addAll(errors);
      return new SimpleSinkCommand.FlushResult(
          result.successCount(), result.flushedDataSize(), allErrors);
    } catch (Exception e) {
      log.error("Bulk write of validated records failed: {}", e.getMessage());
      for (Pair<RecordFleakData, T> pair : goodRecords) {
        errors.add(new ErrorOutput(pair.getLeft(), "Write failed: " + e.getMessage()));
      }
      return new SimpleSinkCommand.FlushResult(0, 0, errors);
    }
  }

  // ===== HELPER METHODS =====

  /**
   * Handles errors from scheduled (timer-based) flushes by writing to DLQ.
   *
   * @param errors The errors to handle
   */
  protected void handleScheduledFlushErrors(List<ErrorOutput> errors) {
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

  /**
   * Handles errors from scheduled flushes by converting snapshot to errors and writing to DLQ.
   *
   * @param snapshot The failed records
   * @param errorMessage The error message
   */
  protected void handleScheduledFlushErrors(
      List<Pair<RecordFleakData, T>> snapshot, String errorMessage) {
    List<ErrorOutput> errors =
        snapshot.stream().map(pair -> new ErrorOutput(pair.getLeft(), errorMessage)).toList();
    handleScheduledFlushErrors(errors);
  }

  /**
   * Creates a complete failure result for all records in a batch.
   *
   * @param batch The batch that failed
   * @param errorMessage The error message
   * @return A FlushResult with all records as errors
   */
  protected SimpleSinkCommand.FlushResult createCompleteFailureResult(
      List<Pair<RecordFleakData, T>> batch, String errorMessage) {
    List<ErrorOutput> errors =
        batch.stream().map(pair -> new ErrorOutput(pair.getLeft(), errorMessage)).toList();
    return new SimpleSinkCommand.FlushResult(0, 0, errors);
  }
}
