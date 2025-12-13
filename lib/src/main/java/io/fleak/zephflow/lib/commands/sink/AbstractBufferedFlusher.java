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

import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
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

  protected final Object bufferLock = new Object();
  protected List<Pair<RecordFleakData, T>> buffer = new ArrayList<>();

  protected final DlqWriter dlqWriter;
  protected final RecordFleakDataEncoder recordEncoder = new RecordFleakDataEncoder();
  protected final boolean testMode;

  private ScheduledExecutorService flushScheduler;
  private ScheduledFuture<?> flushTask;

  protected AbstractBufferedFlusher(DlqWriter dlqWriter, JobContext jobContext) {
    this.dlqWriter = dlqWriter;
    this.testMode =
        jobContext != null
            && Boolean.TRUE.equals(jobContext.getOtherProperties().get(JobContext.FLAG_TEST_MODE));
  }

  protected final boolean isSyncMode() {
    return testMode;
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
   * @return true if the record can be written
   */
  protected abstract boolean canWriteRecord(T record);

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
    startFlushTimer();
  }

  // ===== TIMER MANAGEMENT =====

  /**
   * Starts the timer-based flush scheduler. Does nothing if interval is 0 or negative, or if
   * already started.
   */
  private synchronized void startFlushTimer() {
    if (testMode) {
      log.debug("Test mode enabled, timer-based flushing disabled");
      return;
    }

    if (flushScheduler != null) {
      log.warn("Flush timer already running, skipping restart");
      return;
    }

    long intervalMs = getFlushIntervalMs();
    if (intervalMs <= 0) {
      log.debug("Timer-based flushing disabled (interval={})", intervalMs);
      return;
    }

    String threadName = getSchedulerThreadName();
    log.info("Starting timer-based flush with interval {}ms, thread: {}", intervalMs, threadName);

    flushScheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, threadName);
              t.setDaemon(true);
              return t;
            });

    flushTask =
        flushScheduler.scheduleWithFixedDelay(
            () -> {
              try {
                executeScheduledFlush();
              } catch (Exception e) {
                log.error("Error during timer-based flush", e);
              }
            },
            Math.min(intervalMs, 1000),
            intervalMs,
            TimeUnit.MILLISECONDS);

    log.info("Timer-based flush started successfully");
  }

  /** Stops the timer-based flush scheduler. Call from subclass close(). */
  protected final synchronized void stopFlushTimer() {
    if (flushTask != null) {
      log.debug("Cancelling flush task");
      flushTask.cancel(false);
      flushTask = null;
    }

    if (flushScheduler != null) {
      log.debug("Shutting down flush scheduler");
      flushScheduler.shutdown();
      try {
        if (!flushScheduler.awaitTermination(30, TimeUnit.SECONDS)) {
          log.warn("Flush scheduler did not terminate in time, forcing shutdown");
          flushScheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        log.warn("Interrupted while waiting for flush scheduler to terminate");
        flushScheduler.shutdownNow();
        Thread.currentThread().interrupt();
      }
      flushScheduler = null;
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

    List<Pair<RecordFleakData, T>> snapshot = null;

    synchronized (bufferLock) {
      buffer.addAll(preparedInputEvents.rawAndPreparedList());
      log.debug(
          "Added {} records to buffer. Buffer: {} / {}",
          preparedInputEvents.rawAndPreparedList().size(),
          buffer.size(),
          getBatchSize());

      if (isSyncMode() || buffer.size() >= getBatchSize()) {
        log.info(
            "Triggering flush: syncMode={}, bufferSize={}, batchSize={}",
            isSyncMode(),
            buffer.size(),
            getBatchSize());
        snapshot = swapBuffer();
      }
    }

    if (snapshot != null) {
      return executeFlush(snapshot, metricTags);
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
   * Executes a scheduled (timer-triggered) or close-time flush. Subclasses should call this from
   * their timer callback.
   */
  protected final void executeScheduledFlush() {
    List<Pair<RecordFleakData, T>> snapshot;

    synchronized (bufferLock) {
      if (buffer.isEmpty()) {
        log.trace("Scheduled flush skipped: buffer empty");
        return;
      }
      log.info("Scheduled flush triggered: {} records buffered", buffer.size());
      snapshot = swapBuffer();
    }

    try {
      SimpleSinkCommand.FlushResult result = executeFlush(snapshot, Map.of());
      if (!result.errorOutputList().isEmpty()) {
        reportErrorMetrics(result.errorOutputList().size(), Map.of());
        handleScheduledFlushErrors(result.errorOutputList());
      }
    } catch (Exception e) {
      log.error("Error during scheduled flush", e);
      reportErrorMetrics(snapshot.size(), Map.of());
      handleScheduledFlushErrors(snapshot, e.getMessage());
    }
  }

  /**
   * Swaps buffer and returns snapshot if not empty. Use for close-time flushing.
   *
   * @return The buffer snapshot, or null if empty
   */
  protected final List<Pair<RecordFleakData, T>> swapBufferIfNotEmpty() {
    synchronized (bufferLock) {
      if (buffer.isEmpty()) {
        return null;
      }
      return swapBuffer();
    }
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
      if (canWriteRecord(pair.getRight())) {
        goodRecords.add(pair);
      } else {
        log.warn("Record {} failed validation in recovery mode", i);
        errors.add(new ErrorOutput(pair.getLeft(), "Record validation failed"));
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
   * Atomically swaps the buffer with a new empty buffer and returns the old one. MUST be called
   * inside synchronized(bufferLock) block.
   *
   * @return The old buffer contents
   */
  protected List<Pair<RecordFleakData, T>> swapBuffer() {
    List<Pair<RecordFleakData, T>> snapshot = buffer;
    buffer = new ArrayList<>();
    return snapshot;
  }

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
