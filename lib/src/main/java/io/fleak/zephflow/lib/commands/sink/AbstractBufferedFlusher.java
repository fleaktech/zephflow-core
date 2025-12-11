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

import io.delta.kernel.types.StructType;
import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.deltalakesink.DeltaLakeDataConverter;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Base class for buffered sink flushers that provides common functionality for buffer management,
 * record-level recovery, retry logic, and DLQ handling.
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

  protected AbstractBufferedFlusher(DlqWriter dlqWriter) {
    this.dlqWriter = dlqWriter;
  }

  /** Returns the schema for record validation. */
  protected abstract StructType getSchema();

  /**
   * Performs the actual write operation. Called outside any locks.
   *
   * @param data The data to write
   * @return The flush result
   * @throws Exception if write fails
   */
  protected abstract SimpleSinkCommand.FlushResult doWriteBatch(List<T> data) throws Exception;

  /**
   * Checks if exception is retryable (e.g., transient network error).
   *
   * @param e The exception to check
   * @return true if the exception is retryable
   */
  protected abstract boolean isRetryableException(Exception e);

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
   * Validates if a record can be written by attempting to convert it to columnar format.
   *
   * @param record The record to validate
   * @return true if the record can be written
   */
  @SuppressWarnings("unchecked")
  protected boolean canWriteRecord(T record) {
    if (record == null) return false;
    if (!(record instanceof Map)) return true;
    try (var ignored =
        DeltaLakeDataConverter.convertToColumnarBatch(
            List.of((Map<String, Object>) record), getSchema())) {
      return true;
    } catch (Exception e) {
      log.debug("Record validation failed: {}", e.getMessage());
      return false;
    }
  }

  /**
   * Attempts to write a batch with recovery. First tries to write all records; if that fails,
   * filters out bad records and writes the good ones.
   *
   * @param batch The batch to write
   * @return The write result including any errors
   */
  protected WriteResult writeWithRecovery(List<Pair<RecordFleakData, T>> batch) {
    List<T> dataToWrite = batch.stream().map(Pair::getRight).toList();

    try {
      SimpleSinkCommand.FlushResult result = writeWithRetry(dataToWrite);
      return new WriteResult(result, List.of());
    } catch (Exception e) {
      log.warn("Batch write failed, entering recovery mode: {}", e.getMessage());
      return filterAndWrite(batch);
    }
  }

  /**
   * Filters out bad records and writes the good ones.
   *
   * @param batch The batch to filter and write
   * @return The write result including errors for bad records
   */
  protected WriteResult filterAndWrite(List<Pair<RecordFleakData, T>> batch) {
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
      return new WriteResult(new SimpleSinkCommand.FlushResult(0, 0, List.of()), errors);
    }

    List<T> goodData = goodRecords.stream().map(Pair::getRight).toList();
    try {
      SimpleSinkCommand.FlushResult result = writeWithRetry(goodData);
      return new WriteResult(result, errors);
    } catch (Exception e) {
      log.error("Bulk write of validated records failed: {}", e.getMessage());
      for (Pair<RecordFleakData, T> pair : goodRecords) {
        errors.add(new ErrorOutput(pair.getLeft(), "Write failed: " + e.getMessage()));
      }
      return new WriteResult(new SimpleSinkCommand.FlushResult(0, 0, List.of()), errors);
    }
  }

  /**
   * Writes data with retry logic using exponential backoff.
   *
   * @param data The data to write
   * @return The flush result
   * @throws Exception if all retries are exhausted
   */
  protected SimpleSinkCommand.FlushResult writeWithRetry(List<T> data) throws Exception {
    Exception lastException = null;
    long retryDelayMs = INITIAL_RETRY_DELAY_MS;

    for (int attempt = 1; attempt <= MAX_WRITE_RETRIES; attempt++) {
      try {
        return doWriteBatch(data);
      } catch (Exception e) {
        lastException = e;
        if (!isRetryableException(e) || attempt == MAX_WRITE_RETRIES) {
          throw e;
        }
        log.warn(
            "Retryable error (attempt {}/{}), retrying in {}ms: {}",
            attempt,
            MAX_WRITE_RETRIES,
            retryDelayMs,
            e.getMessage());
        try {
          Thread.sleep(retryDelayMs);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IOException("Retry interrupted", ie);
        }
        retryDelayMs *= 2;
      }
    }
    throw lastException;
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

  /** Result of a write operation including flush result and any additional errors. */
  protected record WriteResult(
      SimpleSinkCommand.FlushResult flushResult, List<ErrorOutput> additionalErrors) {}
}
