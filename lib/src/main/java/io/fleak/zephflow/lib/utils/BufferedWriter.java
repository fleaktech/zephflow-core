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
package io.fleak.zephflow.lib.utils;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * A reusable buffered writer component that provides: - Thread-safe buffer management - Batch size
 * threshold triggering - Scheduled flush timer - Configurable flush handler callback
 *
 * <p>This component uses composition to allow any class to have buffered write capability without
 * inheritance constraints.
 */
@Slf4j
public class BufferedWriter<T> implements Closeable {
  private final int batchSize;
  private final long flushIntervalMs;
  private final FlushHandler<T> flushHandler;
  private final String threadName;
  private final boolean syncMode;

  private List<T> buffer = new ArrayList<>();
  private final Object bufferLock = new Object();
  private ScheduledExecutorService scheduler;
  private ScheduledFuture<?> flushTask;
  private volatile boolean started = false;
  private volatile boolean closed = false;

  @FunctionalInterface
  public interface FlushHandler<T> {
    void flush(List<T> batch) throws Exception;
  }

  public BufferedWriter(
      int batchSize, long flushIntervalMs, FlushHandler<T> flushHandler, String threadName) {
    this(batchSize, flushIntervalMs, flushHandler, threadName, false);
  }

  public BufferedWriter(
      int batchSize,
      long flushIntervalMs,
      FlushHandler<T> flushHandler,
      String threadName,
      boolean syncMode) {
    this.batchSize = batchSize;
    this.flushIntervalMs = flushIntervalMs;
    this.flushHandler = flushHandler;
    this.threadName = threadName;
    this.syncMode = syncMode;
  }

  public synchronized void start() {
    if (started) {
      log.warn("BufferedWriter already started, skipping");
      return;
    }

    if (syncMode) {
      log.debug("Sync mode enabled, timer-based flushing disabled");
      started = true;
      return;
    }

    if (flushIntervalMs <= 0) {
      throw new IllegalArgumentException(
          "flushIntervalMs must be positive, but was: " + flushIntervalMs);
    }

    log.info(
        "Starting BufferedWriter with interval {}ms, batchSize={}, thread: {}",
        flushIntervalMs,
        batchSize,
        threadName);

    scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, threadName);
              t.setDaemon(true);
              return t;
            });

    flushTask =
        scheduler.scheduleWithFixedDelay(
            this::scheduledFlush,
            Math.min(flushIntervalMs, 1000),
            flushIntervalMs,
            TimeUnit.MILLISECONDS);

    started = true;
    log.info("BufferedWriter started successfully");
  }

  public void write(T item) {
    write(List.of(item));
  }

  public void write(List<T> items) {
    write(items, true);
  }

  /**
   * Writes items to the buffer with optional auto-flush.
   *
   * @param items The items to write
   * @param autoFlush If true, triggers flush when batch size reached or in sync mode
   */
  public void write(List<T> items, boolean autoFlush) {
    if (closed) {
      throw new IllegalStateException("BufferedWriter is closed");
    }

    List<T> toFlush = null;

    synchronized (bufferLock) {
      buffer.addAll(items);
      log.debug(
          "Added {} items to buffer. Buffer size: {} / {}", items.size(), buffer.size(), batchSize);

      if (autoFlush && (syncMode || buffer.size() >= batchSize)) {
        log.info(
            "Triggering flush: syncMode={}, bufferSize={}, batchSize={}",
            syncMode,
            buffer.size(),
            batchSize);
        toFlush = swapBuffer();
      }
    }

    if (toFlush != null && !toFlush.isEmpty()) {
      doFlush(toFlush);
    }
  }

  /**
   * Checks if the buffer has reached the batch size threshold.
   *
   * @return true if buffer size >= batchSize or in sync mode
   */
  public boolean shouldFlush() {
    synchronized (bufferLock) {
      return syncMode || buffer.size() >= batchSize;
    }
  }

  public void flush() {
    List<T> toFlush;
    synchronized (bufferLock) {
      if (buffer.isEmpty()) {
        return;
      }
      toFlush = swapBuffer();
    }
    if (!toFlush.isEmpty()) {
      doFlush(toFlush);
    }
  }

  public List<T> flushAndGet() {
    synchronized (bufferLock) {
      if (buffer.isEmpty()) {
        return List.of();
      }
      return swapBuffer();
    }
  }

  private void scheduledFlush() {
    try {
      List<T> toFlush;
      synchronized (bufferLock) {
        if (buffer.isEmpty()) {
          log.trace("Scheduled flush skipped: buffer empty");
          return;
        }
        log.info("Scheduled flush triggered: {} items buffered", buffer.size());
        toFlush = swapBuffer();
      }
      doFlush(toFlush);
    } catch (Exception e) {
      log.error("Error during scheduled flush", e);
    } catch (Error e) {
      log.error("Fatal error during scheduled flush - scheduler will stop", e);
      throw e;
    }
  }

  private void doFlush(List<T> batch) {
    if (batch.isEmpty()) {
      return;
    }
    log.info("Flushing {} items", batch.size());
    try {
      flushHandler.flush(batch);
    } catch (Exception e) {
      log.error("Error during flush", e);
    }
  }

  private List<T> swapBuffer() {
    List<T> snapshot = buffer;
    buffer = new ArrayList<>();
    return snapshot;
  }

  public int getBufferSize() {
    synchronized (bufferLock) {
      return buffer.size();
    }
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;

    if (flushTask != null) {
      log.debug("Cancelling flush task");
      flushTask.cancel(false);
      flushTask = null;
    }

    if (scheduler != null) {
      log.debug("Shutting down scheduler");
      scheduler.shutdown();
      try {
        if (!scheduler.awaitTermination(30, TimeUnit.SECONDS)) {
          log.warn("Scheduler did not terminate in time, forcing shutdown");
          scheduler.shutdownNow();
        }
      } catch (InterruptedException e) {
        log.warn("Interrupted while waiting for scheduler to terminate");
        scheduler.shutdownNow();
        Thread.currentThread().interrupt();
      }
      scheduler = null;
    }

    // Final flush
    flush();
  }
}
