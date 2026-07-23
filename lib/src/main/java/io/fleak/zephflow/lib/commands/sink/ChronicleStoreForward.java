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

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.core.io.SingleThreadedChecked;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.wire.DocumentContext;

/**
 * Chronicle-Queue-backed {@link SinkStoreForward}. Persists raw records to an append-only disk
 * queue during a connectivity outage, and a single background worker drains them back to the remote
 * oldest-first once it recovers.
 *
 * <p>State machine (held entirely here; the sink command just asks {@link #isBuffering()}):
 *
 * <ul>
 *   <li><b>DIRECT</b> ({@code buffering == false}) — healthy; the sink writes straight to remote.
 *   <li><b>BUFFERING</b> ({@code buffering == true}) — outage; {@link #offer} appends to disk and
 *       the worker keeps trying to drain. When the queue empties (confirmed under {@code stateLock}
 *       so a concurrent {@link #offer} cannot leapfrog it), it flips back to DIRECT.
 * </ul>
 *
 * <p>Durability across restarts uses an explicit <i>delivered watermark</i> written to a small ack
 * file, advanced only after a chunk is successfully delivered (or permanently dropped). We do not
 * use a named Chronicle tailer, whose auto-persisted read position would advance past records that
 * were read but not yet delivered — losing them on a mid-outage restart.
 */
@Slf4j
public class ChronicleStoreForward implements SinkStoreForward {

  public record Config(
      Path storePath, long maxBytes, long retryIntervalMs, int drainChunkSize, String nodeId) {}

  private static final String PAYLOAD_KEY = "r";
  static final String ACK_FILE_NAME = "sf-ack.idx";
  private static final String ACK_CAUGHT_UP = "END";
  static final String LOCK_FILE_NAME = "sf.lock";

  // Cross-repo contract: written once the buffer is fully drained and its queue files are gone,
  // deleted the moment a new outage starts. "Present" is a one-directional guarantee that the
  // directory holds no undelivered records, so grid's orphan cleaner (FLE-2272) can safely reclaim
  // an unowned dir that carries it. Produced here; consumed by grid in a separate change.
  static final String DRAINED_MARKER_FILE_NAME = "sf-drained";

  private final long maxBytes;
  private final long retryIntervalMs;
  private final int drainChunkSize;
  private final String nodeId;
  private final ConnectionFailureClassifier classifier;

  private final FleakCounter bufferedCounter;
  private final FleakCounter replayedCounter;
  private final FleakCounter droppedCounter;

  // Outage-only resource: built when buffering starts, torn down (closed + files deleted) when the
  // buffer fully drains. null exactly when buffering == false. Guarded by stateLock; volatile so
  // the
  // worker's lock-free tailer reads always see the current instance.
  private volatile ChronicleQueue queue;
  private volatile ExcerptAppender appender;
  private volatile ExcerptTailer tailer;
  private final Path storePath;
  private final Path ackFile;
  private final Path drainedMarker;
  private final FileChannel lockChannel;
  private final FileLock dirLock;

  // Outstanding bytes still on disk (appended minus drained). Bounds disk use against maxBytes.
  // Reset to 0 on restart: the cap then applies to newly appended data only.
  private final AtomicLong outstandingBytes = new AtomicLong(0);

  private final Object stateLock = new Object();
  private volatile boolean buffering = false;
  private volatile boolean closed = false;

  private ReplayTarget replayTarget;
  private Thread worker;

  public ChronicleStoreForward(
      Config config,
      ConnectionFailureClassifier classifier,
      FleakCounter bufferedCounter,
      FleakCounter replayedCounter,
      FleakCounter droppedCounter) {
    this.maxBytes = config.maxBytes();
    this.retryIntervalMs = config.retryIntervalMs();
    this.drainChunkSize = config.drainChunkSize();
    this.nodeId = config.nodeId();
    this.classifier = classifier;
    this.bufferedCounter = bufferedCounter;
    this.replayedCounter = replayedCounter;
    this.droppedCounter = droppedCounter;
    this.storePath = config.storePath();
    this.ackFile = storePath.resolve(ACK_FILE_NAME);
    this.drainedMarker = storePath.resolve(DRAINED_MARKER_FILE_NAME);
    // Exclusive OS-level lock on the buffer directory: cross-process sharing would silently
    // corrupt the queue and the ack watermark, so a second process must fail fast instead.
    try {
      Files.createDirectories(storePath);
      this.lockChannel =
          FileChannel.open(
              storePath.resolve(LOCK_FILE_NAME),
              StandardOpenOption.CREATE,
              StandardOpenOption.WRITE);
    } catch (IOException e) {
      throw new IllegalStateException(
          "store-and-forward [" + nodeId + "] cannot open buffer dir " + storePath, e);
    }
    FileLock lock;
    try {
      lock = lockChannel.tryLock();
    } catch (OverlappingFileLockException e) {
      lock = null; // held by another instance in this JVM
    } catch (IOException e) {
      closeQuietly(lockChannel);
      throw new IllegalStateException(
          "store-and-forward [" + nodeId + "] cannot lock buffer dir " + storePath, e);
    }
    if (lock == null) {
      closeQuietly(lockChannel);
      throw new IllegalStateException(
          "store-and-forward ["
              + nodeId
              + "] buffer dir "
              + storePath
              + " is already locked by another process; each job replica needs its own directory");
    }
    this.dirLock = lock;
    // Open over whatever is already on disk (a prior run's backlog, if any); start() decides
    // whether
    // to resume it or reclaim it. The ack is left intact here so a restart can seek to its
    // watermark.
    buildQueue();
  }

  /** Builds the queue/appender/tailer over {@link #storePath} without touching any file. */
  private void buildQueue() {
    queue = SingleChronicleQueueBuilder.single(storePath.toFile()).build();
    appender = queue.createAppender();
    tailer = queue.createTailer();
    // We manage threading ourselves: the appender is only touched from offer() under stateLock, the
    // tailer only from the worker (and from start() before the worker exists). Chronicle's
    // single-thread ownership check is too strict for that controlled hand-off, so disable it.
    ((SingleThreadedChecked) appender).singleThreadedCheckDisabled(true);
    ((SingleThreadedChecked) tailer).singleThreadedCheckDisabled(true);
  }

  /**
   * New outage after a teardown: guarantee a clean slate before appending. Removes the drained
   * marker first (so "marker present" never coexists with undelivered data), then any stale ack (so
   * a later restart cannot {@code toEnd()} past these fresh records), then any residual queue files
   * (so already-delivered leftovers are not replayed), then builds an empty queue. The marker/ack
   * deletes are hard: on failure this throws and {@link #offer} reports the records as not stored
   * rather than risk buffering under a stale watermark.
   */
  private void rebuildQueueFresh() throws IOException {
    Files.deleteIfExists(drainedMarker);
    Files.deleteIfExists(ackFile);
    // Residual delivered records must be gone too, else the fresh tailer would replay them. Treat a
    // failure as hard (like the marker/ack deletes above): refuse to buffer into a dirty queue and
    // let offer() report the records as not stored, rather than silently re-deliver on rebuild.
    if (!deleteQueueFiles()) {
      throw new IOException(
          "store-and-forward ["
              + nodeId
              + "] could not clear residual queue files in "
              + storePath);
    }
    buildQueue();
  }

  /**
   * Fully drained → reclaim disk. Closes the queue, deletes its files, and marks the directory
   * drained. Caller must hold {@link #stateLock} (or run single-threaded in {@link #start}).
   */
  private void teardownQueue() {
    if (queue != null) {
      queue.close();
    }
    queue = null;
    appender = null;
    tailer = null;
    boolean allGone = deleteQueueFiles();
    try {
      if (allGone) {
        Files.deleteIfExists(ackFile); // absent ack == "read from the beginning" for a future queue
      } else {
        // A delivered leftover survived deletion: keep an END watermark so a future reader (grid's
        // orphan cleaner) still classifies the dir as fully delivered, never as undelivered.
        writeAck(ACK_CAUGHT_UP);
      }
    } catch (IOException e) {
      log.warn("store-and-forward [{}] could not reset ack on teardown", nodeId, e);
    }
    writeMarker();
    outstandingBytes.set(0);
  }

  /**
   * Deletes the Chronicle queue files (every regular file except the lock, ack, and drained
   * marker). Returns {@code true} if none remain.
   */
  private boolean deleteQueueFiles() {
    boolean allGone = true;
    try (Stream<Path> files = Files.list(storePath)) {
      for (Path f : (Iterable<Path>) files::iterator) {
        String name = f.getFileName().toString();
        if (name.equals(LOCK_FILE_NAME)
            || name.equals(ACK_FILE_NAME)
            || name.equals(DRAINED_MARKER_FILE_NAME)
            || !Files.isRegularFile(f)) {
          continue;
        }
        try {
          Files.deleteIfExists(f);
        } catch (IOException e) {
          allGone = false;
          log.warn("store-and-forward [{}] could not delete queue file {}", nodeId, f, e);
        }
      }
    } catch (IOException e) {
      log.warn("store-and-forward [{}] could not list buffer dir {}", nodeId, storePath, e);
      return false;
    }
    return allGone;
  }

  private void writeMarker() {
    try {
      Files.writeString(drainedMarker, "", StandardCharsets.UTF_8);
    } catch (IOException e) {
      log.warn("store-and-forward [{}] could not write drained marker", nodeId, e);
    }
  }

  @Override
  public boolean isBuffering() {
    return buffering;
  }

  @Override
  public boolean shouldBuffer(Throwable t) {
    return classifier.isConnectionFailure(t);
  }

  @Override
  public int offer(List<RecordFleakData> records) {
    if (records.isEmpty()) {
      return 0;
    }
    int stored = 0;
    synchronized (stateLock) {
      if (queue == null) {
        // Coming out of DIRECT: this is a fresh outage, so open a clean queue before appending.
        try {
          rebuildQueueFresh();
        } catch (IOException e) {
          log.error(
              "store-and-forward [{}] cannot open buffer dir {}; {} records reported as not stored",
              nodeId,
              storePath,
              records.size(),
              e);
          return 0; // stays DIRECT; the caller surfaces the unstored records as errors
        }
      }
      for (RecordFleakData record : records) {
        byte[] payload = encode(record);
        long need = payload.length + (long) Integer.BYTES;
        if (outstandingBytes.get() + need > maxBytes) {
          break; // cap reached; drop the remaining tail
        }
        append(payload);
        outstandingBytes.addAndGet(need);
        stored++;
      }
      inc(bufferedCounter, stored);
      int dropped = records.size() - stored;
      if (dropped > 0) {
        inc(droppedCounter, dropped);
        log.warn(
            "store-and-forward [{}] local store full (cap {} bytes); dropped {} records",
            nodeId,
            maxBytes,
            dropped);
      }
      buffering = true;
      stateLock.notifyAll();
    }
    return stored;
  }

  @Override
  public void start(ReplayTarget target) {
    this.replayTarget = target;
    positionFromAck();
    // Resume a backlog from a previous run before the sink does any direct write; if there is none,
    // reclaim the empty queue the constructor just built (and any leftovers from a clean shutdown).
    if (peekHasMore()) {
      buffering = true;
      log.info("store-and-forward [{}] found a backlog on startup; resuming drain", nodeId);
    } else {
      teardownQueue();
    }
    worker = new Thread(this::drainLoop, "sink-store-forward-" + nodeId);
    worker.setDaemon(true);
    worker.start();
  }

  @Override
  public void close() {
    synchronized (stateLock) {
      closed = true;
      stateLock.notifyAll();
    }
    if (worker != null) {
      try {
        worker.join(TimeUnit.SECONDS.toMillis(30));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    // May already be null if the buffer drained back to DIRECT. Never delete files here: a still-
    // buffering instance shut down must leave its undelivered records + ack for the next instance.
    if (queue != null) {
      queue.close();
    }
    try {
      dirLock.release();
    } catch (IOException e) {
      log.warn("store-and-forward [{}] could not release dir lock", nodeId, e);
    }
    closeQuietly(lockChannel);
  }

  private static void closeQuietly(FileChannel channel) {
    try {
      channel.close();
    } catch (IOException e) {
      log.warn("store-and-forward could not close lock file channel", e);
    }
  }

  // ===== worker =====

  private enum DrainOutcome {
    DELIVERED,
    CONN_FAILURE,
    DRAINED
  }

  private void drainLoop() {
    while (true) {
      synchronized (stateLock) {
        while (!buffering && !closed) {
          try {
            stateLock.wait();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
        if (closed) {
          return;
        }
      }
      try {
        if (drainOnce() == DrainOutcome.CONN_FAILURE) {
          sleepBeforeRetry();
        }
      } catch (RuntimeException e) {
        // Never let the worker die silently: log, back off, and try again.
        log.error("store-and-forward [{}] unexpected drain error", nodeId, e);
        sleepBeforeRetry();
      }
    }
  }

  private DrainOutcome drainOnce() {
    Chunk chunk = readChunk();
    if (chunk.records.isEmpty()) {
      // Nothing to read. Confirm the queue is truly empty under the lock so an offer() racing the
      // flip cannot slip a record behind us, then return to DIRECT.
      synchronized (stateLock) {
        if (!peekHasMore()) {
          buffering = false;
          teardownQueue(); // fully caught up: close the queue and reclaim its disk
          return DrainOutcome.DRAINED;
        }
      }
      return DrainOutcome.DELIVERED;
    }
    try {
      replayTarget.deliver(chunk.records);
      outstandingBytes.addAndGet(-chunk.bytes);
      inc(replayedCounter, chunk.records.size());
      advanceAck();
      return DrainOutcome.DELIVERED;
    } catch (Exception e) {
      if (classifier.isConnectionFailure(e)) {
        tailer.moveToIndex(chunk.startIndex); // rewind; keep the records for the next attempt
        log.warn(
            "store-and-forward [{}] replay hit a connectivity failure, will retry: {}",
            nodeId,
            e.getMessage());
        return DrainOutcome.CONN_FAILURE;
      }
      // Permanent failure: these records can never be delivered. The tailer has already advanced
      // past them, so account for the drop and advance the watermark so we don't re-read them.
      outstandingBytes.addAndGet(-chunk.bytes);
      inc(droppedCounter, chunk.records.size());
      advanceAck();
      log.error(
          "store-and-forward [{}] dropping {} records after a non-connectivity replay failure",
          nodeId,
          chunk.records.size(),
          e);
      return DrainOutcome.DELIVERED;
    }
  }

  private Chunk readChunk() {
    List<RecordFleakData> records = new ArrayList<>();
    long bytes = 0;
    long startIndex = -1;
    for (int i = 0; i < drainChunkSize; i++) {
      try (DocumentContext dc = tailer.readingDocument()) {
        if (!dc.isPresent()) {
          break;
        }
        if (startIndex == -1) {
          startIndex = dc.index();
        }
        byte[] arr = dc.wire().read(PAYLOAD_KEY).bytes();
        records.add(decode(arr));
        bytes += arr.length + (long) Integer.BYTES;
      }
    }
    return new Chunk(records, bytes, startIndex);
  }

  /** Worker-thread / pre-worker only. True if an unread excerpt exists, without consuming it. */
  private boolean peekHasMore() {
    return peekNextIndex() != -1;
  }

  /** Reads the next excerpt's index without consuming it (rewinds the tailer to it); -1 if none. */
  private long peekNextIndex() {
    long index;
    try (DocumentContext dc = tailer.readingDocument()) {
      if (!dc.isPresent()) {
        return -1;
      }
      index = dc.index();
    }
    // moveToIndex must be called after the reading context is closed, otherwise it is ignored.
    tailer.moveToIndex(index);
    return index;
  }

  // ===== delivered watermark (restart durability) =====

  /** Positions the tailer at the persisted delivered watermark, or leaves it at the queue start. */
  private void positionFromAck() {
    String token = readAck();
    if (token == null) {
      return; // fresh queue: tailer is at the start
    }
    if (ACK_CAUGHT_UP.equals(token)) {
      tailer.toEnd();
      return;
    }
    try {
      if (!tailer.moveToIndex(Long.parseLong(token))) {
        log.warn("store-and-forward [{}] could not seek to ack index {}", nodeId, token);
      }
    } catch (NumberFormatException e) {
      log.warn("store-and-forward [{}] ignoring malformed ack token: {}", nodeId, token);
    }
  }

  /** Records the next undelivered index (or caught-up) as the durable watermark after a chunk. */
  private void advanceAck() {
    long next = peekNextIndex();
    writeAck(next >= 0 ? Long.toString(next) : ACK_CAUGHT_UP);
  }

  private String readAck() {
    try {
      if (!Files.exists(ackFile)) {
        return null;
      }
      return Files.readString(ackFile, StandardCharsets.UTF_8).trim();
    } catch (Exception e) {
      log.warn("store-and-forward [{}] could not read ack file", nodeId, e);
      return null;
    }
  }

  private void writeAck(String token) {
    // Write-temp-then-atomic-rename so a crash mid-write can never leave a torn ack file that reads
    // as a bogus watermark. The scheme leans on ack correctness for restart durability.
    try {
      Path tmp = storePath.resolve(ACK_FILE_NAME + ".tmp");
      Files.writeString(tmp, token, StandardCharsets.UTF_8);
      try {
        Files.move(tmp, ackFile, StandardCopyOption.ATOMIC_MOVE);
      } catch (AtomicMoveNotSupportedException e) {
        Files.move(tmp, ackFile, StandardCopyOption.REPLACE_EXISTING);
      }
    } catch (Exception e) {
      log.warn("store-and-forward [{}] could not persist ack watermark", nodeId, e);
    }
  }

  private void sleepBeforeRetry() {
    synchronized (stateLock) {
      if (closed) {
        return;
      }
      try {
        stateLock.wait(retryIntervalMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void append(byte[] payload) {
    try (DocumentContext dc = appender.writingDocument()) {
      dc.wire().write(PAYLOAD_KEY).bytes(payload);
    }
  }

  private static byte[] encode(RecordFleakData record) {
    try {
      return JsonUtils.OBJECT_MAPPER.writeValueAsBytes(record.unwrap());
    } catch (Exception e) {
      throw new RuntimeException("failed to serialize record for store-and-forward", e);
    }
  }

  private static RecordFleakData decode(byte[] bytes) {
    try {
      Map<String, Object> map =
          JsonUtils.OBJECT_MAPPER.readValue(bytes, new TypeReference<Map<String, Object>>() {});
      return (RecordFleakData) FleakData.wrap(map);
    } catch (Exception e) {
      throw new RuntimeException("failed to deserialize record for store-and-forward", e);
    }
  }

  private static void inc(FleakCounter counter, long n) {
    if (counter != null && n > 0) {
      counter.increase(n, Map.of());
    }
  }

  private record Chunk(List<RecordFleakData> records, long bytes, long startIndex) {}
}
