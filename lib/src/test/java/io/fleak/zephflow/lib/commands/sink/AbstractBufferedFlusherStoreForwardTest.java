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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class AbstractBufferedFlusherStoreForwardTest {

  private static final DestinationFailureClassifier IO_IS_DESTINATION =
      t -> t instanceof IOException;

  private final CountingCounter sinkOutput = new CountingCounter();
  private final CountingCounter outputSize = new CountingCounter();
  private final CountingCounter sinkError = new CountingCounter();
  private final CountingCounter preprocessError = new CountingCounter();
  private final CountingCounter buffered = new CountingCounter();
  private final CountingCounter replayed = new CountingCounter();
  private final CountingCounter dropped = new CountingCounter();
  private final List<String> logs = new CopyOnWriteArrayList<>();
  private final Logger logger = (Logger) LogManager.getLogger(AbstractBufferedFlusher.class);
  private AbstractAppender appender;
  private Level previousLevel;

  @BeforeEach
  void captureLogs() {
    appender =
        new AbstractAppender("store-forward-test", null, null, false, null) {
          @Override
          public void append(LogEvent event) {
            logs.add(event.getMessage().getFormattedMessage());
          }
        };
    appender.start();
    previousLevel = logger.getLevel();
    logger.addAppender(appender);
    logger.setLevel(Level.ALL);
  }

  @AfterEach
  void releaseLogs() {
    logger.removeAppender(appender);
    logger.setLevel(previousLevel);
    appender.stop();
  }

  @Test
  void timerFlushWithADestinationFailureStoresTheRecords(@TempDir Path dir) throws Exception {
    FakeFlusher flusher = new FakeFlusher(10);
    ChronicleStoreForward store = store(dir, 1_000_000);
    enable(flusher, store);
    store.start(replayTarget(flusher));
    flusher.failure = new IOException("connection refused");

    flusher.flush(events(1, 2), Map.of());
    flusher.executeScheduledFlush();

    assertEquals(new Counts(0, 0, 0, 2, 0, 0), counts());
    assertFalse(lostRecordsLogged());

    flusher.failure = null;
    await(() -> !store.isBuffering());
    assertEquals(List.of(record(1), record(2)), flusher.sentRecords());
    assertEquals(new Counts(2, 22, 0, 2, 2, 0), counts());
    store.close();
    flusher.close();
  }

  @Test
  void closeTimeFlushWithADestinationFailureKeepsTheRecordsOnDisk(@TempDir Path dir)
      throws Exception {
    FakeFlusher flusher = new FakeFlusher(10);
    ChronicleStoreForward store = store(dir, 1_000_000);
    enable(flusher, store);
    store.start(replayTarget(flusher));
    flusher.initialize();
    flusher.failure = new IOException("connection refused");
    flusher.flush(events(1, 2), Map.of());

    new SinkExecutionContext<>(flusher, (event, ts) -> event, store, null, null, null, null, null)
        .close();

    assertEquals(new Counts(0, 0, 0, 2, 0, 0), counts());
    assertFalse(lostRecordsLogged());
    List<RecordFleakData> delivered = new CopyOnWriteArrayList<>();
    ChronicleStoreForward reopened = store(dir, 1_000_000);
    reopened.start(delivered::addAll);
    await(() -> !reopened.isBuffering());
    assertEquals(List.of(record(1), record(2)), delivered);
    reopened.close();
  }

  @Test
  void recordProblemIsCountedAndNotStored(@TempDir Path dir) throws Exception {
    FakeFlusher flusher = new FakeFlusher(10);
    ChronicleStoreForward store = store(dir, 1_000_000);
    enable(flusher, store);
    store.start(replayTarget(flusher));
    flusher.failure = new IllegalArgumentException("malformed record");

    flusher.flush(events(1, 2), Map.of());
    flusher.executeScheduledFlush();

    assertEquals(new Counts(0, 0, 2, 0, 0, 0), counts());
    assertFalse(store.isBuffering());
    assertTrue(lostRecordsLogged());
    store.close();
    flusher.close();
  }

  @Test
  void batchesWhileBufferingAreStoredBehindTheBacklog(@TempDir Path dir) throws Exception {
    FakeFlusher flusher = new FakeFlusher(10);
    ChronicleStoreForward store = store(dir, 1_000_000);
    enable(flusher, store);
    CountDownLatch destinationBack = new CountDownLatch(1);
    SinkStoreForward.ReplayTarget target = replayTarget(flusher);
    store.start(
        records -> {
          destinationBack.await();
          target.deliver(records);
        });
    flusher.failure = new IOException("connection refused");

    flusher.flush(events(1, 2), Map.of());
    flusher.executeScheduledFlush();
    flusher.flush(events(3, 4), Map.of());
    flusher.executeScheduledFlush();

    assertEquals(1, flusher.sendAttempts.get());
    assertEquals(new Counts(0, 0, 0, 4, 0, 0), counts());

    flusher.failure = null;
    destinationBack.countDown();
    await(() -> !store.isBuffering());
    assertEquals(List.of(record(1), record(2), record(3), record(4)), flusher.sentRecords());
    store.close();
    flusher.close();
  }

  @Test
  void recordsTheFullStoreCannotTakeAreErrors(@TempDir Path dir) throws Exception {
    FakeFlusher flusher = new FakeFlusher(2);
    ChronicleStoreForward store = store(dir, 20);
    enable(flusher, store);
    store.start(replayTarget(flusher));
    flusher.failure = new IOException("connection refused");

    SimpleSinkCommand.FlushResult result = flusher.flush(events(1, 2), Map.of());

    assertEquals(
        new SimpleSinkCommand.FlushResult(
            0, 0, List.of(new ErrorOutput(record(2), SimpleSinkCommand.STORE_FORWARD_FULL_MSG))),
        result);
    assertEquals(new Counts(0, 0, 0, 1, 0, 1), counts());
    store.close();
    flusher.close();
  }

  @Test
  void recordErrorsOfADestinationFailureStayErrorsAndTheRestIsStored(@TempDir Path dir)
      throws Exception {
    FakeFlusher flusher = new FakeFlusher(2);
    ChronicleStoreForward store = store(dir, 1_000_000);
    enable(flusher, store);
    store.start(replayTarget(flusher));
    ErrorOutput rejected = new ErrorOutput(record(1), "missing __raw__");
    flusher.failure =
        new DestinationFailureException(new IOException("no such bucket"), List.of(rejected));

    SimpleSinkCommand.FlushResult result = flusher.flush(events(1, 2), Map.of());

    assertEquals(new SimpleSinkCommand.FlushResult(0, 0, List.of(rejected)), result);
    assertEquals(new Counts(0, 0, 0, 1, 0, 0), counts());
    flusher.failure = null;
    await(() -> !store.isBuffering());
    assertEquals(List.of(record(2)), flusher.sentRecords());
    store.close();
    flusher.close();
  }

  @Test
  void withoutAStoreFailuresAreCountedAndLoggedAsLost() throws Exception {
    FakeFlusher flusher = new FakeFlusher(10);
    flusher.failure = new IOException("connection refused");

    flusher.flush(events(1, 2), Map.of());
    flusher.executeScheduledFlush();

    assertEquals(new Counts(0, 0, 2, 0, 0, 0), counts());
    assertTrue(lostRecordsLogged());
    flusher.close();
  }

  @Test
  void replayTargetCountsDeliveredRecordsAndDropsRecordErrors() throws Exception {
    FakeFlusher flusher = new FakeFlusher(10);
    flusher.enableStoreForward(SinkStoreForward.noop(), IO_IS_DESTINATION);
    SinkStoreForward.ReplayTarget target = replayTarget(flusher);

    target.deliver(List.of(record(1), record(2)));
    flusher.failure = new IllegalArgumentException("malformed record");
    target.deliver(List.of(record(3)));
    flusher.failure = new IOException("connection refused");

    assertThrows(IOException.class, () -> target.deliver(List.of(record(4))));
    assertEquals(new Counts(2, 22, 1, 0, 0, 0), counts());
    flusher.close();
  }

  private record Counts(
      long sinkOutput,
      long outputSize,
      long sinkError,
      long buffered,
      long replayed,
      long dropped) {}

  private Counts counts() {
    return new Counts(
        sinkOutput.total(),
        outputSize.total(),
        sinkError.total(),
        buffered.total(),
        replayed.total(),
        dropped.total());
  }

  private void enable(FakeFlusher flusher, ChronicleStoreForward store) {
    flusher.enableStoreForward(store, IO_IS_DESTINATION);
  }

  private SinkStoreForward.ReplayTarget replayTarget(FakeFlusher flusher) {
    return flusher.replayTarget(
        (event, ts) -> event, sinkOutput, outputSize, sinkError, preprocessError);
  }

  private ChronicleStoreForward store(Path dir, long maxBytes) {
    return new ChronicleStoreForward(
        new ChronicleStoreForward.Config(dir, maxBytes, 50, 10, "node"),
        IO_IS_DESTINATION::isDestinationFailure,
        buffered,
        replayed,
        dropped);
  }

  private boolean lostRecordsLogged() {
    return logs.stream().anyMatch(message -> message.contains("Records lost"));
  }

  private static RecordFleakData record(int id) {
    return (RecordFleakData) FleakData.wrap(Map.of("id", id));
  }

  private static SimpleSinkCommand.PreparedInputEvents<RecordFleakData> events(int... ids) {
    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    for (int id : ids) {
      events.add(record(id), record(id));
    }
    return events;
  }

  private static void await(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(20);
    }
    fail("condition not met within timeout");
  }

  private final class FakeFlusher extends AbstractBufferedFlusher<RecordFleakData> {
    private final int batchSize;
    private final List<List<RecordFleakData>> sent = new CopyOnWriteArrayList<>();
    final AtomicInteger sendAttempts = new AtomicInteger();
    volatile Exception failure;

    FakeFlusher(int batchSize) {
      super(null, null, "node", sinkOutput, outputSize, sinkError);
      this.batchSize = batchSize;
    }

    List<RecordFleakData> sentRecords() {
      return sent.stream().flatMap(List::stream).toList();
    }

    @Override
    protected int getBatchSize() {
      return batchSize;
    }

    @Override
    protected long getFlushIntervalMs() {
      return 600_000;
    }

    @Override
    protected SimpleSinkCommand.FlushResult doFlushWithRetry(
        List<Pair<RecordFleakData, RecordFleakData>> batch) throws Exception {
      return doFlush(batch);
    }

    @Override
    protected SimpleSinkCommand.FlushResult doFlush(
        List<Pair<RecordFleakData, RecordFleakData>> batch) throws Exception {
      sendAttempts.incrementAndGet();
      Exception current = failure;
      if (current != null) {
        throw current;
      }
      sent.add(batch.stream().map(Pair::getRight).toList());
      return new SimpleSinkCommand.FlushResult(batch.size(), batch.size() * 11L, List.of());
    }

    @Override
    protected void ensureCanWriteRecord(RecordFleakData record) {}

    @Override
    public void close() {
      stopFlushTimer();
    }
  }

  private static final class CountingCounter implements FleakCounter {
    private final AtomicLong total = new AtomicLong();

    long total() {
      return total.get();
    }

    @Override
    public void increase(Map<String, String> additionalTags) {
      total.incrementAndGet();
    }

    @Override
    public void increase(long n, Map<String, String> additionalTags) {
      total.addAndGet(n);
    }
  }
}
