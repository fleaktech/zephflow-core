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

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

/**
 * A destination outage must delay a buffered sink's records, never lose them: a flush that fails
 * because the destination is down keeps retrying until it is delivered (holding up the pipeline),
 * unless a local store keeps the records, the flusher is closing, or the failure is the records'.
 */
class AbstractBufferedFlusherRetryTest {

  private static final DestinationFailureClassifier IO_IS_DESTINATION =
      t -> t instanceof IOException;

  private final CountingCounter sinkOutput = new CountingCounter();
  private final CountingCounter outputSize = new CountingCounter();
  private final CountingCounter sinkError = new CountingCounter();

  @Test
  void scheduledFlushRetriesADestinationFailureUntilDelivered() throws Exception {
    FakeFlusher flusher = new FakeFlusher(100);
    flusher.enableStoreForward(SinkStoreForward.noop(), IO_IS_DESTINATION);
    flusher.initialize();
    flusher.failuresBeforeSuccess.set(7);
    flusher.flush(events(3), Map.of());

    flusher.executeScheduledFlush();

    assertEquals(8, flusher.sendAttempts.get());
    assertEquals(List.of(record(1), record(2), record(3)), flusher.sentRecords());
    assertEquals(new Counts(3, 33, 0), counts());
    flusher.close();
  }

  @Test
  void inlineFlushRetriesADestinationFailureUntilDelivered() throws Exception {
    FakeFlusher flusher = new FakeFlusher(2);
    flusher.enableStoreForward(SinkStoreForward.noop(), IO_IS_DESTINATION);
    flusher.failuresBeforeSuccess.set(5);

    SimpleSinkCommand.FlushResult result = flusher.flush(events(2), Map.of());

    assertEquals(6, flusher.sendAttempts.get());
    assertEquals(2, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    flusher.close();
  }

  @Test
  void aRecordProblemStillGivesUpAfterThreeAttempts() throws Exception {
    FakeFlusher flusher = new FakeFlusher(100);
    flusher.enableStoreForward(SinkStoreForward.noop(), IO_IS_DESTINATION);
    flusher.initialize();
    flusher.failure = new IllegalArgumentException("malformed record");
    flusher.flush(events(2), Map.of());

    flusher.executeScheduledFlush();

    // three attempts on the batch, then three on the records that passed validation
    assertEquals(6, flusher.sendAttempts.get());
    assertEquals(new Counts(0, 0, 2), counts());
    flusher.close();
  }

  @Test
  void withoutAClassifierEveryFailureGivesUpAfterThreeAttempts() throws Exception {
    FakeFlusher flusher = new FakeFlusher(100);
    flusher.initialize();
    flusher.failure = new IOException("connection refused");
    flusher.flush(events(1), Map.of());

    flusher.executeScheduledFlush();

    assertEquals(6, flusher.sendAttempts.get());
    assertEquals(new Counts(0, 0, 1), counts());
    flusher.close();
  }

  @Test
  void aLocalStoreKeepsTheRecordsAfterThreeAttemptsInstead() throws Exception {
    FakeFlusher flusher = new FakeFlusher(100);
    RecordingStore store = new RecordingStore();
    flusher.enableStoreForward(store, IO_IS_DESTINATION);
    flusher.initialize();
    flusher.failure = new IOException("connection refused");
    flusher.flush(events(2), Map.of());

    flusher.executeScheduledFlush();

    assertEquals(3, flusher.sendAttempts.get());
    assertEquals(List.of(record(1), record(2)), store.stored);
    assertEquals(new Counts(0, 0, 0), counts());
    flusher.close();
  }

  @Test
  void testModeGivesUpAfterThreeAttempts() throws Exception {
    HashMap<String, Serializable> properties = new HashMap<>();
    properties.put(JobContext.FLAG_TEST_MODE, true);
    FakeFlusher flusher =
        new FakeFlusher(100, JobContext.builder().otherProperties(properties).build());
    flusher.enableStoreForward(SinkStoreForward.noop(), IO_IS_DESTINATION);
    flusher.failure = new IOException("connection refused");

    SimpleSinkCommand.FlushResult result = flusher.flush(events(1), Map.of());

    assertEquals(6, flusher.sendAttempts.get());
    assertEquals(1, result.errorOutputList().size());
    flusher.close();
  }

  @Test
  void replayThroughFlushNowLeavesTheRetryingToTheStore() throws Exception {
    FakeFlusher flusher = new FakeFlusher(100);
    flusher.enableStoreForward(SinkStoreForward.noop(), IO_IS_DESTINATION);
    flusher.failure = new IOException("connection refused");

    assertThrows(IOException.class, () -> flusher.flushNow(events(1), Map.of()));
    assertEquals(3, flusher.sendAttempts.get());
    flusher.close();
  }

  @Test
  void closingStopsTheRetriesAndCountsTheBatchAsLost() throws Exception {
    FakeFlusher flusher = new FakeFlusher(100);
    flusher.enableStoreForward(SinkStoreForward.noop(), IO_IS_DESTINATION);
    flusher.initialize();
    flusher.failure = new IOException("connection refused");
    flusher.flush(events(2), Map.of());
    CountDownLatch retrying = new CountDownLatch(10);
    flusher.onAttempt = retrying::countDown;
    Thread timer = new Thread(flusher::executeScheduledFlush);
    timer.start();
    assertTrue(retrying.await(5, TimeUnit.SECONDS));

    flusher.close();
    timer.join(5_000);

    assertFalse(timer.isAlive());
    assertEquals(new Counts(0, 0, 2), counts());
  }

  @Test
  void anInterruptStopsTheRetries() throws Exception {
    FakeFlusher flusher = new FakeFlusher(100);
    flusher.enableStoreForward(SinkStoreForward.noop(), IO_IS_DESTINATION);
    flusher.delayMillis = 60_000L;
    flusher.initialize();
    flusher.failure = new IOException("connection refused");
    flusher.flush(events(1), Map.of());
    CountDownLatch attempted = new CountDownLatch(1);
    flusher.onAttempt = attempted::countDown;
    Thread timer = new Thread(flusher::executeScheduledFlush);
    timer.start();
    assertTrue(attempted.await(5, TimeUnit.SECONDS));

    timer.interrupt();
    timer.join(5_000);

    assertFalse(timer.isAlive());
    assertEquals(new Counts(0, 0, 1), counts());
    flusher.close();
  }

  @Test
  void retryDelayDoublesFromOneSecondAndCapsAtThirty() {
    FakeFlusher flusher = new FakeFlusher(1);
    flusher.delayMillis = null;

    assertEquals(
        List.of(1_000L, 2_000L, 4_000L, 16_000L, 30_000L, 30_000L),
        List.of(
            flusher.retryDelayMillis(1),
            flusher.retryDelayMillis(2),
            flusher.retryDelayMillis(3),
            flusher.retryDelayMillis(5),
            flusher.retryDelayMillis(6),
            flusher.retryDelayMillis(400)));
    flusher.close();
  }

  private record Counts(long sinkOutput, long outputSize, long sinkError) {}

  private Counts counts() {
    return new Counts(sinkOutput.total(), outputSize.total(), sinkError.total());
  }

  private static RecordFleakData record(int id) {
    return (RecordFleakData) FleakData.wrap(Map.of("id", id));
  }

  private static SimpleSinkCommand.PreparedInputEvents<RecordFleakData> events(int count) {
    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    for (int i = 1; i <= count; i++) {
      events.add(record(i), record(i));
    }
    return events;
  }

  private final class FakeFlusher extends AbstractBufferedFlusher<RecordFleakData> {
    private final int batchSize;
    private final List<List<RecordFleakData>> sent = new CopyOnWriteArrayList<>();
    final AtomicInteger sendAttempts = new AtomicInteger();
    final AtomicInteger failuresBeforeSuccess = new AtomicInteger();
    volatile Exception failure;
    volatile Runnable onAttempt = () -> {};

    /** Null uses the production backoff. */
    volatile Long delayMillis = 0L;

    FakeFlusher(int batchSize) {
      this(batchSize, null);
    }

    FakeFlusher(int batchSize, JobContext jobContext) {
      super(null, jobContext, "node", sinkOutput, outputSize, sinkError);
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
    protected long retryDelayMillis(int attempt) {
      Long fixed = delayMillis;
      return fixed == null ? super.retryDelayMillis(attempt) : fixed;
    }

    @Override
    protected SimpleSinkCommand.FlushResult doFlush(
        List<Pair<RecordFleakData, RecordFleakData>> batch) throws Exception {
      sendAttempts.incrementAndGet();
      onAttempt.run();
      if (failuresBeforeSuccess.getAndUpdate(left -> Math.max(0, left - 1)) > 0) {
        throw new IOException("connection refused");
      }
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

  private static final class RecordingStore implements SinkStoreForward {
    final List<RecordFleakData> stored = new CopyOnWriteArrayList<>();

    @Override
    public boolean isBuffering() {
      return false;
    }

    @Override
    public boolean shouldBuffer(Throwable t) {
      return t instanceof IOException;
    }

    @Override
    public int offer(List<RecordFleakData> records) {
      stored.addAll(records);
      return records.size();
    }

    @Override
    public void start(ReplayTarget target) {}

    @Override
    public void close() {}
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
