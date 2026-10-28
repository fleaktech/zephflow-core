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
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.ScalarSinkCommand.SinkResult;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Covers the two store-and-forward touch points in {@link SimpleSinkCommand}. */
class SimpleSinkCommandStoreForwardTest {

  private static RecordFleakData record(int val) {
    return (RecordFleakData) FleakData.wrap(Map.of("val", val));
  }

  @Test
  void connectivityFailureBuffersPreparedRecords() {
    FakeStoreForward storeForward = new FakeStoreForward();
    storeForward.shouldBuffer = true;
    FakeFlusher flusher = new FakeFlusher(true); // always throws -> connectivity
    SinkExecutionContext<Integer> ctx = context(flusher, storeForward);

    List<RecordFleakData> input = List.of(record(0), record(3));
    SinkResult result = new FakeSink().writeToSink(input, "user", ctx);

    // both records preprocessed cleanly, flush failed, both buffered -> counted as success
    assertEquals(new SinkResult(2, 2, new ArrayList<>()), result);
    assertEquals(input, storeForward.offered);
    assertTrue(flusher.flushAttempted);
  }

  @Test
  void whileBufferingEventsGoStraightToDiskWithoutFlushing() {
    FakeStoreForward storeForward = new FakeStoreForward();
    storeForward.buffering = true;
    FakeFlusher flusher = new FakeFlusher(false);
    SinkExecutionContext<Integer> ctx = context(flusher, storeForward);

    List<RecordFleakData> input = List.of(record(0), record(3));
    SinkResult result = new FakeSink().writeToSink(input, "user", ctx);

    assertEquals(new SinkResult(2, 2, new ArrayList<>()), result);
    assertEquals(input, storeForward.offered);
    assertFalse(flusher.flushAttempted);
  }

  @Test
  void capDropOverflowIsReportedAsErrors() {
    FakeStoreForward storeForward = new FakeStoreForward();
    storeForward.buffering = true;
    storeForward.storeLimit = 1; // only the first record fits
    SinkExecutionContext<Integer> ctx = context(new FakeFlusher(false), storeForward);

    List<RecordFleakData> input = List.of(record(0), record(3));
    SinkResult result = new FakeSink().writeToSink(input, "user", ctx);

    SinkResult expected =
        new SinkResult(
            2,
            1,
            List.of(
                new ErrorOutput(
                    record(3), "store-and-forward local buffer is full; record dropped")));
    assertEquals(expected, result);
  }

  @Test
  void slowFlushSpillsTheNextWriteToDisk() {
    FakeStoreForward storeForward = new FakeStoreForward();
    FakeFlusher flusher = new FakeFlusher(false);
    flusher.delayMillis = 50;
    SinkExecutionContext<Integer> ctx = context(flusher, storeForward);
    FakeSink sink = new FakeSink(spillAfterMillis(10));

    sink.writeToSink(List.of(record(0)), "user", ctx);
    flusher.flushAttempted = false;
    List<RecordFleakData> next = List.of(record(1), record(2));
    SinkResult result = sink.writeToSink(next, "user", ctx);

    assertEquals(new SinkResult(2, 2, new ArrayList<>()), result);
    assertEquals(next, storeForward.spilled);
    assertFalse(flusher.flushAttempted);
  }

  @Test
  void fastFlushKeepsWritingDirectly() {
    FakeStoreForward storeForward = new FakeStoreForward();
    FakeFlusher flusher = new FakeFlusher(false);
    SinkExecutionContext<Integer> ctx = context(flusher, storeForward);
    FakeSink sink = new FakeSink(spillAfterMillis(10_000));

    sink.writeToSink(List.of(record(0)), "user", ctx);
    flusher.flushAttempted = false;
    sink.writeToSink(List.of(record(1)), "user", ctx);

    assertTrue(flusher.flushAttempted);
    assertTrue(storeForward.spilled.isEmpty());
  }

  @Test
  void slowFlushKeepsWritingDirectlyWhenSpillIsOff() {
    FakeStoreForward storeForward = new FakeStoreForward();
    FakeFlusher flusher = new FakeFlusher(false);
    flusher.delayMillis = 50;
    SinkExecutionContext<Integer> ctx = context(flusher, storeForward);
    FakeSink sink = new FakeSink(null);

    sink.writeToSink(List.of(record(0)), "user", ctx);
    flusher.flushAttempted = false;
    sink.writeToSink(List.of(record(1)), "user", ctx);

    assertTrue(flusher.flushAttempted);
    assertTrue(storeForward.spilled.isEmpty());
  }

  @Test
  void slowFailedFlushDoesNotSpill() {
    FakeStoreForward storeForward = new FakeStoreForward();
    FakeFlusher flusher = new FakeFlusher(true);
    flusher.delayMillis = 50;
    SinkExecutionContext<Integer> ctx = context(flusher, storeForward);
    FakeSink sink = new FakeSink(spillAfterMillis(10));

    sink.writeToSink(List.of(record(0)), "user", ctx);
    flusher.flushAttempted = false;
    sink.writeToSink(List.of(record(1)), "user", ctx);

    assertTrue(flusher.flushAttempted);
    assertTrue(storeForward.spilled.isEmpty());
  }

  @Test
  void slowFlushInTheMiddleOfAWriteSpillsTheRestOfIt() {
    FakeStoreForward storeForward = new FakeStoreForward();
    FakeFlusher flusher = new FakeFlusher(false);
    flusher.delayMillis = 50;
    SinkExecutionContext<Integer> ctx = context(flusher, storeForward);
    FakeSink sink = new FakeSink(spillAfterMillis(10), 1);

    SinkResult result = sink.writeToSink(List.of(record(0), record(1), record(2)), "user", ctx);

    assertEquals(new SinkResult(3, 3, new ArrayList<>()), result);
    assertEquals(1, flusher.flushCount);
    assertEquals(List.of(record(1)), storeForward.spilled);
    assertEquals(List.of(record(2)), storeForward.offered);
  }

  @Test
  void recordsThatFailPreprocessingAreNotBuffered() {
    FakeStoreForward storeForward = new FakeStoreForward();
    storeForward.buffering = true;
    SinkExecutionContext<Integer> ctx = context(new FakeFlusher(false), storeForward);
    RecordFleakData bad = (RecordFleakData) FleakData.wrap(Map.of("val", "not a number"));

    SinkResult result = new FakeSink().writeToSink(List.of(record(0), bad), "user", ctx);

    assertEquals(2, result.getInputCount());
    assertEquals(1, result.getSuccessCount());
    assertEquals(bad, result.getFailureEvents().getFirst().inputEvent());
    assertEquals(List.of(record(0)), storeForward.offered);
  }

  private static JobContext spillAfterMillis(long millis) {
    return JobContext.builder()
        .otherProperties(
            new HashMap<>(
                Map.<String, Serializable>of(
                    JobContext.SINK_SPILL_AFTER_MS, String.valueOf(millis))))
        .build();
  }

  private SinkExecutionContext<Integer> context(
      FakeFlusher flusher, SinkStoreForward storeForward) {
    FleakCounter noop = new NoopCounter();
    return new SinkExecutionContext<>(
        flusher,
        (event, ts) -> ((Number) event.unwrap().get("val")).intValue(),
        storeForward,
        noop,
        noop,
        noop,
        noop,
        noop);
  }

  private static final class FakeSink extends SimpleSinkCommand<Integer> {
    private final int batchSize;

    FakeSink() {
      this(null);
    }

    FakeSink(JobContext jobContext) {
      this(jobContext, 100);
    }

    FakeSink(JobContext jobContext, int batchSize) {
      super("node", jobContext, null, null);
      this.batchSize = batchSize;
    }

    @Override
    protected int batchSize() {
      return batchSize;
    }

    @Override
    public String commandName() {
      return "fake";
    }

    @Override
    protected io.fleak.zephflow.api.ExecutionContext createExecutionContext(
        io.fleak.zephflow.api.metric.MetricClientProvider metricClientProvider,
        io.fleak.zephflow.api.JobContext jobContext,
        io.fleak.zephflow.api.CommandConfig commandConfig,
        String nodeId) {
      return null;
    }
  }

  private static final class FakeFlusher implements SimpleSinkCommand.Flusher<Integer> {
    private final boolean fail;
    boolean flushAttempted = false;
    int flushCount = 0;
    long delayMillis = 0;

    FakeFlusher(boolean fail) {
      this.fail = fail;
    }

    @Override
    public SimpleSinkCommand.FlushResult flush(
        SimpleSinkCommand.PreparedInputEvents<Integer> preparedInputEvents,
        Map<String, String> metricTags)
        throws InterruptedException {
      flushAttempted = true;
      flushCount++;
      Thread.sleep(delayMillis);
      if (fail) {
        throw new RuntimeException("connection down");
      }
      return new SimpleSinkCommand.FlushResult(
          preparedInputEvents.preparedList().size(), 0, new ArrayList<>());
    }

    @Override
    public void close() {}
  }

  private static final class FakeStoreForward implements SinkStoreForward {
    boolean buffering = false;
    boolean shouldBuffer = false;
    int storeLimit = Integer.MAX_VALUE;
    final List<RecordFleakData> offered = new ArrayList<>();
    final List<RecordFleakData> spilled = new ArrayList<>();

    @Override
    public boolean isBuffering() {
      return buffering;
    }

    @Override
    public boolean shouldBuffer(Throwable t) {
      return shouldBuffer;
    }

    @Override
    public int offer(List<RecordFleakData> records) {
      int stored = Math.min(records.size(), storeLimit);
      offered.addAll(records.subList(0, stored));
      return stored;
    }

    @Override
    public int spill(List<RecordFleakData> records) {
      spilled.addAll(records);
      buffering = true;
      return records.size();
    }

    @Override
    public void start(ReplayTarget target) {}

    @Override
    public void close() {}
  }

  private static final class NoopCounter implements FleakCounter {
    @Override
    public void increase(Map<String, String> additionalTags) {}

    @Override
    public void increase(long n, Map<String, String> additionalTags) {}
  }
}
