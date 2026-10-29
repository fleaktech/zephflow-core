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

import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_TAG_ENV;
import static io.fleak.zephflow.lib.utils.MiscUtils.METRIC_TAG_SERVICE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ExecutionContext;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.execution.ExecutionHooks;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.io.Serializable;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SimpleSinkSpillTest {

  @Test
  void slowSinkSpillsToDiskAndDrainsInTheBackground(@TempDir Path dir) throws Exception {
    RecordingFlusher flusher = new RecordingFlusher(50);
    CountingCounter sinkOutput = new CountingCounter();
    SpillSink sink = new SpillSink(jobContext(dir, "20"), flusher, null, sinkOutput);
    sink.initialize(new MetricClientProvider.NoopMetricClientProvider());
    ExecutionContext context = sink.getExecutionContext();

    sink.writeToSink(List.of(record(0)), "user", context);
    CompletableFuture.runAsync(
            () -> sink.writeToSink(List.of(record(1), record(2)), "user", context))
        .get(5, TimeUnit.SECONDS);
    assertEquals(List.of(record(0)), flusher.delivered);

    flusher.laterFlushes.countDown();
    await(() -> flusher.delivered.size() == 3);
    assertEquals(List.of(record(0), record(1), record(2)), flusher.delivered);
    await(() -> sinkOutput.total() == 3);
    sink.terminate();
  }

  @Test
  void initializeAddsADiskBufferWhenSpillIsOn(@TempDir Path dir) throws Exception {
    SpillSink sink = new SpillSink(jobContext(dir, "20"), new RecordingFlusher(0), null, null);
    sink.initialize(new MetricClientProvider.NoopMetricClientProvider());

    assertInstanceOf(ChronicleStoreForward.class, storeForward(sink));
    sink.terminate();
  }

  @Test
  void initializeKeepsTheSinksOwnStoreForward(@TempDir Path dir) throws Exception {
    SinkStoreForward own =
        new ChronicleStoreForward(
            new ChronicleStoreForward.Config(dir.resolve("own"), 1000, 50, 10, "node"),
            t -> false,
            null,
            null,
            null);
    SpillSink sink = new SpillSink(jobContext(dir, "20"), new RecordingFlusher(0), own, null);
    sink.initialize(new MetricClientProvider.NoopMetricClientProvider());

    assertSame(own, storeForward(sink));
    sink.terminate();
  }

  @Test
  void noDiskBufferWhenSpillIsOff(@TempDir Path dir) throws Exception {
    SpillSink sink = new SpillSink(jobContext(dir, null), new RecordingFlusher(0), null, null);
    sink.initialize(new MetricClientProvider.NoopMetricClientProvider());

    assertSame(SinkStoreForward.noop(), storeForward(sink));
    sink.terminate();
  }

  @Test
  void slowSinkWritesDirectlyWhenSpillIsOff(@TempDir Path dir) throws Exception {
    RecordingFlusher flusher = new RecordingFlusher(50);
    SpillSink sink = new SpillSink(jobContext(dir, null), flusher, null, null);
    sink.initialize(new MetricClientProvider.NoopMetricClientProvider());
    ExecutionContext context = sink.getExecutionContext();

    sink.writeToSink(List.of(record(0)), "user", context);
    flusher.laterFlushes.countDown();
    sink.writeToSink(List.of(record(1)), "user", context);

    assertEquals(List.of(record(0), record(1)), flusher.delivered);
    sink.terminate();
  }

  @Test
  void spilledRecordsAreRetriedThreeTimesThenDropped(@TempDir Path dir) throws Exception {
    RecordingFlusher flusher = new RecordingFlusher(50);
    flusher.failLaterFlushes = true;
    SpillSink sink = new SpillSink(jobContext(dir, "20"), flusher, null, null);
    sink.initialize(new MetricClientProvider.NoopMetricClientProvider());
    ExecutionContext context = sink.getExecutionContext();

    sink.writeToSink(List.of(record(0)), "user", context);
    sink.writeToSink(List.of(record(1)), "user", context);
    flusher.laterFlushes.countDown();

    await(() -> !storeForward(sink).isBuffering(), 60);
    assertEquals(4, flusher.failedFlushes.get());
    sink.terminate();
  }

  @Test
  void spilledRecordThatFailsPreprocessingIsCountedAsAnError(@TempDir Path dir) throws Exception {
    RecordingFlusher flusher = new RecordingFlusher(50);
    SpillSink sink = new SpillSink(jobContext(dir, "20"), flusher, null, null);
    AtomicInteger preprocessCalls = new AtomicInteger();
    sink.preProcessor =
        (event, ts) -> {
          if (preprocessCalls.incrementAndGet() > 2) {
            throw new IllegalArgumentException("bad record");
          }
          return event;
        };
    sink.initialize(new MetricClientProvider.NoopMetricClientProvider());
    ExecutionContext context = sink.getExecutionContext();

    sink.writeToSink(List.of(record(0)), "user", context);
    sink.writeToSink(List.of(record(1)), "user", context);
    flusher.laterFlushes.countDown();

    await(() -> sink.errorCounter.total() == 1);
    await(() -> !storeForward(sink).isBuffering());
    assertEquals(List.of(record(0)), flusher.delivered);
    sink.terminate();
  }

  @Test
  void boundedRunDoesNotSpill(@TempDir Path dir) throws Exception {
    SpillSink sink = new SpillSink(jobContext(dir, "20"), new RecordingFlusher(0), null, null);
    sink.setExecutionHooks(
        new ExecutionHooks(() -> {}, mock(ExecutionHooks.Effects.class), Runnable::run));
    sink.initialize(new MetricClientProvider.NoopMetricClientProvider());

    assertSame(SinkStoreForward.noop(), storeForward(sink));
    sink.terminate();
  }

  @Test
  void spillAfterMillisParsesThePositiveSetting(@TempDir Path dir) {
    assertEquals(30, SimpleSinkCommand.spillAfterMillis(jobContext(dir, " 30 ")));
  }

  @Test
  void spillAfterMillisIsOffForMissingOrInvalidSettings(@TempDir Path dir) {
    assertEquals(0, SimpleSinkCommand.spillAfterMillis(null));
    assertEquals(
        0, SimpleSinkCommand.spillAfterMillis(JobContext.builder().otherProperties(null).build()));
    assertEquals(0, SimpleSinkCommand.spillAfterMillis(jobContext(dir, "abc")));
    assertEquals(0, SimpleSinkCommand.spillAfterMillis(jobContext(dir, "0")));
    assertEquals(0, SimpleSinkCommand.spillAfterMillis(jobContext(dir, "-5")));
  }

  private static SinkStoreForward storeForward(SpillSink sink) {
    return ((SinkExecutionContext<?>) sink.getExecutionContext()).storeForward();
  }

  private static JobContext jobContext(Path dir, String spillAfterMillis) {
    Map<String, Serializable> properties = new HashMap<>();
    properties.put(JobContext.STORE_FORWARD_DIR, dir.toString());
    if (spillAfterMillis != null) {
      properties.put(JobContext.SINK_SPILL_AFTER_MS, spillAfterMillis);
    }
    return JobContext.builder()
        .otherProperties(properties)
        .metricTags(Map.of(METRIC_TAG_SERVICE, "test", METRIC_TAG_ENV, "test"))
        .build();
  }

  private static RecordFleakData record(int val) {
    return (RecordFleakData) FleakData.wrap(Map.of("val", val));
  }

  private static void await(BooleanSupplier condition) throws InterruptedException {
    await(condition, 10);
  }

  private static void await(BooleanSupplier condition, int timeoutSeconds)
      throws InterruptedException {
    long deadline = System.nanoTime() + java.time.Duration.ofSeconds(timeoutSeconds).toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(20);
    }
    fail("condition not met within timeout");
  }

  private static final class SpillSink extends SimpleSinkCommand<RecordFleakData> {
    private final RecordingFlusher flusher;
    private final SinkStoreForward ownStoreForward;
    private final FleakCounter sinkOutputCounter;
    final CountingCounter errorCounter = new CountingCounter();
    SinkMessagePreProcessor<RecordFleakData> preProcessor = (event, ts) -> event;

    SpillSink(
        JobContext jobContext,
        RecordingFlusher flusher,
        SinkStoreForward ownStoreForward,
        FleakCounter sinkOutputCounter) {
      super("node", jobContext, null, null);
      this.flusher = flusher;
      this.ownStoreForward = ownStoreForward;
      this.sinkOutputCounter = sinkOutputCounter;
    }

    @Override
    protected int batchSize() {
      return 100;
    }

    @Override
    public String commandName() {
      return "spill";
    }

    @Override
    protected ExecutionContext createExecutionContext(
        MetricClientProvider metricClientProvider,
        JobContext jobContext,
        CommandConfig commandConfig,
        String nodeId) {
      FleakCounter noop = new CountingCounter();
      return new SinkExecutionContext<>(
          flusher,
          preProcessor,
          ownStoreForward == null ? SinkStoreForward.noop() : ownStoreForward,
          noop,
          errorCounter,
          sinkOutputCounter == null ? noop : sinkOutputCounter,
          noop,
          noop);
    }
  }

  private static final class RecordingFlusher
      implements SimpleSinkCommand.Flusher<RecordFleakData> {
    private final long firstFlushDelayMillis;
    final List<RecordFleakData> delivered = new CopyOnWriteArrayList<>();
    final CountDownLatch laterFlushes = new CountDownLatch(1);
    final AtomicInteger failedFlushes = new AtomicInteger();
    volatile boolean failLaterFlushes = false;

    RecordingFlusher(long firstFlushDelayMillis) {
      this.firstFlushDelayMillis = firstFlushDelayMillis;
    }

    @Override
    public SimpleSinkCommand.FlushResult flush(
        SimpleSinkCommand.PreparedInputEvents<RecordFleakData> preparedInputEvents,
        Map<String, String> metricTags)
        throws InterruptedException {
      if (delivered.isEmpty()) {
        Thread.sleep(firstFlushDelayMillis);
      } else {
        laterFlushes.await();
        if (failLaterFlushes) {
          failedFlushes.incrementAndGet();
          throw new IllegalStateException("remote rejected the write");
        }
      }
      delivered.addAll(preparedInputEvents.preparedList());
      return new SimpleSinkCommand.FlushResult(
          preparedInputEvents.preparedList().size(), 0, new ArrayList<>());
    }

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
