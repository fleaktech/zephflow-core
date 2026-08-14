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
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

/**
 * Pins which flush paths call {@link AbstractBufferedFlusher#reportMetrics}. There are two ways a
 * buffered flush gets counted and they are mutually exclusive:
 *
 * <ul>
 *   <li>Batch-size-triggered ("inline"): the {@code FlushResult} is returned to {@link
 *       SimpleSinkCommand}, which counts it. {@code reportMetrics} must NOT fire, or the sink
 *       double-counts {@code output_event_size} and {@code sink_output_count}.
 *   <li>Timer-driven and close-time ("out of band"): the result goes nowhere, so {@code
 *       reportMetrics} is the only thing that can count it and it MUST fire.
 * </ul>
 */
class AbstractBufferedFlusherMetricsTest {

  private static final long BYTES_PER_RECORD = 11L;

  private final FleakCounter sinkOutputCounter = mock(FleakCounter.class);
  private final FleakCounter outputSizeCounter = mock(FleakCounter.class);
  private final FleakCounter sinkErrorCounter = mock(FleakCounter.class);

  private TestFlusher newFlusher(int batchSize) {
    return new TestFlusher(batchSize, sinkOutputCounter, outputSizeCounter, sinkErrorCounter);
  }

  private static class TestFlusher extends AbstractBufferedFlusher<RecordFleakData> {
    private final int batchSize;

    TestFlusher(
        int batchSize,
        FleakCounter sinkOutputCounter,
        FleakCounter outputSizeCounter,
        FleakCounter sinkErrorCounter) {
      super(null, null, "test-node", sinkOutputCounter, outputSizeCounter, sinkErrorCounter);
      this.batchSize = batchSize;
    }

    @Override
    protected int getBatchSize() {
      return batchSize;
    }

    /**
     * Long enough that the timer never fires on its own; tests drive it via executeScheduledFlush.
     */
    @Override
    protected long getFlushIntervalMs() {
      return 600_000;
    }

    @Override
    protected SimpleSinkCommand.FlushResult doFlush(
        List<Pair<RecordFleakData, RecordFleakData>> batch) {
      return new SimpleSinkCommand.FlushResult(
          batch.size(), batch.size() * BYTES_PER_RECORD, List.of());
    }

    @Override
    protected void ensureCanWriteRecord(RecordFleakData record) {}

    @Override
    public void close() {
      stopFlushTimer();
    }
  }

  private static SimpleSinkCommand.PreparedInputEvents<RecordFleakData> oneEvent() {
    RecordFleakData record = (RecordFleakData) FleakData.wrap(Map.of("id", 1));
    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    events.add(record, record);
    return events;
  }

  @Test
  void inlineFlush_returnsSizeAndDoesNotReport() throws Exception {
    try (TestFlusher flusher = newFlusher(1)) {
      flusher.initialize();

      SimpleSinkCommand.FlushResult result = flusher.flush(oneEvent(), Map.of());

      // The caller (SimpleSinkCommand) gets the size and counts it...
      assertEquals(1, result.successCount());
      assertEquals(BYTES_PER_RECORD, result.flushedDataSize());
      // ...so reporting here too would double-count.
      verify(sinkOutputCounter, never()).increase(anyLong(), anyMap());
      verify(outputSizeCounter, never()).increase(anyLong(), anyMap());
    }
  }

  @Test
  void bufferedBelowBatchSize_reportsNothingAndReturnsZero() throws Exception {
    try (TestFlusher flusher = newFlusher(10)) {
      flusher.initialize();

      SimpleSinkCommand.FlushResult result = flusher.flush(oneEvent(), Map.of());

      assertEquals(0, result.successCount());
      assertEquals(0, result.flushedDataSize());
      verify(sinkOutputCounter, never()).increase(anyLong(), anyMap());
      verify(outputSizeCounter, never()).increase(anyLong(), anyMap());
    }
  }

  @Test
  void timerFlush_reportsExactlyOnce() throws Exception {
    try (TestFlusher flusher = newFlusher(10)) {
      flusher.initialize();
      flusher.flush(oneEvent(), Map.of());
      flusher.flush(oneEvent(), Map.of());
      verify(sinkOutputCounter, never()).increase(anyLong(), anyMap());

      flusher.executeScheduledFlush();

      verify(sinkOutputCounter).increase(2L, Map.of());
      verify(outputSizeCounter).increase(2 * BYTES_PER_RECORD, Map.of());
      verifyNoMoreInteractions(sinkOutputCounter, outputSizeCounter);
    }
  }

  /**
   * A batch that flushes inline and a later batch that flushes on the timer must produce exactly
   * one count each — this is the combination that used to double-count the inline half.
   */
  @Test
  void mixedInlineAndTimerFlushes_eachCountedOnce() throws Exception {
    try (TestFlusher flusher = newFlusher(2)) {
      flusher.initialize();

      // Two events reach batchSize=2 -> inline flush, counted by SimpleSinkCommand via the result.
      flusher.flush(oneEvent(), Map.of());
      SimpleSinkCommand.FlushResult inline = flusher.flush(oneEvent(), Map.of());
      assertEquals(2 * BYTES_PER_RECORD, inline.flushedDataSize());
      verify(sinkOutputCounter, never()).increase(anyLong(), anyMap());

      // A third event stays buffered until the timer fires.
      flusher.flush(oneEvent(), Map.of());
      flusher.executeScheduledFlush();

      verify(sinkOutputCounter).increase(1L, Map.of());
      verify(outputSizeCounter).increase(BYTES_PER_RECORD, Map.of());
      verifyNoMoreInteractions(sinkOutputCounter, outputSizeCounter);
    }
  }

  @Test
  void reportMetrics_passesResultAndTagsToCounters() throws Exception {
    try (TestFlusher flusher = newFlusher(10)) {
      Map<String, String> metricTags = Map.of("tenant_id", "789");

      flusher.reportMetrics(new SimpleSinkCommand.FlushResult(200, 10000L, List.of()), metricTags);
      flusher.reportErrorMetrics(5, metricTags);

      verify(sinkOutputCounter).increase(200L, metricTags);
      verify(outputSizeCounter).increase(10000L, metricTags);
      verify(sinkErrorCounter).increase(5L, metricTags);
    }
  }
}
