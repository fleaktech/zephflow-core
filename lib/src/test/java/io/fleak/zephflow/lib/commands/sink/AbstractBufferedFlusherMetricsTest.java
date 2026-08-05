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

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.ArrayList;
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

  /** Records every reportMetrics call so we can assert on how many happened, and from where. */
  private static class RecordingFlusher extends AbstractBufferedFlusher<RecordFleakData> {
    final List<SimpleSinkCommand.FlushResult> reported = new ArrayList<>();
    private final int batchSize;

    RecordingFlusher(int batchSize) {
      super(null, null, "test-node");
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
    protected void reportMetrics(
        SimpleSinkCommand.FlushResult result, Map<String, String> metricTags) {
      reported.add(result);
    }

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
    try (RecordingFlusher flusher = new RecordingFlusher(1)) {
      flusher.initialize();

      SimpleSinkCommand.FlushResult result = flusher.flush(oneEvent(), Map.of());

      // The caller (SimpleSinkCommand) gets the size and counts it...
      assertEquals(1, result.successCount());
      assertEquals(BYTES_PER_RECORD, result.flushedDataSize());
      // ...so reporting here too would double-count.
      assertTrue(
          flusher.reported.isEmpty(),
          "inline flush must not call reportMetrics; SimpleSinkCommand already counts the result");
    }
  }

  @Test
  void bufferedBelowBatchSize_reportsNothingAndReturnsZero() throws Exception {
    try (RecordingFlusher flusher = new RecordingFlusher(10)) {
      flusher.initialize();

      SimpleSinkCommand.FlushResult result = flusher.flush(oneEvent(), Map.of());

      assertEquals(0, result.successCount());
      assertEquals(0, result.flushedDataSize());
      assertTrue(flusher.reported.isEmpty());
    }
  }

  @Test
  void timerFlush_reportsExactlyOnce() throws Exception {
    try (RecordingFlusher flusher = new RecordingFlusher(10)) {
      flusher.initialize();
      flusher.flush(oneEvent(), Map.of());
      flusher.flush(oneEvent(), Map.of());
      assertTrue(flusher.reported.isEmpty(), "still buffered, nothing flushed yet");

      flusher.executeScheduledFlush();

      assertEquals(1, flusher.reported.size(), "timer flush must be reported exactly once");
      assertEquals(2, flusher.reported.getFirst().successCount());
      assertEquals(2 * BYTES_PER_RECORD, flusher.reported.getFirst().flushedDataSize());
    }
  }

  /**
   * A batch that flushes inline and a later batch that flushes on the timer must produce exactly
   * one count each — this is the combination that used to double-count the inline half.
   */
  @Test
  void mixedInlineAndTimerFlushes_eachCountedOnce() throws Exception {
    try (RecordingFlusher flusher = new RecordingFlusher(2)) {
      flusher.initialize();

      // Two events reach batchSize=2 -> inline flush, counted by SimpleSinkCommand via the result.
      flusher.flush(oneEvent(), Map.of());
      SimpleSinkCommand.FlushResult inline = flusher.flush(oneEvent(), Map.of());
      assertEquals(2 * BYTES_PER_RECORD, inline.flushedDataSize());
      assertTrue(flusher.reported.isEmpty());

      // A third event stays buffered until the timer fires.
      flusher.flush(oneEvent(), Map.of());
      flusher.executeScheduledFlush();

      assertEquals(1, flusher.reported.size());
      assertEquals(BYTES_PER_RECORD, flusher.reported.getFirst().flushedDataSize());
    }
  }
}
