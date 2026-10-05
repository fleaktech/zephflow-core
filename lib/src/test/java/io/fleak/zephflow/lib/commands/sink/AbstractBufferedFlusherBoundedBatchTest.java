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
import static org.mockito.Mockito.mock;

import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.execution.EffectOutcome;
import io.fleak.zephflow.api.execution.ExecutionHooks;
import io.fleak.zephflow.api.execution.ExecutionStoppedException;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

class AbstractBufferedFlusherBoundedBatchTest {
  @Test
  void partialAdapterExceptionKeepsItsReceiptPreviousSliceAndUnsentTail() throws Exception {
    var hooks = hooks();
    var stopped = new ExecutionStoppedException("stopped after partial second slice");
    try (var flusher = new RecordingFlusher(hooks, true, stopped)) {
      var input = events();
      var failure =
          assertThrows(
              BoundedFlushException.class, () -> flusher.flushBounded(input, Map.of(), hooks));
      assertSame(stopped, failure.getCause());
      assertEquals(List.of(2, 2), flusher.sizes);
      assertEquals(3, failure.result().successCount());
      assertEquals(30, failure.result().flushedDataSize());
      assertEquals(
          List.of(input.rawAndPreparedList().get(3).getLeft()),
          failure.result().errorOutputList().stream().map(ErrorOutput::inputEvent).toList());
      var receipt = failure.result().effectOutcome();
      assertEquals(4L, receipt.attemptedCount());
      assertEquals(3L, receipt.acknowledgedCount());
      assertEquals(0L, receipt.definiteFailureCount());
      assertEquals(1L, receipt.unknownCount());
      assertEquals(1L, receipt.notAttemptedCount());
      assertEquals(EffectOutcome.Delivery.PARTIAL, receipt.delivery());
      assertEquals(1, flusher.unlocks);
    }
  }

  @Test
  void explicitMiddleRejectionKeepsPreviousAndLaterSuccessWithoutRecovery() throws Exception {
    var hooks = hooks();
    try (var flusher = new RecordingFlusher(hooks, false, null)) {
      var input = events();
      var result = flusher.flushBounded(input, Map.of(), hooks);
      assertEquals(List.of(2, 2, 1), flusher.sizes);
      assertEquals(3, result.successCount());
      assertEquals(30, result.flushedDataSize());
      assertEquals(
          input.rawAndPreparedList().subList(2, 4).stream().map(Pair::getLeft).toList(),
          result.errorOutputList().stream().map(ErrorOutput::inputEvent).toList());
      var receipt = result.effectOutcome();
      assertEquals(5L, receipt.attemptedCount());
      assertEquals(3L, receipt.acknowledgedCount());
      assertEquals(2L, receipt.definiteFailureCount());
      assertEquals(0L, receipt.unknownCount());
      assertEquals(0L, receipt.notAttemptedCount());
      assertEquals(EffectOutcome.Delivery.PARTIAL, receipt.delivery());
      assertEquals(1, flusher.unlocks);
    }
  }

  private static SimpleSinkCommand.PreparedInputEvents<RecordFleakData> events() {
    var result = new SimpleSinkCommand.PreparedInputEvents<RecordFleakData>();
    for (int index = 0; index < 5; index++) {
      var record = (RecordFleakData) FleakData.wrap(Map.of("id", "event-" + index));
      result.add(record, record);
    }
    return result;
  }

  private static ExecutionHooks hooks() {
    return new ExecutionHooks(
        () -> {},
        new ExecutionHooks.Effects() {
          public long started() {
            throw new AssertionError("caller owns receipt publication");
          }

          public void finished(long id, EffectOutcome outcome) {
            throw new AssertionError("caller owns receipt publication");
          }
        },
        Runnable::run);
  }

  private static class RecordingFlusher extends AbstractBufferedFlusher<RecordFleakData> {
    private final List<Integer> sizes = new ArrayList<>();
    private final boolean partialStop;
    private final ExecutionStoppedException stopped;
    private int unlocks;

    private RecordingFlusher(
        ExecutionHooks hooks, boolean partialStop, ExecutionStoppedException stopped) {
      super(
          null,
          JobContext.builder().executionHooks(hooks).build(),
          "fixture",
          mock(FleakCounter.class),
          mock(FleakCounter.class),
          mock(FleakCounter.class));
      this.partialStop = partialStop;
      this.stopped = stopped;
    }

    protected int getBatchSize() {
      return 2;
    }

    protected void ensureCanWriteRecord(RecordFleakData record) {}

    protected void afterWrite() {
      unlocks++;
    }

    public void close() {}

    protected SimpleSinkCommand.FlushResult doFlush(
        List<Pair<RecordFleakData, RecordFleakData>> batch) {
      sizes.add(batch.size());
      if (sizes.size() != 2)
        return new SimpleSinkCommand.FlushResult(batch.size(), 10L * batch.size(), List.of());
      if (partialStop) {
        throw new BoundedFlushException(
            new SimpleSinkCommand.FlushResult(
                1,
                10,
                List.of(new ErrorOutput(batch.getLast().getLeft(), "response unavailable")),
                EffectOutcome.counted(2, 1, 0, 1, 0, "fixture")),
            stopped);
      }
      return new SimpleSinkCommand.FlushResult(
          0,
          0,
          batch.stream().map(item -> new ErrorOutput(item.getLeft(), "rejected")).toList(),
          EffectOutcome.counted(2, 0, 2, 0, 0, "fixture"));
    }
  }
}
