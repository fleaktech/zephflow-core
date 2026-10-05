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

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.execution.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class BoundedSimpleSinkTest {
  @Test
  void acceptedWriteThenLostResponseIsUnknownAndNotRetried() throws Exception {
    AtomicInteger accepted = new AtomicInteger();
    List<EffectOutcome> outcomes = new ArrayList<>();
    var flusher =
        new SimpleSinkCommand.Flusher<RecordFleakData>() {
          public SimpleSinkCommand.FlushResult flush(
              SimpleSinkCommand.PreparedInputEvents<RecordFleakData> input,
              Map<String, String> tags)
              throws IOException {
            accepted.addAndGet(input.preparedList().size());
            throw new IOException("response lost after accepting records");
          }

          public void close() {}
        };
    var command = command(flusher);
    command.setExecutionHooks(hooks(() -> {}, outcomes));
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
    var result = command.writeToSink(records(2), "user", command.getExecutionContext());
    command.abort();
    assertEquals(2, accepted.get());
    assertEquals(0, result.getSuccessCount());
    assertEquals(2, result.getFailureEvents().size());
    assertEquals(1, outcomes.size());
    assertEquals(2L, outcomes.getFirst().unknownCount());
    assertEquals(0L, outcomes.getFirst().definiteFailureCount());
  }

  @Test
  void stopAfterFirstBatchPublishesItsReceiptBeforeStoppingAndSendsNothingElse() throws Exception {
    AtomicBoolean stopped = new AtomicBoolean();
    AtomicInteger accepted = new AtomicInteger();
    List<EffectOutcome> outcomes = new ArrayList<>();
    var command =
        command(
            new SimpleSinkCommand.Flusher<>() {
              public SimpleSinkCommand.FlushResult flush(
                  SimpleSinkCommand.PreparedInputEvents<RecordFleakData> input,
                  Map<String, String> tags) {
                accepted.addAndGet(input.preparedList().size());
                stopped.set(true);
                return new SimpleSinkCommand.FlushResult(input.preparedList().size(), 5, List.of());
              }

              public void close() {}
            });
    command.setExecutionHooks(
        hooks(
            () -> {
              if (stopped.get()) throw new ExecutionStoppedException("cancelled");
            },
            outcomes));
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
    assertThrows(
        ExecutionStoppedException.class,
        () -> command.writeToSink(records(5), "user", command.getExecutionContext()));
    command.abort();
    assertEquals(2, accepted.get());
    assertEquals(1, outcomes.size());
    assertEquals(2L, outcomes.getFirst().acknowledgedCount());
  }

  @Test
  void overlappingErrorListCannotEraseKnownPositiveReceipt() {
    var receipt =
        new SimpleSinkCommand.FlushResult(
            1,
            4,
            records(2).stream()
                .map(record -> new ErrorOutput(record, "partial operation"))
                .toList());
    assertEquals(1L, receipt.boundedOutcome(2).acknowledgedCount());
    assertEquals(1L, receipt.boundedOutcome(2).unknownCount());
    assertEquals(0L, receipt.boundedOutcome(2).definiteFailureCount());
  }

  @Test
  void entirelyRejectedPreparationIsNotAnUnknownRemoteDelivery() throws Exception {
    List<EffectOutcome> outcomes = new ArrayList<>();
    var command =
        command(
            new SimpleSinkCommand.Flusher<RecordFleakData>() {
              public SimpleSinkCommand.FlushResult flush(
                  SimpleSinkCommand.PreparedInputEvents<RecordFleakData> input,
                  Map<String, String> tags) {
                fail("No valid prepared record may reach the sink");
                return null;
              }

              public void close() {}
            },
            (record, timestamp) -> {
              throw new IllegalArgumentException("invalid record");
            });
    command.setExecutionHooks(hooks(() -> {}, outcomes));
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
    var result = command.writeToSink(records(2), "user", command.getExecutionContext());
    command.abort();
    assertEquals(2, result.getFailureEvents().size());
    assertEquals(EffectOutcome.Delivery.FAILED, outcomes.getFirst().delivery());
    assertEquals(2L, outcomes.getFirst().definiteFailureCount());
    assertEquals(0L, outcomes.getFirst().attemptedCount());
    assertEquals(2L, outcomes.getFirst().notAttemptedCount());
    assertEquals(0L, outcomes.getFirst().unknownCount());
  }

  @Test
  void stopBetweenEffectAdmissionAndAdapterDoesNotInventAnAttempt() throws Exception {
    AtomicBoolean stopped = new AtomicBoolean();
    List<EffectOutcome> outcomes = new ArrayList<>();
    var command =
        command(
            new SimpleSinkCommand.Flusher<RecordFleakData>() {
              public SimpleSinkCommand.FlushResult flush(
                  SimpleSinkCommand.PreparedInputEvents<RecordFleakData> input,
                  Map<String, String> tags) {
                fail("Cancelled before adapter invocation");
                return null;
              }

              public void close() {}
            });
    command.setExecutionHooks(
        new ExecutionHooks(
            () -> {
              if (stopped.get()) throw new ExecutionStoppedException("cancelled");
            },
            new ExecutionHooks.Effects() {
              public long started() {
                stopped.set(true);
                return 1;
              }

              public void finished(long id, EffectOutcome outcome) {
                outcomes.add(outcome);
              }
            },
            Runnable::run));
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
    try {
      assertThrows(
          ExecutionStoppedException.class,
          () -> command.writeToSink(records(2), "user", command.getExecutionContext()));
      assertEquals(1, outcomes.size());
      assertEquals(0L, outcomes.getFirst().attemptedCount());
      assertEquals(0L, outcomes.getFirst().unknownCount());
      assertEquals(2L, outcomes.getFirst().notAttemptedCount());
      assertEquals(EffectOutcome.Delivery.NOT_ATTEMPTED, outcomes.getFirst().delivery());
    } finally {
      command.abort();
    }
  }

  @Test
  void stopPreservesErrorsFromCompletedAndCurrentBatchesExactlyOnce() throws Exception {
    AtomicInteger batches = new AtomicInteger();
    AtomicBoolean stopped = new AtomicBoolean();
    List<EffectOutcome> outcomes = new ArrayList<>();
    var command =
        command(
            new SimpleSinkCommand.Flusher<RecordFleakData>() {
              public SimpleSinkCommand.FlushResult flush(
                  SimpleSinkCommand.PreparedInputEvents<RecordFleakData> input,
                  Map<String, String> tags) {
                if (batches.incrementAndGet() == 2) stopped.set(true);
                return new SimpleSinkCommand.FlushResult(
                    1,
                    1,
                    List.of(new ErrorOutput(input.preparedList().getLast(), "rejected")),
                    new EffectOutcome(
                        2L, 1L, 1L, 0L, 0L, "test_receipt", EffectOutcome.Delivery.PARTIAL));
              }

              public void close() {}
            });
    command.setExecutionHooks(
        hooks(
            () -> {
              if (stopped.get()) throw new ExecutionStoppedException("cancelled");
            },
            outcomes));
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
    try {
      var failure =
          assertThrows(
              ExecutionProgressStoppedException.class,
              () -> command.writeToSink(records(6), "user", command.getExecutionContext()));
      assertEquals(2, batches.get());
      assertEquals(
          List.of(records(6).get(1), records(6).get(3)),
          failure.errors().stream().map(ErrorOutput::inputEvent).toList());
      assertEquals(2, outcomes.size());
      assertEquals(2, outcomes.stream().mapToLong(EffectOutcome::definiteFailureCount).sum());
    } finally {
      command.abort();
    }
  }

  @Test
  void stopDuringPreparationPreservesPreviousAcknowledgementAndKnownRejection() throws Exception {
    AtomicBoolean stopped = new AtomicBoolean();
    AtomicInteger preparedCount = new AtomicInteger();
    AtomicInteger delivered = new AtomicInteger();
    List<EffectOutcome> outcomes = new ArrayList<>();
    var input = records(6);
    var command =
        command(
            new SimpleSinkCommand.Flusher<RecordFleakData>() {
              public SimpleSinkCommand.FlushResult flush(
                  SimpleSinkCommand.PreparedInputEvents<RecordFleakData> events,
                  Map<String, String> tags) {
                delivered.addAndGet(events.preparedList().size());
                return new SimpleSinkCommand.FlushResult(
                    events.preparedList().size(), 1, List.of());
              }

              public void close() {}
            },
            (record, timestamp) -> {
              if (preparedCount.incrementAndGet() == 3) {
                stopped.set(true);
                throw new IllegalArgumentException("locally rejected");
              }
              return record;
            });
    command.setExecutionHooks(
        hooks(
            () -> {
              if (stopped.get()) throw new ExecutionStoppedException("cancelled");
            },
            outcomes));
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
    try {
      var failure =
          assertThrows(
              ExecutionProgressStoppedException.class,
              () -> command.writeToSink(input, "user", command.getExecutionContext()));
      assertEquals(
          List.of(input.get(2)), failure.errors().stream().map(ErrorOutput::inputEvent).toList());
      assertEquals(3, preparedCount.get());
      assertEquals(2, delivered.get());
      assertEquals(2, outcomes.size());
      assertEquals(2L, outcomes.getFirst().acknowledgedCount());
      var rejected = outcomes.getLast();
      assertEquals(0L, rejected.attemptedCount());
      assertEquals(1L, rejected.definiteFailureCount());
      assertEquals(0L, rejected.unknownCount());
      assertEquals(2L, rejected.notAttemptedCount());
      assertEquals(EffectOutcome.Delivery.FAILED, rejected.delivery());
    } finally {
      command.abort();
    }
  }

  @Test
  void opaqueAdapterStopDoesNotEraseEarlierLocalRejection() throws Exception {
    List<EffectOutcome> outcomes = new ArrayList<>();
    var input = records(2);
    var command =
        command(
            new SimpleSinkCommand.Flusher<RecordFleakData>() {
              public SimpleSinkCommand.FlushResult flush(
                  SimpleSinkCommand.PreparedInputEvents<RecordFleakData> events,
                  Map<String, String> tags) {
                assertEquals(List.of(input.get(1)), events.preparedList());
                throw new ExecutionStoppedException("opaque operation stopped after entry");
              }

              public void close() {}
            },
            (record, timestamp) -> {
              if (record.equals(input.getFirst())) throw new IllegalArgumentException("rejected");
              return record;
            });
    command.setExecutionHooks(hooks(() -> {}, outcomes));
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
    try {
      var failure =
          assertThrows(
              ExecutionProgressStoppedException.class,
              () -> command.writeToSink(input, "user", command.getExecutionContext()));
      assertEquals(
          List.of(input.getFirst()),
          failure.errors().stream().map(ErrorOutput::inputEvent).toList());
      assertEquals(1, outcomes.size());
      assertEquals(1L, outcomes.getFirst().attemptedCount());
      assertEquals(1L, outcomes.getFirst().definiteFailureCount());
      assertEquals(1L, outcomes.getFirst().unknownCount());
      assertEquals(1L, outcomes.getFirst().notAttemptedCount());
    } finally {
      command.abort();
    }
  }

  private static ExecutionHooks hooks(ExecutionControl control, List<EffectOutcome> outcomes) {
    return new ExecutionHooks(
        control,
        new ExecutionHooks.Effects() {
          long id;

          public long started() {
            control.checkpoint();
            return ++id;
          }

          public void finished(long id, EffectOutcome outcome) {
            outcomes.add(outcome);
          }
        },
        Runnable::run);
  }

  private static List<RecordFleakData> records(int size) {
    return java.util.stream.IntStream.range(0, size)
        .mapToObj(i -> (RecordFleakData) FleakData.wrap(Map.of("id", i)))
        .toList();
  }

  private static SimpleSinkCommand<RecordFleakData> command(
      SimpleSinkCommand.Flusher<RecordFleakData> flusher) {
    return command(flusher, (record, timestamp) -> record);
  }

  private static SimpleSinkCommand<RecordFleakData> command(
      SimpleSinkCommand.Flusher<RecordFleakData> flusher,
      SimpleSinkCommand.SinkMessagePreProcessor<RecordFleakData> preprocessor) {
    return new SimpleSinkCommand<>(
        "sink",
        JobContext.builder().build(),
        config -> new CommandConfig() {},
        (config, node, job) -> {}) {
      protected int batchSize() {
        return 2;
      }

      public String commandName() {
        return "test-sink";
      }

      protected ExecutionContext createExecutionContext(
          MetricClientProvider metrics, JobContext job, CommandConfig config, String node) {
        FleakCounter counter = new MetricClientProvider.NoopMetricClientProvider.NoopFleakCounter();
        return new SinkExecutionContext<>(
            flusher, preprocessor, counter, counter, counter, counter, counter);
      }
    };
  }
}
