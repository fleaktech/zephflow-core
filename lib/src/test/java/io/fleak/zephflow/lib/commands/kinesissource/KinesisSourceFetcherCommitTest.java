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
package io.fleak.zephflow.lib.commands.kinesissource;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.lib.commands.source.BatchCommitStrategy;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.commands.source.SourceLoopHarness;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.RecordProcessorCheckpointer;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

class KinesisSourceFetcherCommitTest {

  private final List<String> checkpoints = new ArrayList<>();

  @Test
  void commitThroughCheckpointsEachShardAtItsLastHandledRecord() throws Exception {
    KinesisSourceFetcher fetcher = new KinesisSourceFetcher();
    RecordProcessorCheckpointer shard1 = checkpointer("shard-1");
    RecordProcessorCheckpointer shard2 = checkpointer("shard-2");
    deliver(fetcher, "shard-1", shard1, record("1"), record("2"), noData("3"), record("4"));
    deliver(fetcher, "shard-2", shard2, record("10"));
    Fetcher.Committer committer = fetcher.committer();
    List<List<String>> perCommit = new ArrayList<>();

    assertEquals(4, fetcher.fetch().size());
    committer.commitThrough(1);
    perCommit.add(drainCheckpoints());
    committer.commitThrough(3);
    perCommit.add(drainCheckpoints());
    committer.commitThrough(4);
    perCommit.add(drainCheckpoints());

    assertEquals(
        List.of(List.of("shard-1@1"), List.of("shard-1@4"), List.of("shard-2@10")), perCommit);
  }

  @Test
  void aShutdownRequestDoesNotCheckpointQueuedRecords() throws Exception {
    KinesisSourceFetcher fetcher = new KinesisSourceFetcher();
    RecordProcessorCheckpointer shard1 = checkpointer("shard-1");
    ShardRecordProcessor processor = deliver(fetcher, "shard-1", shard1, record("1"));

    processor.shutdownRequested(ShutdownRequestedInput.builder().checkpointer(shard1).build());

    verifyNoInteractions(shard1);
  }

  private static ShardRecordProcessor deliver(
      KinesisSourceFetcher fetcher,
      String shardId,
      RecordProcessorCheckpointer checkpointer,
      KinesisClientRecord... records) {
    ShardRecordProcessor processor = fetcher.newShardRecordProcessor();
    processor.initialize(InitializationInput.builder().shardId(shardId).build());
    processor.processRecords(
        ProcessRecordsInput.builder().records(List.of(records)).checkpointer(checkpointer).build());
    return processor;
  }

  private RecordProcessorCheckpointer checkpointer(String shardId) throws Exception {
    RecordProcessorCheckpointer checkpointer = mock();
    doAnswer(call -> checkpoints.add(shardId + "@" + call.<String>getArgument(0)))
        .when(checkpointer)
        .checkpoint(anyString(), anyLong());
    return checkpointer;
  }

  private List<String> drainCheckpoints() {
    List<String> drained = List.copyOf(checkpoints);
    checkpoints.clear();
    return drained;
  }

  private static KinesisClientRecord record(String sequenceNumber) {
    return KinesisClientRecord.builder()
        .sequenceNumber(sequenceNumber)
        .subSequenceNumber(0)
        .partitionKey("pk")
        .data(ByteBuffer.wrap("{}".getBytes(UTF_8)))
        .build();
  }

  private static KinesisClientRecord noData(String sequenceNumber) {
    return KinesisClientRecord.builder()
        .sequenceNumber(sequenceNumber)
        .subSequenceNumber(0)
        .partitionKey("pk")
        .build();
  }

  @Test
  void theLoopCheckpointsTheLastHandledRecordByTheEndOfTheFetch() throws Exception {
    KinesisSourceFetcher fetcher = new KinesisSourceFetcher();
    deliver(fetcher, "shard-1", checkpointer("shard-1"), record("1"), record("2"), record("3"));

    SourceLoopHarness.run(
        fetcher,
        SourceLoopHarness.oneEventPerRecord(),
        SourceLoopHarness.failing(record -> false),
        null);

    assertEquals("shard-1@3", checkpoints.getLast());
  }

  @Test
  void checkpointsAtMostOncePerSecondAndAtTheEndOfEachFetch() {
    assertEquals(BatchCommitStrategy.ofInterval(1000), new KinesisSourceFetcher().commitStrategy());
  }

  @Test
  void recordsLeftAfterTheLastCommitAreCheckpointedWithTheNextFetch() throws Exception {
    KinesisSourceFetcher fetcher = new KinesisSourceFetcher();
    RecordProcessorCheckpointer shard1 = checkpointer("shard-1");
    deliver(fetcher, "shard-1", shard1, record("1"), noData("2"));
    Fetcher.Committer committer = fetcher.committer();
    List<List<String>> perCommit = new ArrayList<>();

    assertEquals(1, fetcher.fetch().size());
    committer.commitThrough(1);
    perCommit.add(drainCheckpoints());
    deliver(fetcher, "shard-2", checkpointer("shard-2"), record("10"));
    assertEquals(1, fetcher.fetch().size());
    committer.commitThrough(1);
    perCommit.add(drainCheckpoints());

    assertEquals(List.of(List.of("shard-1@1"), List.of("shard-1@2", "shard-2@10")), perCommit);
  }

  @Test
  void pauseShutsDownGracefullyAndEachShardCheckpointsItsLastHandledRecord() throws Exception {
    Scheduler scheduler = mock();
    List<ShardRecordProcessor> processors = new ArrayList<>();
    RecordProcessorCheckpointer onShutdown = checkpointer("shard-1");
    when(scheduler.startGracefulShutdown())
        .thenAnswer(
            call -> {
              checkpoints.add("graceful-shutdown");
              for (ShardRecordProcessor processor : processors) {
                processor.shutdownRequested(
                    ShutdownRequestedInput.builder().checkpointer(onShutdown).build());
              }
              return CompletableFuture.completedFuture(true);
            });
    CountDownLatch stopped = new CountDownLatch(1);
    doAnswer(
            call -> {
              stopped.await();
              return null;
            })
        .when(scheduler)
        .run();
    KinesisSourceFetcher fetcher = new KinesisSourceFetcher(() -> scheduler);
    processors.add(deliver(fetcher, "shard-1", checkpointer("shard-1"), record("1"), record("2")));
    assertEquals(2, fetcher.fetch().size());
    fetcher.committer().commitThrough(2);
    deliver(fetcher, "shard-1", checkpointer("shard-1"), record("3"));

    fetcher.pause();
    fetcher.resume();

    assertEquals(List.of("shard-1@2", "graceful-shutdown", "shard-1@2"), checkpoints);
    assertEquals(List.of(), fetcher.fetch());
    verify(scheduler, timeout(5_000)).run();
    stopped.countDown();
    fetcher.close();
  }

  @Test
  void aTimedOutGracefulShutdownForcesTheOldSchedulerToStopBeforeResumeStartsANewOne()
      throws Exception {
    List<String> events = new CopyOnWriteArrayList<>();
    CountDownLatch oldStopped = new CountDownLatch(1);
    Scheduler old = mock();
    when(old.startGracefulShutdown()).thenReturn(CompletableFuture.completedFuture(false));
    doAnswer(
            call -> {
              events.add("old shutdown");
              oldStopped.countDown();
              return null;
            })
        .when(old)
        .shutdown();
    doAnswer(
            call -> {
              oldStopped.await();
              events.add("old run exited");
              return null;
            })
        .when(old)
        .run();
    CountDownLatch newStopped = new CountDownLatch(1);
    Scheduler next = runsUntil(newStopped, events, "new run started");
    KinesisSourceFetcher fetcher = new KinesisSourceFetcher(schedulers(old, next));
    fetcher.start();
    verify(old, timeout(5_000)).run();

    fetcher.pause();
    fetcher.resume();

    verify(next, timeout(5_000)).run();
    assertEquals(List.of("old shutdown", "old run exited", "new run started"), events);
    assertEquals(List.of(), fetcher.fetch());
    newStopped.countDown();
    fetcher.close();
  }

  @Test
  void anOldRunExitingAfterResumeDoesNotMarkTheNewSchedulerAsStopped() throws Exception {
    List<String> events = new CopyOnWriteArrayList<>();
    CountDownLatch oldStopped = new CountDownLatch(1);
    Scheduler old = runsUntil(oldStopped, events, "old run started");
    when(old.startGracefulShutdown()).thenReturn(CompletableFuture.completedFuture(true));
    CountDownLatch newStopped = new CountDownLatch(1);
    Scheduler next = runsUntil(newStopped, events, "new run started");
    KinesisSourceFetcher fetcher =
        new KinesisSourceFetcher(schedulers(old, next), Duration.ofMillis(200));
    fetcher.start();
    verify(old, timeout(5_000)).run();

    fetcher.pause();
    fetcher.resume();
    oldStopped.countDown();

    verify(next, timeout(5_000)).run();
    assertEquals(List.of(), fetcher.fetch());
    newStopped.countDown();
    fetcher.close();
  }

  private static Scheduler runsUntil(CountDownLatch stopped, List<String> events, String started) {
    Scheduler scheduler = mock();
    doAnswer(
            call -> {
              events.add(started);
              stopped.await(30, TimeUnit.SECONDS);
              return null;
            })
        .when(scheduler)
        .run();
    return scheduler;
  }

  private static Supplier<Scheduler> schedulers(Scheduler... schedulers) {
    Iterator<Scheduler> next = List.of(schedulers).iterator();
    return next::next;
  }

  @Test
  void pauseReleasesAShardProcessorBlockedOnAFullQueue() throws Exception {
    KinesisSourceFetcher fetcher = new KinesisSourceFetcher();
    KinesisClientRecord[] records =
        IntStream.range(0, 600)
            .mapToObj(i -> record(Integer.toString(i)))
            .toArray(KinesisClientRecord[]::new);
    RecordProcessorCheckpointer shard1 = checkpointer("shard-1");
    Thread processing = new Thread(() -> deliver(fetcher, "shard-1", shard1, records));
    processing.start();
    Thread.sleep(300);
    assertTrue(processing.isAlive());

    fetcher.pause();

    processing.join(5_000);
    assertFalse(processing.isAlive());
  }
}
