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
package io.fleak.zephflow.lib.commands.deltalakesink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.delta.kernel.Operation;
import io.delta.kernel.Table;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.execution.ExecutionControl;
import io.fleak.zephflow.api.execution.ExecutionHooks;
import io.fleak.zephflow.api.execution.ExecutionStoppedException;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.AbstractBufferedFlusher;
import io.fleak.zephflow.lib.commands.sink.BoundedFlushException;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.utils.BufferedWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** Real local Delta commits plus deterministic lifecycle fault injection. */
class DeltaLakeBoundedWriterTest {
  @TempDir Path directory;

  private static final Map<String, Object> AVRO =
      Map.of(
          "type",
          "record",
          "name",
          "Record",
          "fields",
          List.of(
              Map.of("name", "id", "type", "int"),
              Map.of("name", "name", "type", "string"),
              Map.of("name", "value", "type", "double")));

  @Test
  void committedReceiptSurvivesRejectedCheckpointScheduling() throws Exception {
    Fixture fixture = fixture(List.of(), () -> {}, null);
    executor(fixture.writer()).shutdown();
    try {
      BoundedFlushException failure =
          assertThrows(
              BoundedFlushException.class,
              () -> fixture.writer().flushBounded(events(1), Map.of(), fixture.hooks()));
      assertInstanceOf(RejectedExecutionException.class, failure.getCause());
      assertCommittedReceipt(fixture, failure.result());
    } finally {
      fixture.writer().abort();
    }
  }

  @Test
  void committedReceiptSurvivesStopBeforeCheckpointScheduling() throws Exception {
    ExecutionControl control =
        () -> {
          if (Files.exists(directory.resolve("table/_delta_log/00000000000000000001.json"))) {
            throw new ExecutionStoppedException("cancelled after commit");
          }
        };
    Fixture fixture = fixture(List.of(), control, null);
    try {
      BoundedFlushException failure =
          assertThrows(
              BoundedFlushException.class,
              () -> fixture.writer().flushBounded(events(1), Map.of(), fixture.hooks()));
      assertInstanceOf(ExecutionStoppedException.class, failure.getCause());
      assertCommittedReceipt(fixture, failure.result());
    } finally {
      fixture.writer().abort();
    }
  }

  @Test
  void acknowledgedCommitRemainsKnownWhenResourceCloseFails() throws Exception {
    DlqWriter dlq = mock(DlqWriter.class);
    doThrow(new IOException("close failed")).when(dlq).close();
    Fixture fixture = fixture(List.of(), () -> {}, dlq);
    SimpleSinkCommand.FlushResult receipt =
        fixture.writer().flushBounded(events(1), Map.of(), fixture.hooks());
    assertThrows(IOException.class, fixture.writer()::close);
    assertCommittedReceipt(fixture, receipt);
    assertTrue(executor(fixture.writer()).isTerminated());
  }

  @Test
  void stopIsCheckedBeforeEachPartitionWrite() throws Exception {
    AtomicInteger stops = new AtomicInteger();
    ExecutionControl control =
        () -> {
          try {
            if (parquetFiles().size() > 0) {
              stops.incrementAndGet();
              throw new ExecutionStoppedException("cancelled between partitions");
            }
          } catch (IOException failure) {
            throw new IllegalStateException(failure);
          }
        };
    Fixture fixture = fixture(List.of("name"), control, null);
    try {
      assertThrows(
          ExecutionStoppedException.class,
          () -> fixture.writer().flushBounded(events(2), Map.of(), fixture.hooks()));
      assertEquals(1, stops.get());
      assertEquals(0, version(fixture));
      assertEquals(1, parquetFiles().size(), "second partition must not begin writing");
    } finally {
      fixture.writer().abort();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void shutdownWaitsForActualHookExitAndAbortCancelsQueuedHook(boolean abort) throws Exception {
    Fixture fixture = fixture(List.of(), () -> {}, null);
    TrackingExecutor checkpointExecutor = new TrackingExecutor();
    executor(fixture.writer()).shutdown();
    setField(DeltaLakeWriter.class, fixture.writer(), "checkpointExecutor", checkpointExecutor);
    CountDownLatch hookEntered = new CountDownLatch(1);
    CountDownLatch releaseHook = new CountDownLatch(1);
    AtomicInteger invocations = new AtomicInteger();
    PostCommitHook hook = mock(PostCommitHook.class);
    when(hook.getType()).thenReturn(PostCommitHook.PostCommitHookType.CHECKPOINT);
    doAnswer(
            invocation -> {
              invocations.incrementAndGet();
              hookEntered.countDown();
              boolean interrupted = false;
              for (; ; ) {
                try {
                  releaseHook.await();
                  break;
                } catch (InterruptedException ignored) {
                  interrupted = true;
                }
              }
              if (interrupted) Thread.currentThread().interrupt();
              return null;
            })
        .when(hook)
        .threadSafeInvoke(any());
    TransactionCommitResult commit = mock(TransactionCommitResult.class);
    when(commit.getPostCommitHooks()).thenReturn(List.of(hook));
    var schedule =
        DeltaLakeWriter.class.getDeclaredMethod(
            "createCheckpointIfReady", TransactionCommitResult.class);
    schedule.setAccessible(true);
    ExecutorService closer = Executors.newSingleThreadExecutor();
    try {
      schedule.invoke(fixture.writer(), commit);
      assertTrue(hookEntered.await(5, TimeUnit.SECONDS));
      schedule.invoke(fixture.writer(), commit);
      Future<?> queuedHook = (Future<?>) checkpointExecutor.getQueue().peek();
      assertNotNull(queuedHook);
      Future<?> shutdown =
          closer.submit(
              () -> {
                if (abort) fixture.writer().abort();
                else fixture.writer().close();
                return null;
              });
      assertTrue(checkpointExecutor.shutdownRequested.await(5, TimeUnit.SECONDS));
      assertThrows(TimeoutException.class, () -> shutdown.get(100, TimeUnit.MILLISECONDS));
      assertFalse(checkpointExecutor.isTerminated());
      releaseHook.countDown();
      shutdown.get(5, TimeUnit.SECONDS);
      assertTrue(checkpointExecutor.isTerminated());
      assertEquals(abort ? 1 : 2, invocations.get());
      assertEquals(abort, queuedHook.isCancelled());
      assertEquals(0, version(fixture));
    } finally {
      releaseHook.countDown();
      fixture.writer().abort();
      closer.shutdownNow();
      assertTrue(closer.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void boundedShutdownDiscardsPendingBufferWithoutCommitting(boolean abort) throws Exception {
    Fixture fixture = fixture(List.of(), () -> {}, null);
    Field field = AbstractBufferedFlusher.class.getDeclaredField("bufferedWriter");
    field.setAccessible(true);
    @SuppressWarnings("unchecked")
    BufferedWriter<Pair<RecordFleakData, Map<String, Object>>> buffer =
        (BufferedWriter<Pair<RecordFleakData, Map<String, Object>>>) field.get(fixture.writer());
    buffer.write(events(2).rawAndPreparedList(), false);
    assertEquals(2, buffer.getBufferSize());
    if (abort) fixture.writer().abort();
    else fixture.writer().close();
    assertEquals(0, buffer.getBufferSize());
    assertEquals(0, version(fixture));
    assertTrue(parquetFiles().isEmpty());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void partitionFailureKeepsPrivateRowsOutOfBoundedLogsOnly(boolean bounded) throws Exception {
    Fixture fixture = fixture(List.of(), () -> {}, null, bounded);
    var invalid = new SimpleSinkCommand.PreparedInputEvents<Map<String, Object>>();
    var value =
        Map.<String, Object>of("id", "private-invalid-row-value", "name", "one", "value", 1.5);
    invalid.add((RecordFleakData) FleakData.wrap(value), value);
    try (var logs = new io.fleak.zephflow.lib.utils.BoundedLogCapture(DeltaLakeWriter.class)) {
      assertThrows(
          InvalidRecordException.class,
          () -> fixture.writer().doFlush(invalid.rawAndPreparedList()));
      assertFalse(logs.events().isEmpty());
      if (bounded) {
        assertTrue(logs.events().stream().allMatch(event -> event.getThrown() == null));
        assertTrue(
            logs.events().stream()
                .noneMatch(
                    event ->
                        event
                            .getMessage()
                            .getFormattedMessage()
                            .contains("private-invalid-row-value")));
      } else {
        assertTrue(
            logs.events().stream()
                .anyMatch(event -> event.getThrown() instanceof InvalidRecordException));
      }
    } finally {
      fixture.writer().abort();
    }
  }

  @Test
  void initializationFailureDoesNotLogPrivatePathOrCause() {
    var hooks = new ExecutionHooks(() -> {}, mock(ExecutionHooks.Effects.class), Runnable::run);
    var context = JobContext.builder().executionHooks(hooks).build();
    var writer =
        new DeltaLakeWriter(
            DeltaLakeSinkDto.Config.builder()
                .tablePath(directory.resolve("token=private-delta-token").toString())
                .avroSchema(AVRO)
                .build(),
            context,
            null,
            mock(FleakCounter.class),
            mock(FleakCounter.class),
            mock(FleakCounter.class),
            "sink");
    try (var logs = new io.fleak.zephflow.lib.utils.BoundedLogCapture(DeltaLakeWriter.class)) {
      assertThrows(IllegalStateException.class, writer::initialize);
      assertFalse(logs.events().isEmpty());
      assertTrue(logs.events().stream().allMatch(event -> event.getThrown() == null));
      assertTrue(
          logs.events().stream()
              .noneMatch(
                  event ->
                      event.getMessage().getFormattedMessage().contains("private-delta-token")));
    }
  }

  private Fixture fixture(List<String> partitions, ExecutionControl control, DlqWriter dlq) {
    return fixture(partitions, control, dlq, true);
  }

  private Fixture fixture(
      List<String> partitions, ExecutionControl control, DlqWriter dlq, boolean bounded) {
    String path = directory.resolve("table").toString();
    Engine engine = DefaultEngine.create(new Configuration());
    Table table = Table.forPath(engine, path);
    var transaction =
        table
            .createTransactionBuilder(engine, "bounded test", Operation.CREATE_TABLE)
            .withSchema(
                engine,
                new StructType()
                    .add("id", IntegerType.INTEGER, false)
                    .add("name", StringType.STRING, true)
                    .add("value", DoubleType.DOUBLE, true))
            .withTableProperties(engine, Map.of("delta.checkpointInterval", "1"));
    if (!partitions.isEmpty()) transaction = transaction.withPartitionColumns(engine, partitions);
    transaction.build(engine).commit(engine, empty());
    ExecutionHooks hooks =
        new ExecutionHooks(control, mock(ExecutionHooks.Effects.class), Runnable::run);
    JobContext context = mock(JobContext.class);
    when(context.getExecutionHooks()).thenReturn(bounded ? hooks : null);
    DeltaLakeWriter writer =
        new DeltaLakeWriter(
            DeltaLakeSinkDto.Config.builder()
                .tablePath(path)
                .batchSize(100)
                .partitionColumns(partitions)
                .avroSchema(AVRO)
                .build(),
            context,
            dlq,
            mock(FleakCounter.class),
            mock(FleakCounter.class),
            mock(FleakCounter.class),
            "sink");
    writer.initialize();
    return new Fixture(writer, hooks, engine, table);
  }

  private void assertCommittedReceipt(Fixture fixture, SimpleSinkCommand.FlushResult receipt)
      throws IOException {
    assertEquals(1, receipt.successCount());
    assertTrue(receipt.flushedDataSize() > 0);
    assertTrue(receipt.errorOutputList().isEmpty());
    assertEquals(1, version(fixture));
    assertFalse(parquetFiles().isEmpty());
  }

  private long version(Fixture fixture) {
    return fixture.table().getLatestSnapshot(fixture.engine()).getVersion();
  }

  private List<Path> parquetFiles() throws IOException {
    try (var paths = Files.walk(directory)) {
      return paths
          .filter(
              path ->
                  path.toString().endsWith(".parquet") && !path.toString().contains("_delta_log"))
          .toList();
    }
  }

  private static SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> events(int count) {
    var events = new SimpleSinkCommand.PreparedInputEvents<Map<String, Object>>();
    for (int i = 0; i < count; i++) {
      Map<String, Object> data = Map.of("id", i, "name", "partition" + i, "value", 1.5);
      events.add((RecordFleakData) FleakData.wrap(data), data);
    }
    return events;
  }

  private static ExecutorService executor(DeltaLakeWriter writer)
      throws ReflectiveOperationException {
    Field field = DeltaLakeWriter.class.getDeclaredField("checkpointExecutor");
    field.setAccessible(true);
    return (ExecutorService) field.get(writer);
  }

  private static void setField(Class<?> owner, Object target, String name, Object value)
      throws ReflectiveOperationException {
    Field field = owner.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }

  private static <T> CloseableIterable<T> empty() {
    return new CloseableIterable<>() {
      public CloseableIterator<T> iterator() {
        return new CloseableIterator<>() {
          public boolean hasNext() {
            return false;
          }

          public T next() {
            throw new NoSuchElementException();
          }

          public void close() {}
        };
      }

      public void close() {}
    };
  }

  private record Fixture(
      DeltaLakeWriter writer, ExecutionHooks hooks, Engine engine, Table table) {}

  private static final class TrackingExecutor extends ThreadPoolExecutor {
    private final CountDownLatch shutdownRequested = new CountDownLatch(1);

    private TrackingExecutor() {
      super(1, 1, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1));
    }

    @Override
    public void shutdown() {
      super.shutdown();
      shutdownRequested.countDown();
    }

    @Override
    public List<Runnable> shutdownNow() {
      List<Runnable> queued = super.shutdownNow();
      shutdownRequested.countDown();
      return queued;
    }
  }
}
