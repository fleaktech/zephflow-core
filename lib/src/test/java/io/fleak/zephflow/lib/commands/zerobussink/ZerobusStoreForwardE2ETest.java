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
package io.fleak.zephflow.lib.commands.zerobussink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.databricks.zerobus.ZerobusJsonStream;
import com.databricks.zerobus.ZerobusSdk;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ExecutionContext;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.ScalarSinkCommand.SinkResult;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.ChronicleStoreForward;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentMatchers;

/**
 * End-to-end test of the store-and-forward path for the Zerobus sink: a real {@link
 * SimpleSinkCommand} wired with a real {@link ChronicleStoreForward}, the real {@link
 * ZerobusConnectionFailureClassifier}, the real {@link ZerobusSinkFlusher} (JSON mode) and {@link
 * ZerobusMessageProcessor}. Only the gRPC stream is mocked — it fails while "down" and records
 * payloads while "up", so we can drive a full outage → recovery → replay and assert delivery order.
 */
class ZerobusStoreForwardE2ETest {

  private static RecordFleakData record(int id) {
    return (RecordFleakData) FleakData.wrap(Map.of("id", id));
  }

  @Test
  void bufferDuringOutageThenReplayInOrderOnRecovery(@TempDir Path dir) throws Exception {
    AtomicBoolean streamUp = new AtomicBoolean(false);
    List<String> delivered = new CopyOnWriteArrayList<>();

    ZerobusJsonStream stream = mock(ZerobusJsonStream.class);
    when(stream.ingestRecordsOffset(ArgumentMatchers.<String>anyIterable()))
        .thenAnswer(
            inv -> {
              if (!streamUp.get()) {
                // simulate a connectivity failure: nothing reaches the server
                throw new RuntimeException("stream down", new IOException("connection refused"));
              }
              Iterable<String> payloads = inv.getArgument(0);
              payloads.forEach(delivered::add);
              return Optional.of((long) delivered.size());
            });

    // JSON-mode flusher with a mocked stream; config==null so it never tries a real reconnect.
    ZerobusSinkFlusher flusher =
        new ZerobusSinkFlusher("c.s.t", mock(ZerobusSdk.class), null, null, null, stream);
    ZerobusMessageProcessor preprocessor = new ZerobusMessageProcessor();

    ChronicleStoreForward storeForward =
        new ChronicleStoreForward(
            new ChronicleStoreForward.Config(dir, 1_000_000, 50, 2, "node-e2e"),
            new ZerobusConnectionFailureClassifier(),
            null,
            null,
            null);
    // ReplayTarget mirrors ZerobusSinkCommand: preprocess raw records, then flush.
    storeForward.start(
        records -> {
          SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> prepared =
              new SimpleSinkCommand.PreparedInputEvents<>();
          for (RecordFleakData r : records) {
            prepared.add(r, preprocessor.preprocess(r, 0L));
          }
          flusher.flush(prepared, Map.of());
        });

    FleakCounter noop = new NoopCounter();
    SinkExecutionContext<Map<String, Object>> ctx =
        new SinkExecutionContext<>(
            flusher, preprocessor, storeForward, noop, noop, noop, noop, noop);
    TestZerobusSink sink = new TestZerobusSink();

    // 1) Outage: the first write fails to reach the stream and is buffered, not delivered.
    SinkResult r1 = sink.writeToSink(List.of(record(1), record(2)), "user", ctx);
    assertEquals(2, r1.getSuccessCount()); // safely persisted
    assertTrue(storeForward.isBuffering());
    assertTrue(delivered.isEmpty());

    // 2) Still down: subsequent writes go straight to disk via the outage fast-path.
    sink.writeToSink(List.of(record(3)), "user", ctx);
    assertTrue(storeForward.isBuffering());
    assertTrue(delivered.isEmpty());

    // 3) Recovery: the forwarder drains the backlog oldest-first, then returns to DIRECT.
    streamUp.set(true);
    await(() -> !storeForward.isBuffering());
    assertEquals(List.of("{\"id\":1}", "{\"id\":2}", "{\"id\":3}"), delivered);

    // Draining reclaims disk: the queue files are gone, only the lock + drained marker remain.
    await(() -> reclaimed(dir));
    assertTrue(Files.exists(dir.resolve("sf-drained")));

    // 4) Back to DIRECT: a new write is delivered straight through.
    SinkResult r4 = sink.writeToSink(List.of(record(4)), "user", ctx);
    assertEquals(1, r4.getSuccessCount());
    assertEquals(List.of("{\"id\":1}", "{\"id\":2}", "{\"id\":3}", "{\"id\":4}"), delivered);

    storeForward.close();
  }

  /** True once the buffer dir holds nothing but the ownership lock and drained marker. */
  private static boolean reclaimed(Path dir) {
    try (Stream<Path> files = Files.list(dir)) {
      return files
          .map(p -> p.getFileName().toString())
          .noneMatch(n -> !n.equals("sf.lock") && !n.equals("sf-drained"));
    } catch (IOException e) {
      return false;
    }
  }

  private static void await(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + java.time.Duration.ofSeconds(10).toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(20);
    }
    fail("condition not met within timeout");
  }

  private static final class TestZerobusSink extends SimpleSinkCommand<Map<String, Object>> {
    TestZerobusSink() {
      super("node-e2e", null, null, null);
    }

    @Override
    protected int batchSize() {
      return 2;
    }

    @Override
    public String commandName() {
      return "test_zerobus";
    }

    @Override
    protected ExecutionContext createExecutionContext(
        MetricClientProvider metricClientProvider,
        JobContext jobContext,
        CommandConfig commandConfig,
        String nodeId) {
      return null;
    }
  }

  private static final class NoopCounter implements FleakCounter {
    @Override
    public void increase(Map<String, String> additionalTags) {}

    @Override
    public void increase(long n, Map<String, String> additionalTags) {}
  }
}
