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
import static org.mockito.Mockito.*;

import com.databricks.zerobus.ZerobusJsonStream;
import com.databricks.zerobus.ZerobusSdk;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ExecutionContext;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.ChronicleStoreForward;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentMatchers;

/**
 * Blackbox-style test of the Zerobus sink's store-and-forward behavior. It runs the whole real
 * production path — {@link io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand}, the real {@link
 * ChronicleStoreForward}, the real {@link ZerobusConnectionFailureClassifier}, the real {@link
 * ZerobusSinkFlusher} (JSON mode) and {@link ZerobusMessageProcessor} — against a {@link
 * FakeZerobusEndpoint} that records what it receives and can be disconnected/reconnected.
 *
 * <p><b>Why the disconnection is injected at the stream seam:</b> the real Zerobus SDK transport is
 * a closed-source native library (TLS + Databricks OAuth over gRPC), so a faithful socket-level
 * fake server that the real client connects to is not feasible in a unit test. The native client
 * surfaces a connection loss as an ingest exception; this test reproduces exactly that at the only
 * seam the SDK exposes to Java. A true socket-level test belongs against a real/staging Zerobus
 * endpoint.
 */
class ZerobusSinkBlackboxTest {

  // A backlog larger than the drain chunk, so recovery drains over many chunks — exercising
  // multi-cycle replay and ordering rather than a single trivial drain.
  private static final int HEALTHY = 100;
  private static final int OUTAGE = 800;
  private static final int POST = 100;
  private static final int TOTAL = HEALTHY + OUTAGE + POST; // 1000
  private static final int STREAM_BATCH = 20;
  private static final int DRAIN_CHUNK = 64;

  @Test
  void streamThroughOutageBuffersLocallyThenDeliversEverythingOnRecovery(@TempDir Path dir)
      throws Exception {
    FakeZerobusEndpoint endpoint = new FakeZerobusEndpoint();

    // The mocked stream is the wire to the endpoint: forward on success, throw a connectivity
    // error when the endpoint is unreachable — exactly how the native client surfaces a drop.
    ZerobusJsonStream stream = mock(ZerobusJsonStream.class);
    when(stream.ingestRecordsOffset(ArgumentMatchers.<String>anyIterable()))
        .thenAnswer(inv -> endpoint.ingest(inv.getArgument(0)));

    ZerobusSinkFlusher flusher =
        new ZerobusSinkFlusher(mock(ZerobusSdk.class), null, null, null, stream);
    ZerobusMessageProcessor preprocessor = new ZerobusMessageProcessor();

    AtomicLong buffered = new AtomicLong();
    AtomicLong replayed = new AtomicLong();
    ChronicleStoreForward storeForward =
        new ChronicleStoreForward(
            new ChronicleStoreForward.Config(dir, 100_000_000, 50, DRAIN_CHUNK, "blackbox-node"),
            new ZerobusConnectionFailureClassifier(),
            counter(buffered),
            counter(replayed),
            null);
    storeForward.start(
        records -> {
          SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> prepared =
              new SimpleSinkCommand.PreparedInputEvents<>();
          for (RecordFleakData r : records) {
            prepared.add(r, preprocessor.preprocess(r, 0L));
          }
          flusher.flush(prepared, Map.of());
        });

    FleakCounter noop = counter(new AtomicLong());
    SinkExecutionContext<Map<String, Object>> ctx =
        new SinkExecutionContext<>(
            flusher, preprocessor, storeForward, noop, noop, noop, noop, noop);
    TestSink sink = new TestSink();

    // ---- the streaming script ----

    // healthy: stream records 1..HEALTHY, each delivered straight to the endpoint
    stream(sink, ctx, 1, HEALTHY);
    assertEquals(idRange(1, HEALTHY), endpoint.received());
    assertFalse(storeForward.isBuffering());

    // ---- network goes down ----
    endpoint.disconnect();
    long receivedAtOutageStart = endpoint.receivedCount();

    // keep streaming during the outage; these must be written locally, not delivered
    stream(sink, ctx, HEALTHY + 1, HEALTHY + OUTAGE);
    assertTrue(storeForward.isBuffering(), "sink should be buffering during the outage");
    assertEquals(OUTAGE, buffered.get(), "every outage record must be written locally");
    // verify nothing was delivered to the endpoint while the network was down
    assertEquals(receivedAtOutageStart, endpoint.receivedCount());
    assertEquals(idRange(1, HEALTHY), endpoint.received());

    // ---- network is restored: the backlog drains over many chunks ----
    endpoint.reconnect();
    await(() -> !storeForward.isBuffering());
    assertEquals(idRange(1, HEALTHY + OUTAGE), endpoint.received());
    assertEquals(OUTAGE, replayed.get(), "every buffered record was replayed");

    // ---- back to DIRECT: streaming resumes straight to the endpoint ----
    stream(sink, ctx, HEALTHY + OUTAGE + 1, TOTAL);

    // every record, across the whole run, in order, exactly once — no loss, no dupes, no reorder
    assertEquals(idRange(1, TOTAL), endpoint.received());
    assertEquals(TOTAL, endpoint.receivedCount());

    storeForward.close();
  }

  /** Streams ids [from, to] through the sink in batches, like an upstream producer. */
  private static void stream(
      TestSink sink, SinkExecutionContext<Map<String, Object>> ctx, int from, int to) {
    List<RecordFleakData> batch = new ArrayList<>(STREAM_BATCH);
    for (int id = from; id <= to; id++) {
      batch.add(record(id));
      if (batch.size() == STREAM_BATCH || id == to) {
        sink.writeToSink(List.copyOf(batch), "user", ctx);
        batch.clear();
      }
    }
  }

  // ===== fake endpoint =====

  /** A stand-in Zerobus endpoint: records received JSON payloads and can be disconnected. */
  private static final class FakeZerobusEndpoint {
    private final AtomicBoolean connected = new AtomicBoolean(true);
    private final List<String> received = new ArrayList<>();

    synchronized Optional<Long> ingest(Iterable<String> payloads) {
      if (!connected.get()) {
        throw new RuntimeException("endpoint unreachable", new IOException("connection refused"));
      }
      payloads.forEach(received::add);
      return Optional.of((long) received.size());
    }

    void disconnect() {
      connected.set(false);
    }

    void reconnect() {
      connected.set(true);
    }

    synchronized List<String> received() {
      return new ArrayList<>(received);
    }

    synchronized long receivedCount() {
      return received.size();
    }
  }

  private static RecordFleakData record(int id) {
    return (RecordFleakData) FleakData.wrap(Map.of("id", id));
  }

  private static List<String> idRange(int from, int to) {
    List<String> out = new ArrayList<>();
    for (int id = from; id <= to; id++) {
      out.add("{\"id\":" + id + "}");
    }
    return out;
  }

  private static FleakCounter counter(AtomicLong sink) {
    return new FleakCounter() {
      @Override
      public void increase(Map<String, String> additionalTags) {
        sink.incrementAndGet();
      }

      @Override
      public void increase(long n, Map<String, String> additionalTags) {
        sink.addAndGet(n);
      }
    };
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

  private static final class TestSink extends SimpleSinkCommand<Map<String, Object>> {
    TestSink() {
      super("blackbox-node", null, null, null);
    }

    @Override
    protected int batchSize() {
      return 100;
    }

    @Override
    public String commandName() {
      return "test_zerobus_blackbox";
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
}
