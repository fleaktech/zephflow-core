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

import com.databricks.zerobus.ZerobusException;
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
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentMatchers;

/**
 * Heavy soak variant of {@link ZerobusSinkBlackboxTest}: streams a large volume of fat-payload
 * records through a long outage so the backlog spans many drain chunks and a real amount of data
 * lands on disk. Verifies under load that no record is lost, duplicated, or reordered.
 *
 * <p>Opt-in only — it is slow and disk-heavy, so it runs only when {@code RUN_SOAK=true} is set in
 * the environment (env vars propagate to the Gradle test worker). Tune size with {@code
 * SOAK_RECORDS}. Example:
 *
 * <pre>RUN_SOAK=true SOAK_RECORDS=200000 ./gradlew :lib:test \
 *   --tests io.fleak.zephflow.lib.commands.zerobussink.ZerobusSinkStoreForwardSoakTest</pre>
 */
@Tag("soak")
@EnabledIfEnvironmentVariable(named = "RUN_SOAK", matches = "true")
class ZerobusSinkStoreForwardSoakTest {

  private static final int TOTAL = envInt("SOAK_RECORDS", 100_000);
  private static final int HEALTHY = TOTAL / 20; // ~5%
  private static final int POST = TOTAL / 20; // ~5%
  private static final int OUTAGE = TOTAL - HEALTHY - POST; // ~90%
  private static final int STREAM_BATCH = 500;
  private static final int DRAIN_CHUNK = 1_000;
  // ~480-byte filler so each JSON record is ~0.5 KB; the whole outage backlog is tens of MB on
  // disk.
  private static final String BLOB = "x".repeat(480);

  @Test
  void soakStreamThroughLongOutageLosesNothing(@TempDir Path dir) throws Exception {
    SoakEndpoint endpoint = new SoakEndpoint();

    ZerobusJsonStream stream = mock(ZerobusJsonStream.class);
    when(stream.ingestRecordsOffset(ArgumentMatchers.<String>anyIterable()))
        .thenAnswer(inv -> endpoint.ingest(inv.getArgument(0)));

    ZerobusSinkFlusher flusher =
        new ZerobusSinkFlusher("c.s.t", mock(ZerobusSdk.class), null, null, null, stream);
    ZerobusMessageProcessor preprocessor = new ZerobusMessageProcessor();

    AtomicLong buffered = new AtomicLong();
    AtomicLong replayed = new AtomicLong();
    ChronicleStoreForward storeForward =
        new ChronicleStoreForward(
            new ChronicleStoreForward.Config(dir, 4_000_000_000L, 50, DRAIN_CHUNK, "soak-node"),
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

    // healthy
    stream(sink, ctx, 1, HEALTHY);
    assertEquals(HEALTHY, endpoint.total(), "all healthy records delivered directly");

    // outage: stream a large backlog to disk
    endpoint.disconnect();
    long deliveredAtOutageStart = endpoint.total();
    stream(sink, ctx, HEALTHY + 1, HEALTHY + OUTAGE);
    assertTrue(storeForward.isBuffering(), "buffering during outage");
    assertEquals(OUTAGE, buffered.get(), "every outage record written locally");
    assertEquals(deliveredAtOutageStart, endpoint.total(), "nothing delivered during the outage");

    // recovery: drain the whole backlog (many chunks)
    endpoint.reconnect();
    await(() -> !storeForward.isBuffering(), 300);
    assertEquals(OUTAGE, replayed.get(), "every buffered record replayed");
    assertEquals(HEALTHY + OUTAGE, endpoint.total());

    // draining the tens-of-MB backlog reclaims the disk: only empty lock/marker files remain.
    await(() -> dirBytes(dir) == 0L, 30);

    // back to direct
    stream(sink, ctx, HEALTHY + OUTAGE + 1, TOTAL);

    // global invariants across the whole run
    assertEquals(TOTAL, endpoint.total(), "exactly TOTAL records received");
    assertEquals(TOTAL, endpoint.distinctReceived(), "no duplicates, no gaps");
    assertEquals(0, endpoint.outOfOrder(), "delivered strictly in order");

    storeForward.close();
  }

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

  /**
   * Memory-light endpoint: tracks order/dupes/gaps via a BitSet instead of keeping every payload.
   */
  private static final class SoakEndpoint {
    private final AtomicBoolean connected = new AtomicBoolean(true);
    private final BitSet seen = new BitSet(TOTAL + 1);
    private long total = 0;
    private int expectedNext = 1;
    private int outOfOrder = 0;

    synchronized Optional<Long> ingest(Iterable<String> payloads) throws ZerobusException {
      if (!connected.get()) {
        // Faithful to the native client: an offline failure surfaces as a message-only
        // ZerobusException with no IOException/gRPC cause.
        throw new ZerobusException("server lack of ack timeout");
      }
      for (String payload : payloads) {
        int id = parseId(payload);
        if (id != expectedNext) {
          outOfOrder++;
        }
        expectedNext = id + 1;
        seen.set(id);
        total++;
      }
      return Optional.of(total);
    }

    void disconnect() {
      connected.set(false);
    }

    void reconnect() {
      connected.set(true);
    }

    synchronized long total() {
      return total;
    }

    synchronized int distinctReceived() {
      return seen.cardinality();
    }

    synchronized int outOfOrder() {
      return outOfOrder;
    }
  }

  private static int parseId(String payload) {
    try {
      Map<String, Object> map =
          JsonUtils.OBJECT_MAPPER.readValue(
              payload, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
      return ((Number) map.get("id")).intValue();
    } catch (Exception e) {
      throw new RuntimeException("bad payload: " + payload, e);
    }
  }

  private static RecordFleakData record(int id) {
    return (RecordFleakData) FleakData.wrap(Map.of("id", id, "blob", BLOB));
  }

  private static int envInt(String name, int dflt) {
    String v = System.getenv(name);
    return v == null ? dflt : Integer.parseInt(v.trim());
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

  /** Total bytes of regular files under {@code dir}; ~0 once the buffer is reclaimed. */
  private static long dirBytes(Path dir) {
    try (Stream<Path> paths = Files.walk(dir)) {
      return paths
          .filter(Files::isRegularFile)
          .mapToLong(ZerobusSinkStoreForwardSoakTest::size)
          .sum();
    } catch (IOException e) {
      return -1;
    }
  }

  private static long size(Path p) {
    try {
      return Files.size(p);
    } catch (IOException e) {
      return 0;
    }
  }

  private static void await(BooleanSupplier condition, int timeoutSeconds)
      throws InterruptedException {
    long deadline = System.nanoTime() + java.time.Duration.ofSeconds(timeoutSeconds).toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      Thread.sleep(50);
    }
    fail("condition not met within timeout");
  }

  private static final class TestSink extends SimpleSinkCommand<Map<String, Object>> {
    TestSink() {
      super("soak-node", null, null, null);
    }

    @Override
    protected int batchSize() {
      return 1_000;
    }

    @Override
    public String commandName() {
      return "test_zerobus_soak";
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
