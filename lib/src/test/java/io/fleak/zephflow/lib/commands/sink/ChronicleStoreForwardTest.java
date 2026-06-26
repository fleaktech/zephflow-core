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

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ChronicleStoreForwardTest {

  // IOException == connectivity; anything else == permanent.
  private static final ConnectionFailureClassifier IO_IS_CONNECTION =
      t -> {
        for (Throwable c = t; c != null && c != c.getCause(); c = c.getCause()) {
          if (c instanceof IOException) {
            return true;
          }
        }
        return false;
      };

  private static RecordFleakData record(String value) {
    return (RecordFleakData) FleakData.wrap(Map.of("v", value));
  }

  private static List<String> values(List<RecordFleakData> records) {
    return records.stream().map(r -> (String) r.unwrap().get("v")).toList();
  }

  private ChronicleStoreForward.Config config(Path dir, long maxBytes, int chunk) {
    return new ChronicleStoreForward.Config(dir, maxBytes, 50, chunk, "node-1");
  }

  @Test
  void offerBuffersThenWorkerDrainsInOrderAndReturnsToDirect(@TempDir Path dir) throws Exception {
    CountingCounter buffered = new CountingCounter();
    CountingCounter replayed = new CountingCounter();
    List<RecordFleakData> delivered = new CopyOnWriteArrayList<>();

    ChronicleStoreForward sf =
        new ChronicleStoreForward(
            config(dir, 1_000_000, 2), IO_IS_CONNECTION, buffered, replayed, null);
    sf.start(delivered::addAll);

    List<RecordFleakData> input = List.of(record("a"), record("b"), record("c"));
    assertEquals(3, sf.offer(input));
    assertTrue(sf.isBuffering());

    await(() -> !sf.isBuffering());

    assertEquals(List.of("a", "b", "c"), values(delivered));
    assertEquals(3, buffered.total());
    assertEquals(3, replayed.total());
    sf.close();
  }

  @Test
  void replayRetriesOnConnectionFailureThenSucceeds(@TempDir Path dir) throws Exception {
    List<RecordFleakData> delivered = new CopyOnWriteArrayList<>();
    AtomicInteger attempts = new AtomicInteger();
    SinkStoreForward.ReplayTarget target =
        records -> {
          // fail the first two delivery attempts with a connectivity error, then succeed
          if (attempts.getAndIncrement() < 2) {
            throw new IOException("connection down");
          }
          delivered.addAll(records);
        };

    ChronicleStoreForward sf =
        new ChronicleStoreForward(config(dir, 1_000_000, 10), IO_IS_CONNECTION, null, null, null);
    sf.start(target);
    sf.offer(List.of(record("x"), record("y")));

    await(() -> !sf.isBuffering());
    assertEquals(List.of("x", "y"), values(delivered));
    assertTrue(attempts.get() >= 3);
    sf.close();
  }

  @Test
  void permanentFailureDropsRecords(@TempDir Path dir) throws Exception {
    CountingCounter dropped = new CountingCounter();
    SinkStoreForward.ReplayTarget target =
        records -> {
          throw new IllegalStateException("bad table"); // not an IOException -> permanent
        };

    ChronicleStoreForward sf =
        new ChronicleStoreForward(
            config(dir, 1_000_000, 10), IO_IS_CONNECTION, null, null, dropped);
    sf.start(target);
    sf.offer(List.of(record("x"), record("y")));

    await(() -> !sf.isBuffering());
    assertEquals(2, dropped.total());
    sf.close();
  }

  @Test
  void capDropsExcessAndReportsStoredCount(@TempDir Path dir) throws Exception {
    CountingCounter dropped = new CountingCounter();
    // Block delivery so nothing drains while we assert what was stored vs dropped.
    ChronicleStoreForward sf =
        new ChronicleStoreForward(config(dir, 30, 10), IO_IS_CONNECTION, null, null, dropped);
    sf.start(
        records -> {
          throw new IOException("hold the line"); // keep it buffering
        });

    // each {"v":"a"} payload is ~9 bytes + 4-byte length prefix; cap 30 fits 2, drops the 3rd.
    int stored = sf.offer(List.of(record("a"), record("b"), record("c")));
    assertEquals(2, stored);
    assertEquals(1, dropped.total());
    sf.close();
  }

  @Test
  void restartResumesBacklogFromDisk(@TempDir Path dir) throws Exception {
    // First run: deliveries always fail (connectivity), so the 3 records stay on disk.
    ChronicleStoreForward first =
        new ChronicleStoreForward(config(dir, 1_000_000, 10), IO_IS_CONNECTION, null, null, null);
    first.start(
        records -> {
          throw new IOException("down for the whole first run");
        });
    first.offer(List.of(record("a"), record("b"), record("c")));
    await(first::isBuffering);
    first.close();

    // Second run on the SAME directory: connection restored, backlog must drain in order.
    List<RecordFleakData> delivered = new CopyOnWriteArrayList<>();
    ChronicleStoreForward second =
        new ChronicleStoreForward(config(dir, 1_000_000, 10), IO_IS_CONNECTION, null, null, null);
    second.start(delivered::addAll);

    await(() -> !second.isBuffering());
    assertEquals(List.of("a", "b", "c"), values(delivered));
    second.close();
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
