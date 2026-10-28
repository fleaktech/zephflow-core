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

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
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
    // Gate delivery so the worker can't drain (and flip buffering back to false) before we observe
    // the buffering state; releasing the latch then lets it drain to completion.
    CountDownLatch release = new CountDownLatch(1);
    sf.start(
        records -> {
          try {
            release.await();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
          delivered.addAll(records);
        });

    List<RecordFleakData> input = List.of(record("a"), record("b"), record("c"));
    assertEquals(3, sf.offer(input));
    assertTrue(sf.isBuffering());

    release.countDown();
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

  @Test
  void drainReclaimsQueueFilesLeavingOnlyLockAndMarker(@TempDir Path dir) throws Exception {
    AtomicBoolean connected = new AtomicBoolean(false);
    List<RecordFleakData> delivered = new CopyOnWriteArrayList<>();
    ChronicleStoreForward sf =
        new ChronicleStoreForward(config(dir, 1_000_000, 10), IO_IS_CONNECTION, null, null, null);
    sf.start(
        records -> {
          if (!connected.get()) {
            throw new IOException("down");
          }
          delivered.addAll(records);
        });

    sf.offer(List.of(record("a"), record("b")));
    await(sf::isBuffering);
    assertFalse(queueFiles(dir).isEmpty(), "queue files should exist during an outage");

    connected.set(true);
    awaitReclaimed(dir);

    assertEquals(List.of("a", "b"), values(delivered));
    assertEquals(List.of(), queueFiles(dir), "queue files and ack reclaimed after drain");
    assertTrue(Files.exists(dir.resolve("sf-drained")), "drained marker written");
    assertTrue(Files.exists(dir.resolve("sf.lock")), "ownership lock kept");
    sf.close();
  }

  @Test
  void secondOutageRebuildsQueueAndDrainsAgain(@TempDir Path dir) throws Exception {
    AtomicBoolean connected = new AtomicBoolean(false);
    List<RecordFleakData> delivered = new CopyOnWriteArrayList<>();
    ChronicleStoreForward sf =
        new ChronicleStoreForward(config(dir, 1_000_000, 10), IO_IS_CONNECTION, null, null, null);
    sf.start(
        records -> {
          if (!connected.get()) {
            throw new IOException("down");
          }
          delivered.addAll(records);
        });

    connected.set(false);
    sf.offer(List.of(record("a")));
    await(sf::isBuffering);
    connected.set(true);
    awaitReclaimed(dir);

    connected.set(false);
    sf.offer(List.of(record("b")));
    await(sf::isBuffering);
    connected.set(true);
    awaitReclaimed(dir);

    assertEquals(List.of("a", "b"), values(delivered));
    sf.close();
  }

  @Test
  void crashMidSecondOutageLosesNothing(@TempDir Path dir) throws Exception {
    // Instance A: fully drain one outage (queue torn down + reclaimed), then start a second outage
    // whose delivery fails, then close() while still buffering — a mid-outage "crash".
    AtomicBoolean connected = new AtomicBoolean(true);
    ChronicleStoreForward a =
        new ChronicleStoreForward(config(dir, 1_000_000, 10), IO_IS_CONNECTION, null, null, null);
    a.start(
        records -> {
          if (!connected.get()) {
            throw new IOException("down");
          }
        });

    connected.set(false);
    a.offer(List.of(record("old")));
    await(a::isBuffering);
    connected.set(true);
    awaitReclaimed(dir);

    connected.set(false);
    a.offer(List.of(record("x"), record("y")));
    await(a::isBuffering);
    a.close();

    // Instance B on the SAME dir, connection restored: the second outage's records must all replay
    // (no stale ack from the first outage's teardown skips them).
    List<RecordFleakData> delivered = new CopyOnWriteArrayList<>();
    ChronicleStoreForward b =
        new ChronicleStoreForward(config(dir, 1_000_000, 10), IO_IS_CONNECTION, null, null, null);
    b.start(delivered::addAll);
    await(() -> !b.isBuffering());
    assertEquals(List.of("x", "y"), values(delivered));
    b.close();
  }

  @Test
  void offerReportsRecordsNotStoredWhenBufferCannotBeReopened(@TempDir Path dir) throws Exception {
    ChronicleStoreForward sf =
        new ChronicleStoreForward(config(dir, 1_000_000, 10), IO_IS_CONNECTION, null, null, null);
    sf.start(
        records -> {
          throw new IOException("down");
        });
    // start() with no backlog tore the queue down and wrote the drained marker; queue == null.
    // Replace that marker with a NON-EMPTY DIRECTORY so the next offer's rebuild fails to clear it
    // (DirectoryNotEmptyException) — a deterministic stand-in for any un-clearable buffer dir.
    Path marker = dir.resolve("sf-drained");
    Files.delete(marker);
    Files.createDirectory(marker);
    Files.writeString(marker.resolve("blocker"), "x");

    // The failed rebuild must not escape offer(): the records are reported as not stored and the
    // sink stays DIRECT rather than crashing or silently losing them.
    int stored = sf.offer(List.of(record("a"), record("b")));
    assertEquals(0, stored);
    assertFalse(sf.isBuffering());
    sf.close();
  }

  @Test
  void secondInstanceOnSameDirFailsFastUntilFirstCloses(@TempDir Path dir) {
    ChronicleStoreForward first =
        new ChronicleStoreForward(config(dir, 1_000_000, 10), IO_IS_CONNECTION, null, null, null);

    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () ->
                new ChronicleStoreForward(
                    config(dir, 1_000_000, 10), IO_IS_CONNECTION, null, null, null));
    assertTrue(e.getMessage().contains("already locked by another process"));

    first.close();
    ChronicleStoreForward second =
        new ChronicleStoreForward(config(dir, 1_000_000, 10), IO_IS_CONNECTION, null, null, null);
    second.close();
  }

  /**
   * The real FLE-2131 scenario: two replicas of the same job (same base dir, same node id) buffer
   * and drain concurrently. With per-replica paths they must not interfere: each replica delivers
   * exactly its own records, in order.
   */
  @Test
  void coLocatedReplicasBufferAndDrainWithoutInterference(@TempDir Path base) throws Exception {
    Path dir0 = replicaStorePath(base, 0);
    Path dir1 = replicaStorePath(base, 1);
    assertNotEquals(dir0, dir1);

    List<RecordFleakData> delivered0 = new CopyOnWriteArrayList<>();
    List<RecordFleakData> delivered1 = new CopyOnWriteArrayList<>();
    ChronicleStoreForward sf0 =
        new ChronicleStoreForward(
            new ChronicleStoreForward.Config(dir0, 1_000_000, 50, 10, "node-1"),
            IO_IS_CONNECTION,
            null,
            null,
            null);
    ChronicleStoreForward sf1 =
        new ChronicleStoreForward(
            new ChronicleStoreForward.Config(dir1, 1_000_000, 50, 10, "node-1"),
            IO_IS_CONNECTION,
            null,
            null,
            null);
    sf0.start(delivered0::addAll);
    sf1.start(delivered1::addAll);

    int n = 200;
    Thread offer0 = offerThread(sf0, "a", n);
    Thread offer1 = offerThread(sf1, "b", n);
    offer0.start();
    offer1.start();
    offer0.join(TimeUnit.SECONDS.toMillis(30));
    offer1.join(TimeUnit.SECONDS.toMillis(30));

    await(() -> !sf0.isBuffering() && !sf1.isBuffering());

    assertEquals(prefixedValues("a", n), values(delivered0));
    assertEquals(prefixedValues("b", n), values(delivered1));
    sf0.close();
    sf1.close();
  }

  private static Path replicaStorePath(Path base, int replicaIndex) {
    JobContext jc =
        JobContext.builder()
            .otherProperties(Map.of(JobContext.REPLICA_INDEX, String.valueOf(replicaIndex)))
            .metricTags(Map.of("job_id", "job-1"))
            .build();
    return StoreForwardPaths.resolve(base.toString(), jc, "node-1");
  }

  private static Thread offerThread(ChronicleStoreForward sf, String prefix, int n) {
    return new Thread(
        () -> {
          for (int i = 0; i < n; i++) {
            sf.offer(List.of(record(prefix + i)));
          }
        },
        "offer-" + prefix);
  }

  private static List<String> prefixedValues(String prefix, int n) {
    List<String> out = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      out.add(prefix + i);
    }
    return out;
  }

  /**
   * The lock must hold across OS processes, not just within this JVM: a forked {@code java} process
   * must fail to lock {@code sf.lock} while we hold it, and succeed once we close.
   */
  @Test
  void dirLockIsEnforcedAcrossProcesses(@TempDir Path dir, @TempDir Path probeDir)
      throws Exception {
    ChronicleStoreForward sf =
        new ChronicleStoreForward(config(dir, 1_000_000, 10), IO_IS_CONNECTION, null, null, null);

    Path probe = probeDir.resolve("LockProbe.java");
    Files.writeString(
        probe,
        """
        import java.nio.channels.FileChannel;
        import java.nio.channels.FileLock;
        import java.nio.file.Path;
        import java.nio.file.StandardOpenOption;

        public class LockProbe {
          public static void main(String[] args) throws Exception {
            try (FileChannel ch = FileChannel.open(Path.of(args[0]), StandardOpenOption.WRITE)) {
              FileLock lock = ch.tryLock();
              System.exit(lock == null ? 17 : 0);
            }
          }
        }
        """);
    Path lockFile = dir.resolve("sf.lock");

    assertEquals(17, runLockProbe(probe, lockFile), "child process must not acquire a held lock");
    sf.close();
    assertEquals(0, runLockProbe(probe, lockFile), "lock must be free after close");
  }

  private static int runLockProbe(Path probe, Path lockFile) throws Exception {
    Process p =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                probe.toString(),
                lockFile.toString())
            .inheritIO()
            .start();
    assertTrue(p.waitFor(60, TimeUnit.SECONDS), "lock probe process timed out");
    return p.exitValue();
  }

  /** Files in the buffer dir other than the ownership lock and the drained marker. */
  private static List<String> queueFiles(Path dir) throws IOException {
    try (Stream<Path> files = Files.list(dir)) {
      return files
          .map(p -> p.getFileName().toString())
          .filter(n -> !n.equals("sf.lock") && !n.equals("sf-drained"))
          .sorted()
          .toList();
    }
  }

  /**
   * Waits until the buffer dir holds no queue files or ack, i.e. a drain teardown has reclaimed it.
   */
  private static void awaitReclaimed(Path dir) throws InterruptedException {
    await(
        () -> {
          try {
            return queueFiles(dir).isEmpty();
          } catch (IOException e) {
            return false;
          }
        });
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
