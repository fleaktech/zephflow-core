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
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class StoreForwardCleanerTest {

  private static final ConnectionFailureClassifier IO_IS_CONNECTION =
      t -> {
        for (Throwable c = t; c != null && c != c.getCause(); c = c.getCause()) {
          if (c instanceof IOException) {
            return true;
          }
        }
        return false;
      };

  private static final Duration MIN_AGE = Duration.ofMinutes(1);
  private static final Duration OLD = Duration.ofMinutes(10);

  @Test
  void reclaimsDrainedStaleUnownedDir(@TempDir Path base) throws Exception {
    Path dir = drainedDir(base);
    age(dir, OLD);

    StoreForwardCleaner.sweep(base, null, MIN_AGE);

    assertFalse(Files.exists(dir));
  }

  @Test
  void reclaimsNeverBufferedDir(@TempDir Path base) throws Exception {
    Path dir = makeNeverBuffered(base.resolve("empty"));
    age(dir, OLD);

    StoreForwardCleaner.sweep(base, null, MIN_AGE);

    assertFalse(Files.exists(dir));
  }

  @Test
  void keepsDirWithUndeliveredRecords(@TempDir Path base) throws Exception {
    Path dir = undeliveredDir(base);
    age(dir, OLD);

    StoreForwardCleaner.sweep(base, null, MIN_AGE);

    assertTrue(Files.exists(dir));
  }

  @Test
  void keepsDirStillOwnedByLiveProcess(@TempDir Path base) throws Exception {
    Path dir = base.resolve("live");
    ChronicleStoreForward sf = open(dir); // holds the lock, not closed
    age(dir, OLD);

    StoreForwardCleaner.sweep(base, null, MIN_AGE);

    assertTrue(Files.exists(dir));
    sf.close();
  }

  @Test
  void keepsOwnDir(@TempDir Path base) throws Exception {
    Path dir = drainedDir(base);
    age(dir, OLD);

    StoreForwardCleaner.sweep(base, dir, MIN_AGE);

    assertTrue(Files.exists(dir));
  }

  @Test
  void keepsFreshDirWithinGracePeriod(@TempDir Path base) throws Exception {
    Path dir = drainedDir(base);
    // not aged: newest mtime is now, inside the grace window

    StoreForwardCleaner.sweep(base, null, MIN_AGE);

    assertTrue(Files.exists(dir));
  }

  @Test
  void prunesEmptiedNodeAndJobDirsButKeepsBase(@TempDir Path base) throws Exception {
    Path leaf = base.resolve("job-1").resolve("node-1").resolve("replica-0");
    ChronicleStoreForward sf = open(leaf);
    sf.start(records -> {});
    sf.close();
    age(leaf, OLD);

    StoreForwardCleaner.sweep(base, null, MIN_AGE);

    assertFalse(Files.exists(base.resolve("job-1")));
    assertTrue(Files.exists(base));
  }

  @Test
  void reclaimsOnlyEligibleDirsInASingleSweep(@TempDir Path base) throws Exception {
    Path drained = makeDrained(base.resolve("drained"));
    Path undelivered = makeUndelivered(base.resolve("undelivered"));
    Path live = base.resolve("live");
    ChronicleStoreForward alive = open(live); // still owns its lock
    age(drained, OLD);
    age(undelivered, OLD);
    age(live, OLD);

    StoreForwardCleaner.sweep(base, null, MIN_AGE);

    assertFalse(Files.exists(drained), "drained dir reclaimed");
    assertTrue(Files.exists(undelivered), "undelivered dir kept");
    assertTrue(Files.exists(live), "live dir kept");
    alive.close();
  }

  @Test
  void keepsJobDirWhenASiblingReplicaStillHasData(@TempDir Path base) throws Exception {
    Path node = base.resolve("job-1").resolve("node-1");
    Path replica0 = makeDrained(node.resolve("replica-0"));
    Path replica1 = makeUndelivered(node.resolve("replica-1"));
    age(replica0, OLD);
    age(replica1, OLD);

    StoreForwardCleaner.sweep(base, null, MIN_AGE);

    assertFalse(Files.exists(replica0), "drained replica reclaimed");
    assertTrue(Files.exists(replica1), "replica with data kept");
    assertTrue(Files.exists(node), "node dir kept because a replica remains");
    assertTrue(Files.exists(base.resolve("job-1")), "job dir kept because a replica remains");
  }

  @Test
  void keepsPartiallyDrainedDir(@TempDir Path base) throws Exception {
    Path dir = makePartiallyDrained(base.resolve("partial"));
    age(dir, OLD);

    StoreForwardCleaner.sweep(base, null, MIN_AGE);

    assertTrue(Files.exists(dir));
  }

  @Test
  void keepsDirWithUnparseableAck(@TempDir Path base) throws Exception {
    Path dir = makeNeverBuffered(base.resolve("bad-ack"));
    writeRawAck(dir, "not-an-index");
    age(dir, OLD);

    StoreForwardCleaner.sweep(base, null, MIN_AGE);

    assertTrue(Files.exists(dir));
  }

  /**
   * The production entry point: resolves the base dir from {@code localStorePath}/{@link
   * JobContext} and reclaims on a background thread using the real {@link
   * StoreForwardCleaner#DEFAULT_MIN_AGE}.
   */
  @Test
  void sweepOnceResolvesBaseFromJobContextAndReclaimsAsync(@TempDir Path base) throws Exception {
    Path dir = makeDrained(base.resolve("drained"));
    age(dir, StoreForwardCleaner.DEFAULT_MIN_AGE.plusMinutes(5)); // exceed the real grace period

    StoreForwardCleaner.sweepOnce(
        base.toString(), JobContext.builder().build(), base.resolve("me"));

    await(() -> !Files.exists(dir));
  }

  // ===== helpers =====

  private static ChronicleStoreForward open(Path dir) {
    return new ChronicleStoreForward(
        new ChronicleStoreForward.Config(dir, 1_000_000, 50, 10, "node-1"),
        IO_IS_CONNECTION,
        null,
        null,
        null);
  }

  /** A directory whose records were all delivered (ack watermark == END), then closed. */
  private static Path drainedDir(Path base) throws Exception {
    return makeDrained(base.resolve("drained"));
  }

  private static Path makeDrained(Path dir) throws Exception {
    ChronicleStoreForward sf = open(dir);
    List<RecordFleakData> delivered = new CopyOnWriteArrayList<>();
    sf.start(delivered::addAll);
    sf.offer(List.of(record("a")));
    await(() -> !sf.isBuffering());
    sf.close();
    return dir;
  }

  private static Path makeNeverBuffered(Path dir) throws Exception {
    ChronicleStoreForward sf = open(dir);
    sf.start(records -> {});
    sf.close();
    return dir;
  }

  /** A directory whose delivery never succeeded, so records stay on disk undelivered. */
  private static Path undeliveredDir(Path base) throws Exception {
    return makeUndelivered(base.resolve("undelivered"));
  }

  private static Path makeUndelivered(Path dir) throws Exception {
    ChronicleStoreForward sf = open(dir);
    sf.start(
        records -> {
          throw new IOException("down");
        });
    sf.offer(List.of(record("x")));
    await(sf::isBuffering);
    sf.close();
    return dir;
  }

  /**
   * A directory where the first record was delivered (advancing the ack watermark to a concrete
   * index) but the second is stuck undelivered — exercises the numeric-watermark branch of {@link
   * ChronicleStoreForward#hasUndeliveredRecords}.
   */
  private static Path makePartiallyDrained(Path dir) throws Exception {
    List<RecordFleakData> delivered = new CopyOnWriteArrayList<>();
    AtomicInteger attempts = new AtomicInteger();
    // drainChunkSize 1 so each record is its own chunk/ack step.
    ChronicleStoreForward sf =
        new ChronicleStoreForward(
            new ChronicleStoreForward.Config(dir, 1_000_000, 50, 1, "node-1"),
            IO_IS_CONNECTION,
            null,
            null,
            null);
    sf.start(
        records -> {
          if (attempts.getAndIncrement() == 0) {
            delivered.addAll(records); // first chunk delivered -> ack advances to a numeric index
          } else {
            throw new IOException("down"); // everything after stays undelivered
          }
        });
    sf.offer(List.of(record("x"), record("y")));
    await(() -> delivered.size() == 1 && attempts.get() >= 2);
    sf.close();
    return dir;
  }

  private static void writeRawAck(Path dir, String token) throws IOException {
    Files.writeString(dir.resolve(ChronicleStoreForward.ACK_FILE_NAME), token);
  }

  private static RecordFleakData record(String value) {
    return (RecordFleakData) FleakData.wrap(Map.of("v", value));
  }

  private static void age(Path dir, Duration back) throws IOException {
    FileTime t = FileTime.fromMillis(System.currentTimeMillis() - back.toMillis());
    Files.setLastModifiedTime(dir, t);
    try (Stream<Path> files = Files.list(dir)) {
      for (Path f : (Iterable<Path>) files::iterator) {
        Files.setLastModifiedTime(f, t);
      }
    }
  }

  private static void await(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (System.nanoTime() < deadline) {
      if (condition.getAsBoolean()) {
        return;
      }
      TimeUnit.MILLISECONDS.sleep(20);
    }
    fail("condition not met within timeout");
  }
}
