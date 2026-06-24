# FS Source Parallel Sharded Reads Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let Grid run N replicas of an `fssource` task where each replica processes a disjoint subset of files via stable hash partitioning of the file URN.

**Architecture:** Grid's jobmaster injects `REPLICA_INDEX`/`REPLICA_COUNT` into each per-replica DAG's `jobContext.otherProperties` at fan-out. zephflow-core's `FsSourceCommand` reads those keys, applies a `Partitioner.owns(urn, index, count)` filter in its existing list-and-filter stream, and folds the replica identity into the checkpoint `sourceId` so each replica checkpoints independently. A shared `DagJobContextInjector` helper in Grid's `:lib` module removes the duplicated locate-or-create JSON walking that `TaskSupervisor` currently carries.

**Tech Stack:** Java 21, JUnit 5, Guava (`com.google.common.hash.Hashing`), Jackson (`com.fasterxml.jackson.databind`), Gradle. Two repos: `zephflow-core` and `grid`.

## Global Constraints

- Repos are separate: `/Users/dan/fleak/zephflow-core` (branch `FLE-2110`) and `/Users/dan/fleak/grid` (branch `fssource-checkpoint-url-injection`).
- The cross-repo contract is by string literal only. New key names must be byte-identical in both repos: `REPLICA_INDEX` and `REPLICA_COUNT`.
- Every new `.java` file must start with the Apache 2.0 license header used across the repo (copy verbatim from any existing file in the same module, e.g. `SourceIdHasher.java`). Grid `:lib` files use the same header style; jobmaster files do not carry it — match the module's existing convention.
- Use sha256 for hashing (consistency with `SourceIdHasher`), reduced to int via `HashCode.asInt()`.
- Partition key is `fileEntry.key().urn()` — the full scheme-prefixed path (e.g. `s3://bucket/logs/app.json`), never the bare filename.
- Use `Math.floorMod` (never `%`) when reducing a hash to a bucket.
- Sharding and the replica-aware `sourceId` engage ONLY when `replicaCount > 1`. With count ≤ 1 or keys absent, behavior is byte-for-byte identical to today (own all files, legacy `sourceId`, existing checkpoints stay valid).
- zephflow-core commit messages end with the `Co-Authored-By: Claude <noreply@anthropic.com>` trailer. Grid commits follow grid's existing convention (no trailer).
- Spotless runs on build in both repos. If a build fails on formatting, run the module's `spotlessApply` and re-commit.

---

## File Structure

**zephflow-core:**
- `api/.../api/JobContext.java` — add two constants (modify).
- `lib/.../fssource/util/Partitioner.java` — new shard-ownership utility.
- `lib/.../fssource/util/SourceIdHasher.java` — add replica-aware overload (modify).
- `lib/.../fssource/FsSourceExecutionContext.java` — add `replicaIndex`/`replicaCount` fields (modify).
- `lib/.../fssource/FsSourceCommand.java` — read keys, apply shard filter, use new `sourceId` (modify).
- Tests: `PartitionerTest`, `SourceIdHasherTest` (extend), `FsSourceCommandShardingTest` (new).

**grid:**
- `lib/.../grid/lib/utils/DagJobContextInjector.java` — new shared JSON-injection helper.
- `lib/.../grid/lib/task/execution/...` — no DTO changes.
- `jobmaster/.../service/task/execution/JobService.java` — inject replica keys in fan-out loop (modify).
- `workeragent/.../execution/TaskSupervisor.java` — refactor `injectMetricTags`/`injectCheckpointUrl` onto the shared helper (modify).
- Tests: `DagJobContextInjectorTest` (new), `JobServiceTest` replica-injection cases.

---

## Task 1: Add replica jobContext key constants (zephflow-core)

**Files:**
- Modify: `api/src/main/java/io/fleak/zephflow/api/JobContext.java:35-36`

**Interfaces:**
- Produces: `JobContext.REPLICA_INDEX` (`String` = `"REPLICA_INDEX"`), `JobContext.REPLICA_COUNT` (`String` = `"REPLICA_COUNT"`).

- [ ] **Step 1: Add the constants**

In `JobContext.java`, the constants block currently reads:

```java
  public static final String FLAG_TEST_MODE = "TEST_MODE";
  public static final String DATA_KEY_PREFIX = "DATA_KEY_PREFIX";
  public static final String CHECKPOINT_URL = "CHECKPOINT_URL";
```

Add the two new constants directly after `CHECKPOINT_URL`:

```java
  public static final String FLAG_TEST_MODE = "TEST_MODE";
  public static final String DATA_KEY_PREFIX = "DATA_KEY_PREFIX";
  public static final String CHECKPOINT_URL = "CHECKPOINT_URL";
  // Replica sharding for source commands (e.g. fssource): this replica's 0-based ordinal and the
  // total replica count. Injected by Grid's jobmaster at fan-out when replicaCount > 1.
  public static final String REPLICA_INDEX = "REPLICA_INDEX";
  public static final String REPLICA_COUNT = "REPLICA_COUNT";
```

- [ ] **Step 2: Compile**

Run: `./gradlew :api:compileJava`
Expected: `BUILD SUCCESSFUL`

- [ ] **Step 3: Commit**

```bash
git add api/src/main/java/io/fleak/zephflow/api/JobContext.java
git commit -m "feat(api): add REPLICA_INDEX/REPLICA_COUNT jobContext keys

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 2: Partitioner utility (zephflow-core)

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/util/Partitioner.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/util/PartitionerTest.java`

**Interfaces:**
- Produces: `static boolean Partitioner.owns(String urn, int replicaIndex, int replicaCount)` — returns true if this replica owns the file. `replicaCount <= 1` → always true.

- [ ] **Step 1: Write the failing test**

Create `PartitionerTest.java`:

```java
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
package io.fleak.zephflow.lib.commands.fssource.util;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PartitionerTest {

  private static List<String> sampleUrns(int n) {
    List<String> urns = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      urns.add("s3://bucket/logs/2026/06/app-" + i + ".json");
    }
    return urns;
  }

  @Test
  void singleReplicaOwnsEverything() {
    for (String urn : sampleUrns(50)) {
      assertTrue(Partitioner.owns(urn, 0, 1));
    }
  }

  @Test
  void zeroOrNegativeCountOwnsEverything() {
    assertTrue(Partitioner.owns("s3://bucket/x.json", 0, 0));
  }

  @Test
  void deterministicForSameInput() {
    String urn = "s3://bucket/logs/app-42.json";
    assertEquals(Partitioner.owns(urn, 1, 3), Partitioner.owns(urn, 1, 3));
  }

  @Test
  void everyUrnOwnedByExactlyOneReplica_noGapsNoOverlap() {
    int count = 4;
    List<String> urns = sampleUrns(500);
    for (String urn : urns) {
      int owners = 0;
      for (int idx = 0; idx < count; idx++) {
        if (Partitioner.owns(urn, idx, count)) owners++;
      }
      assertEquals(1, owners, "urn must be owned by exactly one replica: " + urn);
    }
  }

  @Test
  void unionOfReplicaSubsetsIsFullSet() {
    int count = 3;
    List<String> urns = sampleUrns(300);
    Set<String> covered = new HashSet<>();
    for (int idx = 0; idx < count; idx++) {
      for (String urn : urns) {
        if (Partitioner.owns(urn, idx, count)) covered.add(urn);
      }
    }
    assertEquals(new HashSet<>(urns), covered);
  }

  @Test
  void distributionIsRoughlyEven() {
    int count = 4;
    int[] buckets = new int[count];
    List<String> urns = sampleUrns(4000);
    for (String urn : urns) {
      for (int idx = 0; idx < count; idx++) {
        if (Partitioner.owns(urn, idx, count)) buckets[idx]++;
      }
    }
    int expected = urns.size() / count;
    for (int b : buckets) {
      // Allow generous slack; we only guard against gross skew (e.g. all in one bucket).
      assertTrue(b > expected * 0.6 && b < expected * 1.4, "bucket skew: " + b);
    }
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.util.PartitionerTest"`
Expected: FAIL — `Partitioner` does not exist (compilation error).

- [ ] **Step 3: Write minimal implementation**

Create `Partitioner.java`:

```java
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
package io.fleak.zephflow.lib.commands.fssource.util;

import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;

/**
 * Stateless, deterministic file-to-replica assignment. A file is owned by exactly one replica,
 * decided by a stable hash of its URN. No coordination between replicas.
 */
public final class Partitioner {

  private Partitioner() {}

  /**
   * @param urn the file's stable, scheme-prefixed URN (e.g. {@code s3://bucket/key})
   * @param replicaIndex this replica's 0-based ordinal
   * @param replicaCount total number of replicas
   * @return true if this replica owns the file; always true when {@code replicaCount <= 1}
   */
  public static boolean owns(String urn, int replicaIndex, int replicaCount) {
    if (replicaCount <= 1) {
      return true;
    }
    int bucket = Math.floorMod(hash(urn), replicaCount);
    return bucket == replicaIndex;
  }

  private static int hash(String urn) {
    return Hashing.sha256().hashString(urn, StandardCharsets.UTF_8).asInt();
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.util.PartitionerTest"`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/util/Partitioner.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/util/PartitionerTest.java
git commit -m "feat(fssource): add Partitioner for stable replica file ownership

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 3: Replica-aware SourceIdHasher overload (zephflow-core)

**Files:**
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/util/SourceIdHasher.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/util/SourceIdHasherTest.java` (create if absent)

**Interfaces:**
- Consumes: existing `static String SourceIdHasher.compute(String backend, String root, String fileNameRegex)`.
- Produces: `static String SourceIdHasher.compute(String backend, String root, String fileNameRegex, int replicaIndex, int replicaCount)` — when `replicaCount <= 1`, returns exactly the 3-arg result; otherwise folds index+count into the hash.

- [ ] **Step 1: Write the failing test**

Create `SourceIdHasherTest.java`:

```java
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
package io.fleak.zephflow.lib.commands.fssource.util;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class SourceIdHasherTest {

  @Test
  void singleReplicaOverloadMatchesLegacyThreeArg() {
    String legacy = SourceIdHasher.compute("s3", "s3://bucket/root", "evt_(?<ts>\\d+)\\.log");
    String shardedCount1 =
        SourceIdHasher.compute("s3", "s3://bucket/root", "evt_(?<ts>\\d+)\\.log", 0, 1);
    assertEquals(legacy, shardedCount1);
  }

  @Test
  void countZeroAlsoMatchesLegacy() {
    String legacy = SourceIdHasher.compute("file", "file:///data", null);
    assertEquals(legacy, SourceIdHasher.compute("file", "file:///data", null, 0, 0));
  }

  @Test
  void distinctIdsPerReplicaWhenCountGreaterThanOne() {
    String r0 = SourceIdHasher.compute("s3", "s3://bucket/root", null, 0, 3);
    String r1 = SourceIdHasher.compute("s3", "s3://bucket/root", null, 1, 3);
    String r2 = SourceIdHasher.compute("s3", "s3://bucket/root", null, 2, 3);
    assertNotEquals(r0, r1);
    assertNotEquals(r1, r2);
    assertNotEquals(r0, r2);
  }

  @Test
  void shardedIdDiffersFromLegacyWhenCountGreaterThanOne() {
    String legacy = SourceIdHasher.compute("s3", "s3://bucket/root", null);
    assertNotEquals(legacy, SourceIdHasher.compute("s3", "s3://bucket/root", null, 0, 3));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.util.SourceIdHasherTest"`
Expected: FAIL — 5-arg `compute` does not exist (compilation error).

- [ ] **Step 3: Write minimal implementation**

In `SourceIdHasher.java`, the file currently has one `compute` method. Add the overload below it (keep the existing 3-arg method unchanged):

```java
  public static String compute(String backend, String root, String fileNameRegex) {
    String canonical = backend + "\n" + root + "\n" + (fileNameRegex == null ? "" : fileNameRegex);
    return Hashing.sha256()
        .hashString(canonical, StandardCharsets.UTF_8)
        .toString()
        .substring(0, 16);
  }

  /**
   * Replica-aware source id. When {@code replicaCount <= 1} this returns exactly the same id as the
   * three-argument overload, so existing single-replica checkpoints remain valid. When {@code
   * replicaCount > 1}, the replica index and count are folded into the hash so each replica owns an
   * isolated checkpoint.
   */
  public static String compute(
      String backend, String root, String fileNameRegex, int replicaIndex, int replicaCount) {
    if (replicaCount <= 1) {
      return compute(backend, root, fileNameRegex);
    }
    String canonical =
        backend
            + "\n"
            + root
            + "\n"
            + (fileNameRegex == null ? "" : fileNameRegex)
            + "\n"
            + replicaIndex
            + "\n"
            + replicaCount;
    return Hashing.sha256()
        .hashString(canonical, StandardCharsets.UTF_8)
        .toString()
        .substring(0, 16);
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.util.SourceIdHasherTest"`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/util/SourceIdHasher.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/util/SourceIdHasherTest.java
git commit -m "feat(fssource): replica-aware SourceIdHasher overload (back-compatible)

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 4: Wire replica sharding into FsSourceCommand (zephflow-core)

**Files:**
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceExecutionContext.java`
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommand.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommandShardingTest.java`

**Interfaces:**
- Consumes: `JobContext.REPLICA_INDEX`, `JobContext.REPLICA_COUNT` (Task 1); `Partitioner.owns` (Task 2); `SourceIdHasher.compute(...5 args)` (Task 3).
- Produces: `FsSourceExecutionContext.replicaIndex` (int), `FsSourceExecutionContext.replicaCount` (int).

- [ ] **Step 1: Write the failing test**

Create `FsSourceCommandShardingTest.java`. This drives the real `FsSourceCommand` over a local temp dir with the in-memory checkpoint (no `CHECKPOINT_URL`), three replicas, and asserts disjoint-union coverage.

```java
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
package io.fleak.zephflow.lib.commands.fssource;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendRegistry;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackend;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class FsSourceCommandShardingTest {

  @BeforeEach
  void setUp() {
    FsBackendRegistry.unregister("file");
    FsBackendRegistry.register(new LocalFsBackend());
  }

  @AfterEach
  void tearDown() {
    FsBackendRegistry.unregister("file");
  }

  private List<String> runReplica(Path tempDir, int replicaIndex, int replicaCount)
      throws Exception {
    JobContext jobContext =
        JobContext.builder()
            .otherProperties(
                new HashMap<>(
                    Map.of(
                        JobContext.REPLICA_INDEX, String.valueOf(replicaIndex),
                        JobContext.REPLICA_COUNT, String.valueOf(replicaCount))))
            .build();
    Map<String, Object> rawConfig =
        Map.of(
            "backend", "file",
            "root", tempDir.toUri().toString(),
            "fileNameRegex", "evt_(?<ts>\\d+)\\.log",
            "encodingType", "JSON_OBJECT_LINE");
    List<String> emitted = new ArrayList<>();
    SourceEventAcceptor out =
        new SourceEventAcceptor() {
          @Override
          public void accept(List<RecordFleakData> record) {
            record.forEach(r -> emitted.add((String) r.unwrap().get("v")));
          }

          @Override
          public void terminate() {}
        };
    FsSourceCommand command = new FsSourceCommand("n", jobContext);
    command.parseAndValidateArg(rawConfig);
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
    command.execute("u", out);
    return emitted;
  }

  @Test
  void threeReplicasProcessDisjointUnionOfAllFiles(@TempDir Path tempDir) throws Exception {
    Set<String> expected = new HashSet<>();
    for (int i = 1; i <= 30; i++) {
      Files.writeString(tempDir.resolve("evt_" + i + ".log"), "{\"v\":\"" + i + "\"}");
      expected.add(String.valueOf(i));
    }

    List<String> r0 = runReplica(tempDir, 0, 3);
    List<String> r1 = runReplica(tempDir, 1, 3);
    List<String> r2 = runReplica(tempDir, 2, 3);

    // Disjoint: no value emitted by more than one replica.
    Set<String> all = new HashSet<>();
    for (List<String> r : List.of(r0, r1, r2)) {
      for (String v : r) {
        assertTrue(all.add(v), "value processed by more than one replica: " + v);
      }
    }
    // Union: every file processed exactly once across replicas.
    assertEquals(expected, all);
    // Each replica did real work (guards against one replica grabbing everything).
    assertFalse(r0.isEmpty());
    assertFalse(r1.isEmpty());
    assertFalse(r2.isEmpty());
  }

  @Test
  void noReplicaKeysOwnsAllFiles(@TempDir Path tempDir) throws Exception {
    Set<String> expected = new HashSet<>();
    for (int i = 1; i <= 10; i++) {
      Files.writeString(tempDir.resolve("evt_" + i + ".log"), "{\"v\":\"" + i + "\"}");
      expected.add(String.valueOf(i));
    }
    JobContext jobContext = JobContext.builder().build();
    Map<String, Object> rawConfig =
        Map.of(
            "backend", "file",
            "root", tempDir.toUri().toString(),
            "fileNameRegex", "evt_(?<ts>\\d+)\\.log",
            "encodingType", "JSON_OBJECT_LINE");
    List<String> emitted = new ArrayList<>();
    SourceEventAcceptor out =
        new SourceEventAcceptor() {
          @Override
          public void accept(List<RecordFleakData> record) {
            record.forEach(r -> emitted.add((String) r.unwrap().get("v")));
          }

          @Override
          public void terminate() {}
        };
    FsSourceCommand command = new FsSourceCommand("n", jobContext);
    command.parseAndValidateArg(rawConfig);
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
    command.execute("u", out);
    assertEquals(expected, new HashSet<>(emitted));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.FsSourceCommandShardingTest"`
Expected: FAIL — `threeReplicasProcessDisjointUnionOfAllFiles` fails because, without the shard filter, every replica emits all 30 files, so `all.add(v)` trips the "more than one replica" assertion. (`noReplicaKeysOwnsAllFiles` may already pass.)

- [ ] **Step 3: Add fields to FsSourceExecutionContext**

In `FsSourceExecutionContext.java`, the field block currently is:

```java
  FsBackend backend;
  FsBackendConfig backendConfig;
  FileLister lister;
  FileReader reader;
  CheckpointClient checkpointClient;
```

Add two int fields after `checkpointClient`:

```java
  FsBackend backend;
  FsBackendConfig backendConfig;
  FileLister lister;
  FileReader reader;
  CheckpointClient checkpointClient;
  int replicaIndex;
  int replicaCount = 1;
```

- [ ] **Step 4: Read the keys in createExecutionContext**

In `FsSourceCommand.java`, `createExecutionContext` currently ends with:

```java
    executionContext.checkpointClient = buildCheckpointClient(jobContext);
    return executionContext;
  }
```

Change it to read the replica keys before returning:

```java
    executionContext.checkpointClient = buildCheckpointClient(jobContext);
    executionContext.replicaIndex =
        parseIntProperty(jobContext, JobContext.REPLICA_INDEX, 0);
    executionContext.replicaCount =
        parseIntProperty(jobContext, JobContext.REPLICA_COUNT, 1);
    return executionContext;
  }

  private static int parseIntProperty(JobContext jobContext, String key, int defaultValue) {
    Object value = jobContext.getOtherProperties().get(key);
    if (value == null) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(value.toString().trim());
    } catch (NumberFormatException numberFormatException) {
      log.warn("fs_source: unparseable {}={}; using default {}", key, value, defaultValue);
      return defaultValue;
    }
  }
```

- [ ] **Step 5: Use replica-aware sourceId and add the shard filter in execute()**

In `FsSourceCommand.execute`, the `sourceId` line currently is:

```java
    String sourceId =
        SourceIdHasher.compute(config.getBackend(), config.getRoot(), config.getFileNameRegex());
```

Change it to the 5-arg overload:

```java
    String sourceId =
        SourceIdHasher.compute(
            config.getBackend(),
            config.getRoot(),
            config.getFileNameRegex(),
            executionContext.replicaIndex,
            executionContext.replicaCount);
```

Then, in the stream filter chain, the current code is:

```java
          .filter(pending -> pending.timestamp().compareTo(checkpoint.watermark()) >= 0)
          .filter(pending -> !checkpoint.isCompleted(pending.entry().key().urn()))
```

Add the shard filter as the first filter (before the watermark filter), so non-owned files are dropped earliest:

```java
          .filter(
              pending ->
                  Partitioner.owns(
                      pending.entry().key().urn(),
                      executionContext.replicaIndex,
                      executionContext.replicaCount))
          .filter(pending -> pending.timestamp().compareTo(checkpoint.watermark()) >= 0)
          .filter(pending -> !checkpoint.isCompleted(pending.entry().key().urn()))
```

Add the import near the other `fssource.util` import:

```java
import io.fleak.zephflow.lib.commands.fssource.util.Partitioner;
```

- [ ] **Step 6: Run the sharding test to verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.FsSourceCommandShardingTest"`
Expected: PASS (2 tests).

- [ ] **Step 7: Run the existing fssource tests to verify no regression**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.*"`
Expected: PASS — including `FsSourceCommandResumeTest` (single-replica path unchanged).

- [ ] **Step 8: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceExecutionContext.java \
        lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommand.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommandShardingTest.java
git commit -m "feat(fssource): shard files across replicas via Partitioner + replica-aware sourceId

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

## Task 5: Shared DagJobContextInjector helper (grid)

> **Repo switch:** all remaining tasks are in `/Users/dan/fleak/grid` on branch `fssource-checkpoint-url-injection`.

**Files:**
- Create: `lib/src/main/java/io/fleak/grid/lib/utils/DagJobContextInjector.java`
- Test: `lib/src/test/java/io/fleak/grid/lib/utils/DagJobContextInjectorTest.java`

**Interfaces:**
- Produces:
  - `static void DagJobContextInjector.putOtherProperty(JsonNode dag, String key, String value)`
  - `static void DagJobContextInjector.putMetricTag(JsonNode dag, String key, String value)`
  - Both locate-or-create `jobContext` and the target child object (`otherProperties` / `metricTags`); no-op (returns) if `dag` is not an `ObjectNode`.

- [ ] **Step 1: Write the failing test**

Create `DagJobContextInjectorTest.java`. (Grid `:lib` uses the Apache header — copy it from `lib/src/main/java/io/fleak/grid/lib/utils/JsonUtils2.java`.)

```java
/* <Apache 2.0 header copied verbatim from io/fleak/grid/lib/utils/JsonUtils2.java> */
package io.fleak.grid.lib.utils;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class DagJobContextInjectorTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static JsonNode dag(String json) throws Exception {
    return MAPPER.readTree(json);
  }

  @Test
  void putOtherPropertyCreatesJobContextAndOtherPropertiesWhenMissing() throws Exception {
    JsonNode dag = dag("{\"dag\":[]}");
    DagJobContextInjector.putOtherProperty(dag, "REPLICA_COUNT", "3");
    assertEquals(
        "3", dag.get("jobContext").get("otherProperties").get("REPLICA_COUNT").asText());
  }

  @Test
  void putOtherPropertyPreservesExistingEntries() throws Exception {
    JsonNode dag = dag("{\"jobContext\":{\"otherProperties\":{\"existing\":\"keep\"}}}");
    DagJobContextInjector.putOtherProperty(dag, "REPLICA_INDEX", "1");
    assertEquals("keep", dag.get("jobContext").get("otherProperties").get("existing").asText());
    assertEquals("1", dag.get("jobContext").get("otherProperties").get("REPLICA_INDEX").asText());
  }

  @Test
  void putMetricTagCreatesMetricTagsWhenMissing() throws Exception {
    JsonNode dag = dag("{\"jobContext\":{}}");
    DagJobContextInjector.putMetricTag(dag, "worker_id", "w1");
    assertEquals("w1", dag.get("jobContext").get("metricTags").get("worker_id").asText());
  }

  @Test
  void nonObjectDagIsNoOp() throws Exception {
    JsonNode dag = dag("[]");
    // Must not throw.
    DagJobContextInjector.putOtherProperty(dag, "REPLICA_COUNT", "3");
    assertFalse(dag.has("jobContext"));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :lib:test --tests "io.fleak.grid.lib.utils.DagJobContextInjectorTest"`
Expected: FAIL — `DagJobContextInjector` does not exist (compilation error).

- [ ] **Step 3: Write minimal implementation**

Create `DagJobContextInjector.java`:

```java
/* <Apache 2.0 header copied verbatim from io/fleak/grid/lib/utils/JsonUtils2.java> */
package io.fleak.grid.lib.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;

/**
 * Locate-or-create helper for injecting values into a DAG's {@code jobContext} sub-objects. Both
 * the jobmaster (replica info, at fan-out) and the worker (metric tags, checkpoint URL, at launch)
 * mutate the DAG JSON; this centralizes the traversal so each call site does not duplicate it.
 */
@Slf4j
public final class DagJobContextInjector {

  private DagJobContextInjector() {}

  /** Put {@code key=value} into {@code jobContext.otherProperties}, creating nodes as needed. */
  public static void putOtherProperty(JsonNode dag, String key, String value) {
    ObjectNode child = childObject(dag, "otherProperties");
    if (child != null) {
      child.put(key, value);
    }
  }

  /** Put {@code key=value} into {@code jobContext.metricTags}, creating nodes as needed. */
  public static void putMetricTag(JsonNode dag, String key, String value) {
    ObjectNode child = childObject(dag, "metricTags");
    if (child != null) {
      child.put(key, value);
    }
  }

  private static ObjectNode childObject(JsonNode dag, String childName) {
    if (!(dag instanceof ObjectNode dagNode)) {
      log.warn("DAG is not an ObjectNode; cannot inject {}", childName);
      return null;
    }
    ObjectNode jobContext;
    if (dagNode.has("jobContext") && dagNode.get("jobContext").isObject()) {
      jobContext = (ObjectNode) dagNode.get("jobContext");
    } else {
      jobContext = dagNode.objectNode();
      dagNode.set("jobContext", jobContext);
    }
    ObjectNode child;
    if (jobContext.has(childName) && jobContext.get(childName).isObject()) {
      child = (ObjectNode) jobContext.get(childName);
    } else {
      child = jobContext.objectNode();
      jobContext.set(childName, child);
    }
    return child;
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.grid.lib.utils.DagJobContextInjectorTest"`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/grid/lib/utils/DagJobContextInjector.java \
        lib/src/test/java/io/fleak/grid/lib/utils/DagJobContextInjectorTest.java
git commit -m "feat(lib): shared DagJobContextInjector for jobContext mutation"
```

---

## Task 6: Refactor TaskSupervisor onto the shared injector (grid)

**Files:**
- Modify: `workeragent/src/main/java/io/fleak/grid/workeragent/execution/TaskSupervisor.java`

**Interfaces:**
- Consumes: `DagJobContextInjector.putOtherProperty`, `DagJobContextInjector.putMetricTag` (Task 5).

This task is a behavior-preserving refactor: replace the inline locate-or-create walks in `injectMetricTags` and `injectCheckpointUrl` with calls to the shared helper. The existing `TaskSupervisorInjectCheckpointUrlTest` (already on this branch) is the regression guard.

- [ ] **Step 1: Add the import**

In `TaskSupervisor.java`, add to the import block (alphabetically near the other `io.fleak.grid.lib` imports):

```java
import io.fleak.grid.lib.utils.DagJobContextInjector;
```

- [ ] **Step 2: Replace injectMetricTags body**

The current method is:

```java
  private static void injectMetricTags(JsonNode dag, UUID jobId, UUID taskId, String workerId) {
    if (!(dag instanceof ObjectNode dagNode)) {
      log.warn("DAG is not an ObjectNode, cannot inject metric tags");
      return;
    }
    ObjectNode jobContext;
    if (dagNode.has("jobContext") && dagNode.get("jobContext").isObject()) {
      jobContext = (ObjectNode) dagNode.get("jobContext");
    } else {
      jobContext = dagNode.objectNode();
      dagNode.set("jobContext", jobContext);
    }
    ObjectNode metricTags;
    if (jobContext.has("metricTags") && jobContext.get("metricTags").isObject()) {
      metricTags = (ObjectNode) jobContext.get("metricTags");
    } else {
      metricTags = jobContext.objectNode();
      jobContext.set("metricTags", metricTags);
    }
    metricTags.put("job_id", jobId.toString());
    metricTags.put("task_id", taskId.toString());
    metricTags.put("worker_id", workerId);
  }
```

Replace its body with:

```java
  private static void injectMetricTags(JsonNode dag, UUID jobId, UUID taskId, String workerId) {
    DagJobContextInjector.putMetricTag(dag, "job_id", jobId.toString());
    DagJobContextInjector.putMetricTag(dag, "task_id", taskId.toString());
    DagJobContextInjector.putMetricTag(dag, "worker_id", workerId);
  }
```

- [ ] **Step 3: Replace injectCheckpointUrl body**

The current method (after its Javadoc) is:

```java
  static void injectCheckpointUrl(JsonNode dag, String masterUrl) {
    if (masterUrl == null || masterUrl.isBlank()) {
      log.warn("Master URL not configured; not injecting {}", CHECKPOINT_URL_KEY);
      return;
    }
    if (!(dag instanceof ObjectNode dagNode)) {
      log.warn("DAG is not an ObjectNode, cannot inject {}", CHECKPOINT_URL_KEY);
      return;
    }
    ObjectNode jobContext;
    if (dagNode.has("jobContext") && dagNode.get("jobContext").isObject()) {
      jobContext = (ObjectNode) dagNode.get("jobContext");
    } else {
      jobContext = dagNode.objectNode();
      dagNode.set("jobContext", jobContext);
    }
    ObjectNode otherProperties;
    if (jobContext.has("otherProperties") && jobContext.get("otherProperties").isObject()) {
      otherProperties = (ObjectNode) jobContext.get("otherProperties");
    } else {
      otherProperties = jobContext.objectNode();
      jobContext.set("otherProperties", otherProperties);
    }
    String base =
        masterUrl.endsWith("/") ? masterUrl.substring(0, masterUrl.length() - 1) : masterUrl;
    otherProperties.put(CHECKPOINT_URL_KEY, base + STATE_API_PATH);
  }
```

Replace its body with:

```java
  static void injectCheckpointUrl(JsonNode dag, String masterUrl) {
    if (masterUrl == null || masterUrl.isBlank()) {
      log.warn("Master URL not configured; not injecting {}", CHECKPOINT_URL_KEY);
      return;
    }
    String base =
        masterUrl.endsWith("/") ? masterUrl.substring(0, masterUrl.length() - 1) : masterUrl;
    DagJobContextInjector.putOtherProperty(dag, CHECKPOINT_URL_KEY, base + STATE_API_PATH);
  }
```

Note: the `if (!(dag instanceof ObjectNode))` guard is now handled inside the helper, so it is intentionally removed here.

- [ ] **Step 4: Remove the now-unused ObjectNode import if it is no longer referenced**

Check whether `ObjectNode` is still used elsewhere in `TaskSupervisor.java`:

Run: `grep -n "ObjectNode" workeragent/src/main/java/io/fleak/grid/workeragent/execution/TaskSupervisor.java`
- If the only remaining hits were the two methods you just edited (i.e. no other references), remove `import com.fasterxml.jackson.databind.node.ObjectNode;`.
- If `ObjectNode` is referenced anywhere else, leave the import.

- [ ] **Step 5: Run the existing injection regression tests**

Run: `./gradlew :workeragent:test --tests "io.fleak.grid.workeragent.execution.*"`
Expected: PASS — including `TaskSupervisorInjectCheckpointUrlTest` (all 5 cases), proving the refactor preserved behavior.

- [ ] **Step 6: Commit**

```bash
git add workeragent/src/main/java/io/fleak/grid/workeragent/execution/TaskSupervisor.java
git commit -m "refactor(workeragent): route TaskSupervisor injection through shared helper"
```

---

## Task 7: Inject replica info at fan-out in JobService (grid)

**Files:**
- Modify: `jobmaster/src/main/java/io/fleak/grid/jobmaster/service/task/execution/JobService.java`
- Test: `jobmaster/src/test/java/io/fleak/grid/jobmaster/service/task/execution/JobServiceReplicaInjectionTest.java`

**Interfaces:**
- Consumes: `DagJobContextInjector.putOtherProperty` (Task 5). Key literals `"REPLICA_INDEX"`, `"REPLICA_COUNT"` (must match `JobContext` constants in zephflow-core).

**Critical ordering note:** the fan-out loop reuses the *same* `jobTaskSubmit` (and thus the same `dag` ObjectNode) across iterations, but `TaskService.createTask` deep-copies the DAG (`JacksonBridge.toJackson3`) when it persists each task. Therefore replica keys MUST be set on the shared `dag` node *immediately before* `createAndInitiateTask` in each iteration — each persisted copy then captures that iteration's index. The last iteration leaves the shared node holding the final index, which is harmless because nothing reads it after the loop.

- [ ] **Step 1: Write the failing test**

The current fan-out (`createJob` / its helper) calls `createAndInitiateTask(jobTaskSubmit, ...)`, which delegates to `taskService.createTask` and persists. A focused unit test that exercises the full Spring service would need heavy mocking. Instead, extract the injection decision into a small package-private static method on `JobService` and test that directly.

Create `JobServiceReplicaInjectionTest.java` (jobmaster tests use the Apache header? No — jobmaster sources do not carry it; match neighbors like `InfluxDBMetricsServiceTest`, which has no header):

```java
package io.fleak.grid.jobmaster.service.task.execution;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class JobServiceReplicaInjectionTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static JsonNode dag(String json) throws Exception {
    return MAPPER.readTree(json);
  }

  @Test
  void injectsBothKeysWhenReplicasGreaterThanOne() throws Exception {
    JsonNode dag = dag("{\"jobContext\":{\"otherProperties\":{}}}");
    JobService.injectReplicaInfo(dag, 1, 3);
    JsonNode props = dag.get("jobContext").get("otherProperties");
    assertEquals("1", props.get("REPLICA_INDEX").asText());
    assertEquals("3", props.get("REPLICA_COUNT").asText());
  }

  @Test
  void injectsNothingWhenSingleReplica() throws Exception {
    JsonNode dag = dag("{\"jobContext\":{\"otherProperties\":{}}}");
    JobService.injectReplicaInfo(dag, 0, 1);
    JsonNode props = dag.get("jobContext").get("otherProperties");
    assertFalse(props.has("REPLICA_INDEX"));
    assertFalse(props.has("REPLICA_COUNT"));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :jobmaster:test --tests "io.fleak.grid.jobmaster.service.task.execution.JobServiceReplicaInjectionTest"`
Expected: FAIL — `JobService.injectReplicaInfo` does not exist (compilation error).

- [ ] **Step 3: Add the import and the injectReplicaInfo method**

In `JobService.java`, add the import (near the other `io.fleak.grid.lib` import on line 9):

```java
import io.fleak.grid.lib.utils.DagJobContextInjector;
```

Add this package-private static method to the `JobService` class (place it near the other helpers, e.g. after `validateJob`):

```java
  static final String REPLICA_INDEX_KEY = "REPLICA_INDEX";
  static final String REPLICA_COUNT_KEY = "REPLICA_COUNT";

  /**
   * Inject this replica's index and the total replica count into {@code
   * jobContext.otherProperties}. Source commands in zephflow-core (e.g. fssource) read these to
   * process a disjoint subset of files. No-op when {@code replicaCount <= 1} so single-replica jobs
   * are unchanged. Key names must match io.fleak.zephflow.api.JobContext.REPLICA_INDEX/COUNT.
   */
  static void injectReplicaInfo(JsonNode dag, int replicaIndex, int replicaCount) {
    if (replicaCount <= 1) {
      return;
    }
    DagJobContextInjector.putOtherProperty(dag, REPLICA_INDEX_KEY, String.valueOf(replicaIndex));
    DagJobContextInjector.putOtherProperty(dag, REPLICA_COUNT_KEY, String.valueOf(replicaCount));
  }
```

Add the `JsonNode` import if not already present:

Run: `grep -n "import com.fasterxml.jackson.databind.JsonNode;" jobmaster/src/main/java/io/fleak/grid/jobmaster/service/task/execution/JobService.java`
- If absent, add `import com.fasterxml.jackson.databind.JsonNode;` to the import block.

- [ ] **Step 4: Run the test to verify it passes**

Run: `./gradlew :jobmaster:test --tests "io.fleak.grid.jobmaster.service.task.execution.JobServiceReplicaInjectionTest"`
Expected: PASS (2 tests).

- [ ] **Step 5: Call injectReplicaInfo in the fan-out loop**

The current fan-out loop is:

```java
        int replicas = Math.max(0, jobTask.getReplicas());
        for (var i = 0; i < replicas; i++) {
          if (replicas == 1) {
            jobTaskSubmit.setExternalId(originalExternalId);
          } else {
            if (originalExternalId != null) {
              jobTaskSubmit.setExternalId(originalExternalId + "-" + i);
            }
          }
          createAndInitiateTask(jobTaskSubmit, job.getJobId());
        }
```

Add the injection immediately before `createAndInitiateTask`, so each persisted DAG copy captures this iteration's index:

```java
        int replicas = Math.max(0, jobTask.getReplicas());
        for (var i = 0; i < replicas; i++) {
          if (replicas == 1) {
            jobTaskSubmit.setExternalId(originalExternalId);
          } else {
            if (originalExternalId != null) {
              jobTaskSubmit.setExternalId(originalExternalId + "-" + i);
            }
          }
          injectReplicaInfo(jobTaskSubmit.getDag(), i, replicas);
          createAndInitiateTask(jobTaskSubmit, job.getJobId());
        }
```

- [ ] **Step 6: Compile and run the jobmaster execution tests**

Run: `./gradlew :jobmaster:test --tests "io.fleak.grid.jobmaster.service.task.execution.*"`
Expected: `BUILD SUCCESSFUL`; existing job/task tests still pass.

- [ ] **Step 7: Commit**

```bash
git add jobmaster/src/main/java/io/fleak/grid/jobmaster/service/task/execution/JobService.java \
        jobmaster/src/test/java/io/fleak/grid/jobmaster/service/task/execution/JobServiceReplicaInjectionTest.java
git commit -m "feat(jobmaster): inject REPLICA_INDEX/REPLICA_COUNT per replica at fan-out"
```

---

## Task 8: Full builds and final verification (both repos)

**Files:** none (verification only).

- [ ] **Step 1: zephflow-core full lib + api build**

Run (in `/Users/dan/fleak/zephflow-core`): `./gradlew :api:build :lib:build`
Expected: `BUILD SUCCESSFUL`. If spotless fails, run `./gradlew :lib:spotlessApply :api:spotlessApply`, re-inspect the diff, and amend the relevant commit.

- [ ] **Step 2: grid full build of touched modules**

Run (in `/Users/dan/fleak/grid`): `./gradlew :lib:build :workeragent:build :jobmaster:build`
Expected: `BUILD SUCCESSFUL`. If spotless fails, run `./gradlew :lib:spotlessApply :workeragent:spotlessApply :jobmaster:spotlessApply` and amend.

- [ ] **Step 3: Confirm key-literal parity across repos**

Run (in `/Users/dan/fleak/zephflow-core`): `grep -rn 'REPLICA_INDEX\|REPLICA_COUNT' api/src/main/java/io/fleak/zephflow/api/JobContext.java`
Run (in `/Users/dan/fleak/grid`): `grep -rn 'REPLICA_INDEX\|REPLICA_COUNT' jobmaster/src/main/java/io/fleak/grid/jobmaster/service/task/execution/JobService.java`
Expected: both sides use the byte-identical literals `"REPLICA_INDEX"` and `"REPLICA_COUNT"`.

- [ ] **Step 4: Review the two-repo change sets**

Run (in each repo): `git log --oneline -6` and `git diff --stat main...HEAD` (grid: compare against its base branch).
Expected: zephflow-core has Tasks 1–4; grid has Tasks 5–7. No stray files, no uncommitted changes.

---

## Self-Review

**1. Spec coverage:**
- Contract (two keys) → Task 1. ✓
- Grid injects at fan-out, only when replicas>1 → Task 7. ✓
- Shared `DagJobContextInjector`, two principled call sites → Tasks 5, 6. ✓
- No schema/DTO/migration changes → confirmed; only `JobService`/`TaskSupervisor` bodies and a new `:lib` util. ✓
- `Partitioner.owns` with sha256 + floorMod, urn key → Task 2. ✓
- Shard filter in `execute()` stream → Task 4 (step 5). ✓
- Replica-aware `SourceIdHasher`, back-compatible at count≤1 → Task 3. ✓
- Defaults when keys missing/unparseable (0/1) → Task 4 (`parseIntProperty`). ✓
- Testing: PartitionerTest, SourceIdHasherTest, FsSourceCommand sharding, DagJobContextInjectorTest, JobService injection, TaskSupervisor regression → Tasks 2,3,4,5,6,7. ✓
- Edge cases (negative hash, count change, coverage) → PartitionerTest + SourceIdHasherTest. ✓

**2. Placeholder scan:** No TBD/TODO/"handle edge cases"/"similar to Task N". All code steps contain full code. The only deliberate `<...>` is the instruction to copy the license header verbatim from a named existing file (cannot be reproduced literally here without bloating every file; the source path is exact). ✓

**3. Type consistency:**
- `Partitioner.owns(String, int, int)` — defined Task 2, consumed Task 4. ✓
- `SourceIdHasher.compute(String, String, String, int, int)` — defined Task 3, consumed Task 4. ✓
- `FsSourceExecutionContext.replicaIndex/replicaCount` (int) — defined Task 4 step 3, used step 4/5. ✓
- `DagJobContextInjector.putOtherProperty/putMetricTag(JsonNode, String, String)` — defined Task 5, consumed Tasks 6, 7. ✓
- `JobService.injectReplicaInfo(JsonNode, int, int)` — defined Task 7 step 3, called step 5. ✓
- Key literals `REPLICA_INDEX`/`REPLICA_COUNT` consistent across Tasks 1, 4, 7. ✓
