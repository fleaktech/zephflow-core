# File-System Source Framework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a unified `fs_source` command in zephflow-core covering local FS, S3, and GCS — with deterministic self-partitioning, bounded/unbounded modes, pluggable Lister/Reader backends, three emission strategies, object-store-backed checkpoints with generation migration, and removal of the legacy `gcs_source`.

**Architecture:** `FsSourceCommand` extends `SourceCommand` and owns its own poll loop. Backends provide `FileLister`/`FileReader` only; framework owns partitioning, ordering, stability, watermarks, emission, checkpoint persistence, post-actions. Partitioning is deterministic (`floorMod(murmur3_128(urn), N) == jobIndex`) so no central coordinator is needed. Checkpoint store is by default the same `FsBackend` the source reads from.

**Tech Stack:** Java 21 records, Guava (Murmur3, ImmutableMap), Lombok, JUnit 5, AWS SDK v2 (S3), `google-cloud-storage` (GCS), Testcontainers (LocalStack for S3, gcloud emulator for GCS). New code lives in `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/`.

**Spec:** [`docs/superpowers/specs/2026-05-29-fs-source-framework-design.md`](../specs/2026-05-29-fs-source-framework-design.md)

**Phase boundaries** (commit-stable points for pause/resume):
- **Phase 1 (Tasks 1–8): Foundation.** Data types, SPI interfaces, partition + sourceId utilities, in-memory checkpoint store, FsBackendRegistry. All testable in isolation. No command yet.
- **Phase 2 (Tasks 9–13): Local FS backend + emission strategies + stability + post-actions.** Building blocks for the loop.
- **Phase 3 (Tasks 14–18): `FsSourceCommand` and loop.** Bounded then unbounded; registration in `OperatorCommandRegistry`. End of phase: working `fs_source` for local files with in-memory checkpoint.
- **Phase 4 (Tasks 19–21): Object-store checkpoint + generation migration.** Distributed checkpointing.
- **Phase 5 (Tasks 22–24): S3 backend + GCS backend + cross-job determinism test.** Cloud reach.
- **Phase 6 (Tasks 25–27): Bounded-memory streaming test, `gcs_source` parity test, removal of `gcs_source`.** Ship gate.

**Conventions used throughout:**
- All test classes use JUnit 5 (`org.junit.jupiter.api.Test`).
- Integration tests are tagged `@Tag("integration")`.
- Build via Gradle. Run unit tests for the `lib` module: `./gradlew :lib:test`. Run a single test: `./gradlew :lib:test --tests "FQCN"`.
- Lombok is on the classpath; use `@Data`, `@Builder`, `@RequiredArgsConstructor` consistently with the rest of the codebase.
- Commit after each task. Commit messages use the prefix `feat(fs-source):` for new code, `test(fs-source):` for test-only additions, `refactor(fs-source):` for restructuring, `chore:` for deletions / dependency edits.

---

## Phase 1 — Foundation

### Task 1: Data types — `FileKey`, `FileEntry`, `ListRequest`

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/FileKey.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/FileEntry.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/ListRequest.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/api/FileKeyTest.java`

- [ ] **Step 1: Write the failing test**

```java
// FileKeyTest.java
package io.fleak.zephflow.lib.commands.fssource.api;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class FileKeyTest {

  @Test
  void of_parsesScheme() {
    FileKey k = FileKey.of("s3://bucket/path/file.json");
    assertEquals("s3", k.backend());
    assertEquals("s3://bucket/path/file.json", k.urn());
  }

  @Test
  void of_localFileScheme() {
    FileKey k = FileKey.of("file:///abs/path/x.csv");
    assertEquals("file", k.backend());
  }

  @Test
  void of_gcsScheme() {
    FileKey k = FileKey.of("gs://bucket/k");
    assertEquals("gs", k.backend());
  }

  @Test
  void of_rejectsMissingScheme() {
    assertThrows(IllegalArgumentException.class, () -> FileKey.of("/abs/path"));
  }
}
```

- [ ] **Step 2: Run test, verify it fails**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.api.FileKeyTest"`
Expected: compilation failure (`FileKey` does not exist).

- [ ] **Step 3: Implement `FileKey`**

```java
// FileKey.java
package io.fleak.zephflow.lib.commands.fssource.api;

import java.util.Objects;

/** Stable identifier for a file across listings. {@code backend} matches the {@code FsBackend.scheme()}. */
public record FileKey(String backend, String urn) {

  public FileKey {
    Objects.requireNonNull(backend, "backend");
    Objects.requireNonNull(urn, "urn");
  }

  /** Parse a URN like {@code s3://bucket/key} or {@code file:///abs/path}. */
  public static FileKey of(String urn) {
    int idx = urn.indexOf("://");
    if (idx <= 0) {
      throw new IllegalArgumentException("URN missing scheme: " + urn);
    }
    return new FileKey(urn.substring(0, idx), urn);
  }
}
```

- [ ] **Step 4: Implement `FileEntry` and `ListRequest`**

```java
// FileEntry.java
package io.fleak.zephflow.lib.commands.fssource.api;

import java.time.Instant;

/** Metadata about a single file produced by {@link FileLister}. */
public record FileEntry(FileKey key, long size, Instant lastModified, String displayPath) {}
```

```java
// ListRequest.java
package io.fleak.zephflow.lib.commands.fssource.api;

import java.util.regex.Pattern;

/** Inputs to {@link FileLister#list}. {@code regex}, if non-null, is applied client-side to filenames. */
public record ListRequest(String root, Pattern fileNameRegex) {}
```

- [ ] **Step 5: Run test, verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.api.FileKeyTest"`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/{FileKey,FileEntry,ListRequest}.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/api/FileKeyTest.java
git commit -m "feat(fs-source): foundation data types (FileKey, FileEntry, ListRequest)"
```

---

### Task 2: SPI interfaces — `FileLister`, `FileReader`

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/FileLister.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/FileReader.java`

No tests on this task — these are pure interfaces; their first implementation in Task 9 brings tests.

- [ ] **Step 1: Implement `FileLister`**

```java
// FileLister.java
package io.fleak.zephflow.lib.commands.fssource.api;

import java.util.stream.Stream;

/**
 * Per-backend listing + stat. Implementations MUST return a lazy stream that paginates
 * under the hood — the framework relies on this to avoid materializing entire buckets/directories.
 */
public interface FileLister extends AutoCloseable {

  /** Lazy, paginated iteration over all matching files under the given root. */
  Stream<FileEntry> list(ListRequest req);

  /** Re-stat a single file. Used by {@link StabilityProbe}. */
  FileEntry stat(FileKey key);

  @Override
  default void close() {}
}
```

- [ ] **Step 2: Implement `FileReader`**

```java
// FileReader.java
package io.fleak.zephflow.lib.commands.fssource.api;

import java.io.InputStream;

/**
 * Per-backend streaming reader. The returned InputStream is the caller's responsibility to close.
 * {@code offset > 0} means range-read starting at that byte; backends MAY ignore it if they
 * do not advertise {@link FsBackend.Capability#RANGE_READ}.
 */
public interface FileReader extends AutoCloseable {

  InputStream open(FileKey key, long offset);

  @Override
  default void close() {}
}
```

- [ ] **Step 3: Verify compilation**

Run: `./gradlew :lib:compileJava`
Expected: success.

- [ ] **Step 4: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/{FileLister,FileReader}.java
git commit -m "feat(fs-source): FileLister and FileReader SPIs"
```

---

### Task 3: SPI interfaces — `EmissionStrategy`, `StabilityProbe`, `PostAction`

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/EmissionStrategy.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/StabilityProbe.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/PostAction.java`

- [ ] **Step 1: Implement `EmissionStrategy`**

```java
// EmissionStrategy.java
package io.fleak.zephflow.lib.commands.fssource.api;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;

/**
 * Converts a file into one or more {@code RecordFleakData} events on {@code out}.
 * MUST be bounded-memory for the line and file-reference strategies — never load the entire
 * file into heap. WholeFileEmissionStrategy is the documented exception.
 */
public interface EmissionStrategy {
  void emit(FileEntry file, FileReader reader, SourceEventAcceptor out, JobContext ctx)
      throws Exception;
}
```

- [ ] **Step 2: Implement `StabilityProbe`**

```java
// StabilityProbe.java
package io.fleak.zephflow.lib.commands.fssource.api;

/**
 * Returns true if a file is safe to emit. Default behavior (SizeStableProbe) compares
 * (size, lastModified) across two probes separated by {@code probeDelay}.
 */
public interface StabilityProbe {

  boolean isStable(FileEntry file, FileLister lister);

  /** Always-stable probe used when stability is disabled. */
  StabilityProbe ALWAYS_STABLE = (file, lister) -> true;
}
```

- [ ] **Step 3: Implement `PostAction`**

```java
// PostAction.java
package io.fleak.zephflow.lib.commands.fssource.api;

/** Runs after a file is fully emitted AND committed. May throw to fail the loop. */
public interface PostAction {

  void run(FileEntry file, FsBackend backend) throws Exception;

  PostAction NO_OP = (file, backend) -> {};
}
```

- [ ] **Step 4: Verify compilation**

Run: `./gradlew :lib:compileJava`
Expected: success — note `FsBackend` is referenced but not yet declared. If compilation fails on that, defer adding the import in `PostAction.java` (use fully qualified name) and continue; Task 4 will introduce `FsBackend`.

Actually — to keep this task self-contained, replace the `PostAction.java` body above with the placeholder shape below and revisit in Task 4:

```java
// PostAction.java  (PROVISIONAL — refined in Task 4)
package io.fleak.zephflow.lib.commands.fssource.api;

public interface PostAction {

  void run(FileEntry file) throws Exception;

  PostAction NO_OP = file -> {};
}
```

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/{EmissionStrategy,StabilityProbe,PostAction}.java
git commit -m "feat(fs-source): EmissionStrategy, StabilityProbe, PostAction SPIs"
```

---

### Task 4: `FsBackend` + `FsBackendRegistry`

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/FsBackendConfig.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/FsBackend.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/FsBackendRegistry.java`
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/PostAction.java` (refine to take backend)
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/api/FsBackendRegistryTest.java`

- [ ] **Step 1: Write the failing registry test**

```java
// FsBackendRegistryTest.java
package io.fleak.zephflow.lib.commands.fssource.api;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class FsBackendRegistryTest {

  private static class FakeBackend implements FsBackend {
    @Override public String scheme() { return "fake"; }
    @Override public FileLister createLister(FsBackendConfig cfg) {
      return new FileLister() {
        @Override public Stream<FileEntry> list(ListRequest r) { return Stream.empty(); }
        @Override public FileEntry stat(FileKey k) { throw new UnsupportedOperationException(); }
      };
    }
    @Override public FileReader createReader(FsBackendConfig cfg) {
      return (k, offset) -> { throw new UnsupportedOperationException(); };
    }
    @Override public Set<Capability> capabilities() { return Set.of(); }
  }

  @AfterEach
  void cleanup() {
    FsBackendRegistry.unregister("fake");
  }

  @Test
  void registerAndGet() {
    FakeBackend b = new FakeBackend();
    FsBackendRegistry.register(b);
    assertSame(b, FsBackendRegistry.get("fake"));
  }

  @Test
  void getUnknown_throws() {
    assertThrows(IllegalArgumentException.class, () -> FsBackendRegistry.get("nope"));
  }

  @Test
  void duplicateRegister_throws() {
    FsBackendRegistry.register(new FakeBackend());
    assertThrows(IllegalStateException.class, () -> FsBackendRegistry.register(new FakeBackend()));
  }
}
```

- [ ] **Step 2: Implement `FsBackendConfig` (marker interface)**

```java
// FsBackendConfig.java
package io.fleak.zephflow.lib.commands.fssource.api;

/** Typed per-backend config (e.g. S3BackendConfig, GcsBackendConfig, LocalFsBackendConfig). */
public interface FsBackendConfig {}
```

- [ ] **Step 3: Implement `FsBackend`**

```java
// FsBackend.java
package io.fleak.zephflow.lib.commands.fssource.api;

import java.util.Set;

public interface FsBackend {

  String scheme();

  FileLister createLister(FsBackendConfig cfg);

  FileReader createReader(FsBackendConfig cfg);

  Set<Capability> capabilities();

  enum Capability {
    DELETE,
    MOVE,
    RANGE_READ
  }
}
```

- [ ] **Step 4: Implement `FsBackendRegistry`**

```java
// FsBackendRegistry.java
package io.fleak.zephflow.lib.commands.fssource.api;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Static, mutable registry. v1 backends register themselves via a static initializer. */
public final class FsBackendRegistry {

  private static final ConcurrentMap<String, FsBackend> BACKENDS = new ConcurrentHashMap<>();

  private FsBackendRegistry() {}

  public static void register(FsBackend backend) {
    FsBackend prior = BACKENDS.putIfAbsent(backend.scheme(), backend);
    if (prior != null) {
      throw new IllegalStateException("Backend already registered for scheme: " + backend.scheme());
    }
  }

  public static FsBackend get(String scheme) {
    FsBackend b = BACKENDS.get(scheme);
    if (b == null) {
      throw new IllegalArgumentException("No FsBackend registered for scheme: " + scheme);
    }
    return b;
  }

  /** Test-only. */
  public static void unregister(String scheme) {
    BACKENDS.remove(scheme);
  }
}
```

- [ ] **Step 5: Refine `PostAction.java` to take the backend**

Replace the entire file with:

```java
// PostAction.java
package io.fleak.zephflow.lib.commands.fssource.api;

public interface PostAction {

  void run(FileEntry file, FsBackend backend) throws Exception;

  PostAction NO_OP = (file, backend) -> {};
}
```

- [ ] **Step 6: Run test, verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.api.FsBackendRegistryTest"`
Expected: PASS (3 tests).

- [ ] **Step 7: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/{FsBackend,FsBackendConfig,FsBackendRegistry,PostAction}.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/api/FsBackendRegistryTest.java
git commit -m "feat(fs-source): FsBackend SPI and registry"
```

---

### Task 5: `Partitioner` utility

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/util/Partitioner.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/util/PartitionerTest.java`

- [ ] **Step 1: Write the failing test**

```java
// PartitionerTest.java
package io.fleak.zephflow.lib.commands.fssource.util;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PartitionerTest {

  @Test
  void deterministic() {
    int a = Partitioner.assignedJob("s3://bucket/file-001.json", 4);
    int b = Partitioner.assignedJob("s3://bucket/file-001.json", 4);
    assertEquals(a, b);
  }

  @Test
  void inRange() {
    for (int i = 0; i < 1000; i++) {
      int slot = Partitioner.assignedJob("s3://bucket/file-" + i, 8);
      assertTrue(slot >= 0 && slot < 8, "slot=" + slot);
    }
  }

  @Test
  void coversAllSlotsAndIsDisjoint() {
    int n = 4;
    Set<String> all = new HashSet<>();
    Set<String>[] slots = new Set[n];
    for (int i = 0; i < n; i++) slots[i] = new HashSet<>();
    for (int i = 0; i < 1000; i++) {
      String urn = "s3://bkt/f-" + i;
      all.add(urn);
      slots[Partitioner.assignedJob(urn, n)].add(urn);
    }
    Set<String> union = new HashSet<>();
    for (Set<String> s : slots) {
      for (String u : s) {
        assertTrue(union.add(u), "Duplicate across slots: " + u);
      }
    }
    assertEquals(all, union);
  }

  @Test
  void singleJobOwnsEverything() {
    for (int i = 0; i < 100; i++) {
      assertEquals(0, Partitioner.assignedJob("any-urn-" + i, 1));
    }
  }

  @Test
  void rejectsZeroParallelism() {
    assertThrows(IllegalArgumentException.class, () -> Partitioner.assignedJob("x", 0));
  }
}
```

- [ ] **Step 2: Run test, verify it fails**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.util.PartitionerTest"`
Expected: compilation failure.

- [ ] **Step 3: Implement `Partitioner`**

```java
// Partitioner.java
package io.fleak.zephflow.lib.commands.fssource.util;

import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;

public final class Partitioner {

  private Partitioner() {}

  /** Deterministic, uniform partition assignment. Returns slot in [0, parallelism). */
  public static int assignedJob(String urn, int parallelism) {
    if (parallelism <= 0) {
      throw new IllegalArgumentException("parallelism must be > 0, got " + parallelism);
    }
    long h = Hashing.murmur3_128().hashString(urn, StandardCharsets.UTF_8).asLong();
    return Math.floorMod(h, parallelism);
  }
}
```

- [ ] **Step 4: Run test, verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.util.PartitionerTest"`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/util/Partitioner.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/util/PartitionerTest.java
git commit -m "feat(fs-source): deterministic Partitioner utility"
```

---

### Task 6: `SourceIdHasher` utility

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/util/SourceIdHasher.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/util/SourceIdHasherTest.java`

- [ ] **Step 1: Write the failing test**

```java
// SourceIdHasherTest.java
package io.fleak.zephflow.lib.commands.fssource.util;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class SourceIdHasherTest {

  @Test
  void stableAcrossCalls() {
    String a = SourceIdHasher.compute("s3", "s3://bkt/data/", "invoice_(?<ts>\\d+)\\.json");
    String b = SourceIdHasher.compute("s3", "s3://bkt/data/", "invoice_(?<ts>\\d+)\\.json");
    assertEquals(a, b);
  }

  @Test
  void differentRootsDiffer() {
    String a = SourceIdHasher.compute("s3", "s3://bkt/data1/", null);
    String b = SourceIdHasher.compute("s3", "s3://bkt/data2/", null);
    assertNotEquals(a, b);
  }

  @Test
  void length16Hex() {
    String a = SourceIdHasher.compute("file", "/tmp/x", null);
    assertEquals(16, a.length());
    assertTrue(a.matches("[0-9a-f]{16}"));
  }

  @Test
  void nullRegexAllowed() {
    assertDoesNotThrow(() -> SourceIdHasher.compute("file", "/tmp/x", null));
  }
}
```

- [ ] **Step 2: Implement `SourceIdHasher`**

```java
// SourceIdHasher.java
package io.fleak.zephflow.lib.commands.fssource.util;

import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;

public final class SourceIdHasher {

  private SourceIdHasher() {}

  /**
   * Stable 16-hex-char id derived from the source-identity fields. Two configs with the
   * same {@code (backend, root, fileNameRegex)} share a sourceId — and therefore share checkpoints.
   * Other fields (mode, partition, emission, listingInterval, checkpoint override) do NOT participate.
   */
  public static String compute(String backend, String root, String fileNameRegex) {
    String canonical = backend + "\n" + root + "\n" + (fileNameRegex == null ? "" : fileNameRegex);
    return Hashing.sha256()
        .hashString(canonical, StandardCharsets.UTF_8)
        .toString()
        .substring(0, 16);
  }
}
```

- [ ] **Step 3: Run test, verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.util.SourceIdHasherTest"`
Expected: PASS (4 tests).

- [ ] **Step 4: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/util/SourceIdHasher.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/util/SourceIdHasherTest.java
git commit -m "feat(fs-source): SourceIdHasher utility"
```

---

### Task 7: Checkpoint SPI — `FsCheckpoint` + `CheckpointStore`

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/checkpoint/FsCheckpoint.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/checkpoint/CheckpointStore.java`

No test on the SPI itself; Task 8 brings the in-memory implementation with tests.

- [ ] **Step 1: Implement `FsCheckpoint`**

```java
// FsCheckpoint.java
package io.fleak.zephflow.lib.commands.fssource.checkpoint;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public record FsCheckpoint(
    int version, Instant watermark, Set<String> completedSinceWatermark) {

  public FsCheckpoint {
    completedSinceWatermark = Set.copyOf(completedSinceWatermark);
  }

  public static FsCheckpoint empty() {
    return new FsCheckpoint(1, Instant.EPOCH, Collections.emptySet());
  }

  public FsCheckpoint withCompleted(String urn) {
    Set<String> next = new HashSet<>(completedSinceWatermark);
    next.add(urn);
    return new FsCheckpoint(version, watermark, next);
  }

  /** Advance watermark to {@code newWatermark} and drop any completed entries strictly below it.
   *  The pruning by-timestamp is performed by the caller, which holds the ts->urn mapping. */
  public FsCheckpoint withWatermark(Instant newWatermark, Set<String> retainedCompleted) {
    return new FsCheckpoint(version, newWatermark, retainedCompleted);
  }
}
```

- [ ] **Step 2: Implement `CheckpointStore`**

```java
// CheckpointStore.java
package io.fleak.zephflow.lib.commands.fssource.checkpoint;

import java.util.List;
import java.util.Optional;

public interface CheckpointStore extends AutoCloseable {

  Optional<FsCheckpoint> load(String checkpointKey);

  /** Atomic single-writer PUT. Implementations MUST ensure readers never see a torn write. */
  void save(String checkpointKey, FsCheckpoint cp);

  /** List the generations (parallelism values) for which any shard exists under {@code sourceId}. */
  List<Integer> listGenerations(String sourceId);

  /** List the shard keys ({@code <sourceId>/<generation>/<jobIndex>.json}) for a given generation. */
  List<String> listShards(String sourceId, int generation);

  @Override
  default void close() {}
}
```

- [ ] **Step 3: Verify compilation**

Run: `./gradlew :lib:compileJava`
Expected: success.

- [ ] **Step 4: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/checkpoint/{FsCheckpoint,CheckpointStore}.java
git commit -m "feat(fs-source): CheckpointStore SPI and FsCheckpoint type"
```

---

### Task 8: `InMemoryCheckpointStore`

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/checkpoint/InMemoryCheckpointStore.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/checkpoint/InMemoryCheckpointStoreTest.java`

- [ ] **Step 1: Write the failing test**

```java
// InMemoryCheckpointStoreTest.java
package io.fleak.zephflow.lib.commands.fssource.checkpoint;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.Set;
import org.junit.jupiter.api.Test;

class InMemoryCheckpointStoreTest {

  @Test
  void loadEmptyWhenAbsent() {
    InMemoryCheckpointStore s = new InMemoryCheckpointStore();
    assertTrue(s.load("abc/3/0.json").isEmpty());
  }

  @Test
  void saveThenLoad() {
    InMemoryCheckpointStore s = new InMemoryCheckpointStore();
    FsCheckpoint cp = new FsCheckpoint(1, Instant.parse("2026-01-01T00:00:00Z"), Set.of("u1"));
    s.save("abc/3/0.json", cp);
    assertEquals(cp, s.load("abc/3/0.json").orElseThrow());
  }

  @Test
  void listGenerationsAndShards() {
    InMemoryCheckpointStore s = new InMemoryCheckpointStore();
    s.save("abc/3/0.json", FsCheckpoint.empty());
    s.save("abc/3/1.json", FsCheckpoint.empty());
    s.save("abc/3/2.json", FsCheckpoint.empty());
    s.save("abc/5/4.json", FsCheckpoint.empty());

    assertEquals(java.util.List.of(3, 5), s.listGenerations("abc").stream().sorted().toList());
    assertEquals(3, s.listShards("abc", 3).size());
    assertEquals(1, s.listShards("abc", 5).size());
  }
}
```

- [ ] **Step 2: Implement `InMemoryCheckpointStore`**

```java
// InMemoryCheckpointStore.java
package io.fleak.zephflow.lib.commands.fssource.checkpoint;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Test/dev checkpoint store. Not durable. Thread-safe via concurrent map. */
public final class InMemoryCheckpointStore implements CheckpointStore {

  private final ConcurrentMap<String, FsCheckpoint> store = new ConcurrentHashMap<>();

  @Override
  public Optional<FsCheckpoint> load(String checkpointKey) {
    return Optional.ofNullable(store.get(checkpointKey));
  }

  @Override
  public void save(String checkpointKey, FsCheckpoint cp) {
    store.put(checkpointKey, cp);
  }

  @Override
  public List<Integer> listGenerations(String sourceId) {
    Set<Integer> gens = new HashSet<>();
    String prefix = sourceId + "/";
    for (String key : store.keySet()) {
      if (!key.startsWith(prefix)) continue;
      String rest = key.substring(prefix.length());
      int slash = rest.indexOf('/');
      if (slash <= 0) continue;
      try {
        gens.add(Integer.parseInt(rest.substring(0, slash)));
      } catch (NumberFormatException ignored) {
      }
    }
    return new ArrayList<>(gens);
  }

  @Override
  public List<String> listShards(String sourceId, int generation) {
    String prefix = sourceId + "/" + generation + "/";
    List<String> out = new ArrayList<>();
    for (Map.Entry<String, FsCheckpoint> e : store.entrySet()) {
      if (e.getKey().startsWith(prefix)) out.add(e.getKey());
    }
    return out;
  }
}
```

- [ ] **Step 3: Run test, verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.checkpoint.InMemoryCheckpointStoreTest"`
Expected: PASS (3 tests).

- [ ] **Step 4: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/checkpoint/InMemoryCheckpointStore.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/checkpoint/InMemoryCheckpointStoreTest.java
git commit -m "feat(fs-source): InMemoryCheckpointStore"
```

---

**End of Phase 1.** Foundation complete: data types, SPIs, partitioner, sourceId, registry, in-memory checkpoint store.

---

## Phase 2 — Local FS backend, emission strategies, stability, post-actions

### Task 9: `LocalFsBackend` (config + lister + reader + backend)

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/local/LocalFsBackendConfig.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/local/LocalFsLister.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/local/LocalFsReader.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/local/LocalFsBackend.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/local/LocalFsBackendTest.java`

- [ ] **Step 1: Write the failing test**

```java
// LocalFsBackendTest.java
package io.fleak.zephflow.lib.commands.fssource.backend.local;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.commands.fssource.api.FileEntry;
import io.fleak.zephflow.lib.commands.fssource.api.FileLister;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import io.fleak.zephflow.lib.commands.fssource.api.ListRequest;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LocalFsBackendTest {

  @Test
  void listsMatchingFilesAndIgnoresOthers(@TempDir Path tmp) throws IOException {
    Files.writeString(tmp.resolve("invoice_1.json"), "a");
    Files.writeString(tmp.resolve("invoice_2.json"), "bb");
    Files.writeString(tmp.resolve("other.txt"), "c");

    LocalFsBackend backend = new LocalFsBackend();
    FileLister lister =
        backend.createLister(new LocalFsBackendConfig(tmp.toString()));

    List<FileEntry> out =
        lister
            .list(new ListRequest(tmp.toString(), Pattern.compile("invoice_\\d+\\.json")))
            .toList();

    assertEquals(2, out.size());
    assertTrue(out.stream().allMatch(e -> e.key().backend().equals("file")));
    assertTrue(out.stream().anyMatch(e -> e.displayPath().endsWith("invoice_1.json")));
  }

  @Test
  void readerStreamsFile(@TempDir Path tmp) throws Exception {
    Path p = tmp.resolve("data.bin");
    Files.writeString(p, "hello-world");

    LocalFsBackend backend = new LocalFsBackend();
    FileReader reader = backend.createReader(new LocalFsBackendConfig(tmp.toString()));

    try (InputStream in = reader.open(new io.fleak.zephflow.lib.commands.fssource.api.FileKey("file", p.toUri().toString()), 0)) {
      assertEquals("hello-world", new String(in.readAllBytes()));
    }
  }

  @Test
  void statReturnsCurrentSize(@TempDir Path tmp) throws Exception {
    Path p = tmp.resolve("x");
    Files.writeString(p, "abc");
    LocalFsBackend backend = new LocalFsBackend();
    FileLister lister = backend.createLister(new LocalFsBackendConfig(tmp.toString()));
    FileEntry e = lister.stat(new io.fleak.zephflow.lib.commands.fssource.api.FileKey("file", p.toUri().toString()));
    assertEquals(3, e.size());
  }
}
```

- [ ] **Step 2: Implement `LocalFsBackendConfig`**

```java
// LocalFsBackendConfig.java
package io.fleak.zephflow.lib.commands.fssource.backend.local;

import io.fleak.zephflow.lib.commands.fssource.api.FsBackendConfig;

public record LocalFsBackendConfig(String root) implements FsBackendConfig {}
```

- [ ] **Step 3: Implement `LocalFsLister`**

```java
// LocalFsLister.java
package io.fleak.zephflow.lib.commands.fssource.backend.local;

import io.fleak.zephflow.lib.commands.fssource.api.FileEntry;
import io.fleak.zephflow.lib.commands.fssource.api.FileKey;
import io.fleak.zephflow.lib.commands.fssource.api.FileLister;
import io.fleak.zephflow.lib.commands.fssource.api.ListRequest;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public final class LocalFsLister implements FileLister {

  @Override
  public Stream<FileEntry> list(ListRequest req) {
    Path root = Paths.get(req.root());
    Pattern regex = req.fileNameRegex();
    try {
      return Files.walk(root)
          .filter(Files::isRegularFile)
          .filter(p -> regex == null || regex.matcher(p.getFileName().toString()).matches())
          .map(LocalFsLister::toEntry);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public FileEntry stat(FileKey key) {
    Path p = Paths.get(java.net.URI.create(key.urn()));
    return toEntry(p);
  }

  private static FileEntry toEntry(Path p) {
    try {
      BasicFileAttributes a = Files.readAttributes(p, BasicFileAttributes.class);
      FileKey k = new FileKey("file", p.toUri().toString());
      return new FileEntry(k, a.size(), Instant.ofEpochMilli(a.lastModifiedTime().toMillis()), p.toString());
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
```

- [ ] **Step 4: Implement `LocalFsReader`**

```java
// LocalFsReader.java
package io.fleak.zephflow.lib.commands.fssource.backend.local;

import io.fleak.zephflow.lib.commands.fssource.api.FileKey;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public final class LocalFsReader implements FileReader {

  @Override
  public InputStream open(FileKey key, long offset) {
    try {
      FileChannel ch =
          FileChannel.open(Paths.get(java.net.URI.create(key.urn())), StandardOpenOption.READ);
      if (offset > 0) ch.position(offset);
      return Channels.newInputStream(ch);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
```

- [ ] **Step 5: Implement `LocalFsBackend`**

```java
// LocalFsBackend.java
package io.fleak.zephflow.lib.commands.fssource.backend.local;

import io.fleak.zephflow.lib.commands.fssource.api.FileLister;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackend;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendConfig;
import java.util.Set;

public final class LocalFsBackend implements FsBackend {

  public static final String SCHEME = "file";

  @Override
  public String scheme() {
    return SCHEME;
  }

  @Override
  public FileLister createLister(FsBackendConfig cfg) {
    return new LocalFsLister();
  }

  @Override
  public FileReader createReader(FsBackendConfig cfg) {
    return new LocalFsReader();
  }

  @Override
  public Set<Capability> capabilities() {
    return Set.of(Capability.DELETE, Capability.MOVE, Capability.RANGE_READ);
  }
}
```

- [ ] **Step 6: Run test, verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackendTest"`
Expected: PASS (3 tests).

- [ ] **Step 7: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/local/ \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/local/
git commit -m "feat(fs-source): LocalFsBackend (lister + reader)"
```

---

### Task 10: `FileReferenceEmissionStrategy`

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/emission/FileReferenceEmissionStrategy.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/emission/FileReferenceEmissionStrategyTest.java`

- [ ] **Step 1: Write the failing test**

```java
// FileReferenceEmissionStrategyTest.java
package io.fleak.zephflow.lib.commands.fssource.emission;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.FileEntry;
import io.fleak.zephflow.lib.commands.fssource.api.FileKey;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FileReferenceEmissionStrategyTest {

  @Test
  void emitsSingleMetadataEvent() throws Exception {
    FileEntry entry =
        new FileEntry(
            new FileKey("s3", "s3://bkt/file_1700000000.json"),
            1234,
            Instant.parse("2026-01-01T00:00:00Z"),
            "s3://bkt/file_1700000000.json");

    List<RecordFleakData> emitted = new ArrayList<>();
    SourceEventAcceptor out =
        new SourceEventAcceptor() {
          @Override public void accept(List<RecordFleakData> records) { emitted.addAll(records); }
          @Override public void terminate() {}
        };

    new FileReferenceEmissionStrategy().emit(entry, null, out, JobContext.builder().build());

    assertEquals(1, emitted.size());
    Map<String, Object> payload = emitted.get(0).unwrap();
    assertEquals("s3://bkt/file_1700000000.json", payload.get("file"));
    assertEquals(1234L, ((Number) payload.get("size")).longValue());
    assertNotNull(payload.get("lastModified"));
  }
}
```

- [ ] **Step 2: Implement `FileReferenceEmissionStrategy`**

```java
// FileReferenceEmissionStrategy.java
package io.fleak.zephflow.lib.commands.fssource.emission;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.EmissionStrategy;
import io.fleak.zephflow.lib.commands.fssource.api.FileEntry;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Emits one event per file containing only metadata. Reader stream is NEVER opened. O(1) per file. */
public final class FileReferenceEmissionStrategy implements EmissionStrategy {

  @Override
  public void emit(FileEntry file, FileReader reader, SourceEventAcceptor out, JobContext ctx) {
    Map<String, Object> payload = new LinkedHashMap<>();
    payload.put("file", file.key().urn());
    payload.put("size", file.size());
    payload.put("lastModified", file.lastModified().toString());
    payload.put("displayPath", file.displayPath());
    RecordFleakData record = (RecordFleakData) FleakData.wrap(payload);
    out.accept(List.of(record));
  }
}
```

- [ ] **Step 3: Run test, verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.emission.FileReferenceEmissionStrategyTest"`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/emission/FileReferenceEmissionStrategy.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/emission/FileReferenceEmissionStrategyTest.java
git commit -m "feat(fs-source): FileReferenceEmissionStrategy"
```

---

### Task 11: `LineEmissionStrategy`

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/emission/LineEmissionStrategy.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/emission/LineEmissionStrategyTest.java`

- [ ] **Step 1: Write the failing test**

```java
// LineEmissionStrategyTest.java
package io.fleak.zephflow.lib.commands.fssource.emission;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.FileEntry;
import io.fleak.zephflow.lib.commands.fssource.api.FileKey;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class LineEmissionStrategyTest {

  private static FileReader readerOf(byte[] bytes) {
    return (k, offset) -> new ByteArrayInputStream(bytes);
  }

  @Test
  void emitsOneRecordPerLine() throws Exception {
    String body = "line-1\nline-2\nline-3";
    FileEntry e = new FileEntry(new FileKey("file", "file:///t/x"), body.length(), Instant.EPOCH, "x");

    List<RecordFleakData> out = new ArrayList<>();
    SourceEventAcceptor acc = new SourceEventAcceptor() {
      @Override public void accept(List<RecordFleakData> r) { out.addAll(r); }
      @Override public void terminate() {}
    };

    new LineEmissionStrategy(StandardCharsets.UTF_8, 100)
        .emit(e, readerOf(body.getBytes(StandardCharsets.UTF_8)), acc, JobContext.builder().build());

    assertEquals(3, out.size());
    assertEquals("line-1", out.get(0).unwrap().get("line"));
    assertEquals("line-3", out.get(2).unwrap().get("line"));
  }

  @Test
  void batchesByConfiguredSize() throws Exception {
    StringBuilder b = new StringBuilder();
    for (int i = 0; i < 250; i++) b.append("L").append(i).append("\n");
    FileEntry e = new FileEntry(new FileKey("file", "file:///t/x"), b.length(), Instant.EPOCH, "x");

    int[] batchCount = {0};
    SourceEventAcceptor acc = new SourceEventAcceptor() {
      @Override public void accept(List<RecordFleakData> r) { batchCount[0]++; }
      @Override public void terminate() {}
    };

    new LineEmissionStrategy(StandardCharsets.UTF_8, 100)
        .emit(e, readerOf(b.toString().getBytes(StandardCharsets.UTF_8)), acc, JobContext.builder().build());

    assertEquals(3, batchCount[0]); // 100 + 100 + 50
  }
}
```

- [ ] **Step 2: Implement `LineEmissionStrategy`**

```java
// LineEmissionStrategy.java
package io.fleak.zephflow.lib.commands.fssource.emission;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.EmissionStrategy;
import io.fleak.zephflow.lib.commands.fssource.api.FileEntry;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Emits one {@code RecordFleakData} per line. Bounded memory: O(batchSize × maxLineLen). */
public final class LineEmissionStrategy implements EmissionStrategy {

  private final Charset charset;
  private final int batchSize;

  public LineEmissionStrategy(Charset charset, int batchSize) {
    if (batchSize <= 0) throw new IllegalArgumentException("batchSize must be > 0");
    this.charset = charset;
    this.batchSize = batchSize;
  }

  @Override
  public void emit(FileEntry file, FileReader reader, SourceEventAcceptor out, JobContext ctx)
      throws Exception {
    try (InputStream in = reader.open(file.key(), 0);
         BufferedReader br = new BufferedReader(new InputStreamReader(in, charset))) {
      List<RecordFleakData> batch = new ArrayList<>(batchSize);
      String line;
      while ((line = br.readLine()) != null) {
        batch.add((RecordFleakData) FleakData.wrap(Map.of("line", line, "file", file.key().urn())));
        if (batch.size() >= batchSize) {
          out.accept(batch);
          batch = new ArrayList<>(batchSize);
        }
      }
      if (!batch.isEmpty()) out.accept(batch);
    }
  }
}
```

- [ ] **Step 3: Run test, verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.emission.LineEmissionStrategyTest"`
Expected: PASS (2 tests).

- [ ] **Step 4: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/emission/LineEmissionStrategy.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/emission/LineEmissionStrategyTest.java
git commit -m "feat(fs-source): LineEmissionStrategy"
```

---

### Task 12: `WholeFileEmissionStrategy`

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/emission/WholeFileEmissionStrategy.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/emission/WholeFileEmissionStrategyTest.java`

- [ ] **Step 1: Write the failing test**

```java
// WholeFileEmissionStrategyTest.java
package io.fleak.zephflow.lib.commands.fssource.emission;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.FileEntry;
import io.fleak.zephflow.lib.commands.fssource.api.FileKey;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class WholeFileEmissionStrategyTest {

  @Test
  void emitsOneRecordWithFullContent() throws Exception {
    byte[] body = "hello world".getBytes(StandardCharsets.UTF_8);
    FileReader reader = (k, offset) -> new ByteArrayInputStream(body);
    FileEntry e = new FileEntry(new FileKey("file", "file:///t/x"), body.length, Instant.EPOCH, "x");

    List<RecordFleakData> out = new ArrayList<>();
    SourceEventAcceptor acc = new SourceEventAcceptor() {
      @Override public void accept(List<RecordFleakData> r) { out.addAll(r); }
      @Override public void terminate() {}
    };

    new WholeFileEmissionStrategy(StandardCharsets.UTF_8).emit(e, reader, acc, JobContext.builder().build());

    assertEquals(1, out.size());
    assertEquals("hello world", out.get(0).unwrap().get("content"));
    assertEquals("file:///t/x", out.get(0).unwrap().get("file"));
  }
}
```

- [ ] **Step 2: Implement `WholeFileEmissionStrategy`**

```java
// WholeFileEmissionStrategy.java
package io.fleak.zephflow.lib.commands.fssource.emission;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.EmissionStrategy;
import io.fleak.zephflow.lib.commands.fssource.api.FileEntry;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/** Emits one record per file with the full decoded content. O(fileSize) heap — use carefully. */
public final class WholeFileEmissionStrategy implements EmissionStrategy {

  private final Charset charset;

  public WholeFileEmissionStrategy(Charset charset) {
    this.charset = charset;
  }

  @Override
  public void emit(FileEntry file, FileReader reader, SourceEventAcceptor out, JobContext ctx)
      throws Exception {
    try (InputStream in = reader.open(file.key(), 0)) {
      String content = new String(in.readAllBytes(), charset);
      RecordFleakData record =
          (RecordFleakData) FleakData.wrap(Map.of("file", file.key().urn(), "content", content));
      out.accept(List.of(record));
    }
  }
}
```

- [ ] **Step 3: Run test, verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.emission.WholeFileEmissionStrategyTest"`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/emission/WholeFileEmissionStrategy.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/emission/WholeFileEmissionStrategyTest.java
git commit -m "feat(fs-source): WholeFileEmissionStrategy"
```

---

### Task 13: Stability probe + post-actions

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/SizeStableProbe.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/PostActions.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/api/SizeStableProbeTest.java`

- [ ] **Step 1: Write the failing test**

```java
// SizeStableProbeTest.java
package io.fleak.zephflow.lib.commands.fssource.api;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

class SizeStableProbeTest {

  private static FileLister listerWith(Map<FileKey, FileEntry> table) {
    return new FileLister() {
      @Override public Stream<FileEntry> list(ListRequest r) { return table.values().stream(); }
      @Override public FileEntry stat(FileKey k) { return table.get(k); }
    };
  }

  @Test
  void firstSightingNotStable() {
    SizeStableProbe probe = new SizeStableProbe(Duration.ZERO);
    FileEntry e = new FileEntry(new FileKey("file", "u"), 10, Instant.EPOCH, "u");
    Map<FileKey, FileEntry> tbl = new HashMap<>();
    tbl.put(e.key(), e);
    assertFalse(probe.isStable(e, listerWith(tbl)));
  }

  @Test
  void secondProbeWithUnchangedSizeIsStable() {
    SizeStableProbe probe = new SizeStableProbe(Duration.ZERO);
    FileKey k = new FileKey("file", "u");
    FileEntry e = new FileEntry(k, 10, Instant.EPOCH, "u");
    Map<FileKey, FileEntry> tbl = new HashMap<>();
    tbl.put(k, e);
    assertFalse(probe.isStable(e, listerWith(tbl)));
    assertTrue(probe.isStable(e, listerWith(tbl)));
  }

  @Test
  void sizeChangedIsNotStable() {
    SizeStableProbe probe = new SizeStableProbe(Duration.ZERO);
    FileKey k = new FileKey("file", "u");
    FileEntry first = new FileEntry(k, 10, Instant.EPOCH, "u");
    FileEntry grown = new FileEntry(k, 20, Instant.EPOCH, "u");
    Map<FileKey, FileEntry> tbl = new HashMap<>();
    tbl.put(k, first);
    assertFalse(probe.isStable(first, listerWith(tbl)));
    tbl.put(k, grown);
    assertFalse(probe.isStable(grown, listerWith(tbl)));
  }
}
```

- [ ] **Step 2: Implement `SizeStableProbe`**

```java
// SizeStableProbe.java
package io.fleak.zephflow.lib.commands.fssource.api;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/** Default stability probe. Sees a file twice — if size and lastModified are unchanged, it is stable. */
public final class SizeStableProbe implements StabilityProbe {

  private final Duration probeDelay;
  private final Map<FileKey, ProbeState> seen = new HashMap<>();

  public SizeStableProbe(Duration probeDelay) {
    this.probeDelay = probeDelay;
  }

  @Override
  public boolean isStable(FileEntry file, FileLister lister) {
    ProbeState prior = seen.get(file.key());
    Instant now = Instant.now();
    if (prior == null) {
      seen.put(file.key(), new ProbeState(file.size(), file.lastModified(), now));
      return false;
    }
    if (Duration.between(prior.firstSeenAt, now).compareTo(probeDelay) < 0) {
      return false;
    }
    FileEntry current = lister.stat(file.key());
    if (current.size() == prior.size && current.lastModified().equals(prior.lastModified)) {
      seen.remove(file.key());
      return true;
    }
    seen.put(file.key(), new ProbeState(current.size(), current.lastModified(), now));
    return false;
  }

  private record ProbeState(long size, Instant lastModified, Instant firstSeenAt) {}
}
```

- [ ] **Step 3: Implement `PostActions`**

```java
// PostActions.java
package io.fleak.zephflow.lib.commands.fssource.api;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/** Built-in {@link PostAction} factories. */
public final class PostActions {

  private PostActions() {}

  public static PostAction noOp() {
    return PostAction.NO_OP;
  }

  /** Delete via the backend. Throws if the backend does not advertise DELETE. */
  public static PostAction delete() {
    return (file, backend) -> {
      if (!backend.capabilities().contains(FsBackend.Capability.DELETE)) {
        throw new UnsupportedOperationException(
            "Backend " + backend.scheme() + " does not support DELETE");
      }
      switch (file.key().backend()) {
        case "file" -> Files.delete(Paths.get(java.net.URI.create(file.key().urn())));
        default -> throw new UnsupportedOperationException(
            "Delete not implemented for backend " + file.key().backend() + " in v1");
      }
    };
  }

  /** Move to a sibling prefix. Behavior depends on backend; local FS is implemented here. */
  public static PostAction moveTo(String destinationPrefix) {
    return (file, backend) -> {
      if (!backend.capabilities().contains(FsBackend.Capability.MOVE)) {
        throw new UnsupportedOperationException(
            "Backend " + backend.scheme() + " does not support MOVE");
      }
      if (!"file".equals(file.key().backend())) {
        throw new UnsupportedOperationException(
            "MoveTo not implemented for backend " + file.key().backend() + " in v1");
      }
      Path src = Paths.get(java.net.URI.create(file.key().urn()));
      Path dst =
          Paths.get(java.net.URI.create(destinationPrefix + "/" + src.getFileName().toString()));
      Files.createDirectories(dst.getParent());
      Files.move(src, dst, StandardCopyOption.ATOMIC_MOVE);
    };
  }
}
```

Note: S3 and GCS delete/move implementations are folded into the cloud backends in Tasks 22 and 23 by extending `PostActions` with branches on `file.key().backend()`. v1 wires per-backend delete/move when each backend lands; for now only `file` is implemented and other backends throw an explicit `UnsupportedOperationException`. Tasks 22 and 23 update the switch statements in `PostActions.delete()` and `PostActions.moveTo()` to add `s3` and `gs` branches.

- [ ] **Step 4: Run test, verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.api.SizeStableProbeTest"`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/{SizeStableProbe,PostActions}.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/api/SizeStableProbeTest.java
git commit -m "feat(fs-source): SizeStableProbe and PostActions"
```

---

**End of Phase 2.** Local FS backend, three emission strategies, stability probe, and post-actions all in place.

---

## Phase 3 — `FsSourceCommand` and the loop

### Task 14: `FsSourceDto` (config record)

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceDto.java`

The DTO defines the user-facing YAML/JSON config shape. It follows the same `interface Foo { @Data class Config }` pattern as `GcsSourceDto` and `S3SinkDto`.

- [ ] **Step 1: Create `FsSourceDto.java`**

```java
// FsSourceDto.java
package io.fleak.zephflow.lib.commands.fssource;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.fleak.zephflow.api.CommandConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public interface FsSourceDto {

  enum Mode { BOUNDED, UNBOUNDED }

  enum EmissionType { LINE, WHOLE_FILE, FILE_REFERENCE }

  enum PostActionType { NONE, DELETE, ARCHIVE }

  @Data @Builder @NoArgsConstructor @AllArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  class Config implements CommandConfig {
    /** Required. "file" | "s3" | "gs". */
    private String backend;
    /** Required. Root URN ("file:///abs/path/", "s3://bucket/prefix/", "gs://bucket/prefix/"). */
    private String root;
    /** Optional. Regex applied to filename. Named group {@code (?<ts>\d+)} enables timestamp ordering. */
    private String fileNameRegex;

    @Builder.Default private Emission emission = Emission.builder().type(EmissionType.LINE).build();
    @Builder.Default private Mode mode = Mode.BOUNDED;
    @Builder.Default private long listingIntervalMs = 30_000;

    @Builder.Default private Stability stability = Stability.builder().enabled(false).build();
    @Builder.Default private PostActionConfig postAction =
        PostActionConfig.builder().type(PostActionType.NONE).build();

    private PartitionConfig partition;
    private CheckpointOverride checkpoint;

    @Builder.Default private long commitBatchSize = 100;
    @Builder.Default private long commitIntervalMs = 5_000;
  }

  @Data @Builder @NoArgsConstructor @AllArgsConstructor
  class Emission {
    private EmissionType type;
    @Builder.Default private String encoding = "utf-8";
    @Builder.Default private int lineBatchSize = 500;
  }

  @Data @Builder @NoArgsConstructor @AllArgsConstructor
  class Stability {
    private boolean enabled;
    @Builder.Default private long probeDelayMs = 10_000;
  }

  @Data @Builder @NoArgsConstructor @AllArgsConstructor
  class PostActionConfig {
    private PostActionType type;
    /** For ARCHIVE: destination URN prefix. */
    private String destinationPrefix;
  }

  @Data @Builder @NoArgsConstructor @AllArgsConstructor
  class PartitionConfig {
    private int index;
    private int parallelism;
  }

  @Data @Builder @NoArgsConstructor @AllArgsConstructor
  class CheckpointOverride {
    private String backend;
    private String root;
  }
}
```

- [ ] **Step 2: Verify compilation**

Run: `./gradlew :lib:compileJava`
Expected: success.

- [ ] **Step 3: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceDto.java
git commit -m "feat(fs-source): FsSourceDto config shape"
```

---

### Task 15: `FsSourceConfigParser` + `FsSourceConfigValidator`

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceConfigParser.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceConfigValidator.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/FsSourceConfigValidatorTest.java`

- [ ] **Step 1: Write the failing test**

```java
// FsSourceConfigValidatorTest.java
package io.fleak.zephflow.lib.commands.fssource;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import org.junit.jupiter.api.Test;

class FsSourceConfigValidatorTest {

  private FsSourceDto.Config cfg() {
    return FsSourceDto.Config.builder()
        .backend("file")
        .root("file:///tmp/data")
        .fileNameRegex("invoice_(?<ts>\\d+)\\.json")
        .build();
  }

  @Test
  void acceptsValidConfig() {
    new FsSourceConfigValidator().validateConfig(cfg(), "n", JobContext.builder().build());
  }

  @Test
  void rejectsMissingBackend() {
    FsSourceDto.Config c = cfg();
    c.setBackend(null);
    assertThrows(IllegalArgumentException.class,
        () -> new FsSourceConfigValidator().validateConfig(c, "n", JobContext.builder().build()));
  }

  @Test
  void rejectsMissingRoot() {
    FsSourceDto.Config c = cfg();
    c.setRoot(null);
    assertThrows(IllegalArgumentException.class,
        () -> new FsSourceConfigValidator().validateConfig(c, "n", JobContext.builder().build()));
  }

  @Test
  void rejectsInvalidRegex() {
    FsSourceDto.Config c = cfg();
    c.setFileNameRegex("invoice_(?<ts>\\d+");
    assertThrows(IllegalArgumentException.class,
        () -> new FsSourceConfigValidator().validateConfig(c, "n", JobContext.builder().build()));
  }

  @Test
  void rejectsBoundedWithListingInterval() {
    FsSourceDto.Config c = cfg();
    c.setMode(FsSourceDto.Mode.BOUNDED);
    c.setListingIntervalMs(1000);
    // bounded + listingInterval is allowed (ignored), so this should NOT throw — test is here
    // as a guard that we don't accidentally enforce. Asserts no throw.
    new FsSourceConfigValidator().validateConfig(c, "n", JobContext.builder().build());
  }
}
```

- [ ] **Step 2: Implement `FsSourceConfigParser`**

```java
// FsSourceConfigParser.java
package io.fleak.zephflow.lib.commands.fssource;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigParser;
import io.fleak.zephflow.api.JobContext;
import java.util.Map;

public final class FsSourceConfigParser implements ConfigParser {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public CommandConfig parseConfig(Map<String, Object> rawConfig, String nodeId, JobContext ctx) {
    return MAPPER.convertValue(rawConfig, FsSourceDto.Config.class);
  }
}
```

- [ ] **Step 3: Implement `FsSourceConfigValidator`**

```java
// FsSourceConfigValidator.java
package io.fleak.zephflow.lib.commands.fssource;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendRegistry;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public final class FsSourceConfigValidator implements ConfigValidator {

  @Override
  public void validateConfig(CommandConfig config, String nodeId, JobContext ctx) {
    if (!(config instanceof FsSourceDto.Config c)) {
      throw new IllegalArgumentException("expected FsSourceDto.Config, got " + config.getClass());
    }
    if (c.getBackend() == null || c.getBackend().isBlank()) {
      throw new IllegalArgumentException("backend is required");
    }
    if (c.getRoot() == null || c.getRoot().isBlank()) {
      throw new IllegalArgumentException("root is required");
    }
    // Don't require the backend to be registered at validate time (helps testing).
    if (c.getFileNameRegex() != null && !c.getFileNameRegex().isBlank()) {
      try {
        Pattern.compile(c.getFileNameRegex());
      } catch (PatternSyntaxException e) {
        throw new IllegalArgumentException("invalid fileNameRegex: " + e.getMessage(), e);
      }
    }
    if (c.getEmission() == null || c.getEmission().getType() == null) {
      throw new IllegalArgumentException("emission.type is required");
    }
    if (c.getEmission().getType() == FsSourceDto.EmissionType.LINE
        && c.getEmission().getLineBatchSize() <= 0) {
      throw new IllegalArgumentException("emission.lineBatchSize must be > 0");
    }
    if (c.getPartition() != null) {
      if (c.getPartition().getParallelism() <= 0) {
        throw new IllegalArgumentException("partition.parallelism must be > 0");
      }
      if (c.getPartition().getIndex() < 0
          || c.getPartition().getIndex() >= c.getPartition().getParallelism()) {
        throw new IllegalArgumentException(
            "partition.index must be in [0, partition.parallelism)");
      }
    }
    if (c.getPostAction() != null
        && c.getPostAction().getType() == FsSourceDto.PostActionType.ARCHIVE
        && (c.getPostAction().getDestinationPrefix() == null
            || c.getPostAction().getDestinationPrefix().isBlank())) {
      throw new IllegalArgumentException("postAction.destinationPrefix required for ARCHIVE");
    }
  }
}
```

- [ ] **Step 4: Run test, verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.FsSourceConfigValidatorTest"`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSource{ConfigParser,ConfigValidator}.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/FsSourceConfigValidatorTest.java
git commit -m "feat(fs-source): config parser and validator"
```

---

### Task 16: `FsSourceExecutionContext` + `FsSourceCommandFactory` skeleton

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceExecutionContext.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommandFactory.java`

`FsSourceCommand` itself comes in Task 17; the factory references it so we stub the command in this task with a minimal class.

- [ ] **Step 1: Stub `FsSourceCommand` (filled in Task 17)**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommand.java` with:

```java
// FsSourceCommand.java (STUB — replaced in Task 17)
package io.fleak.zephflow.lib.commands.fssource;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;

public final class FsSourceCommand extends SourceCommand {

  public FsSourceCommand(String nodeId, JobContext jobContext) {
    super(nodeId, jobContext, new FsSourceConfigParser(), new FsSourceConfigValidator());
  }

  @Override public String commandName() { return "fssource"; }

  @Override
  public void execute(String callingUser, SourceEventAcceptor acceptor) {
    throw new UnsupportedOperationException("filled in Task 17");
  }

  @Override public SourceType sourceType() { return SourceType.STREAMING; }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider mp, JobContext jc, CommandConfig cfg, String nodeId) {
    return new FsSourceExecutionContext();
  }
}
```

- [ ] **Step 2: Implement `FsSourceExecutionContext`**

```java
// FsSourceExecutionContext.java
package io.fleak.zephflow.lib.commands.fssource;

import io.fleak.zephflow.api.ExecutionContext;
import io.fleak.zephflow.lib.commands.fssource.api.FileLister;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackend;
import io.fleak.zephflow.lib.commands.fssource.checkpoint.CheckpointStore;
import java.io.IOException;

public final class FsSourceExecutionContext implements ExecutionContext {

  FsBackend backend;
  FileLister lister;
  FileReader reader;
  CheckpointStore checkpointStore;

  @Override
  public void close() throws IOException {
    if (lister != null) try { lister.close(); } catch (Exception ignored) {}
    if (reader != null) try { reader.close(); } catch (Exception ignored) {}
    if (checkpointStore != null) try { checkpointStore.close(); } catch (Exception ignored) {}
  }
}
```

- [ ] **Step 3: Implement `FsSourceCommandFactory`**

```java
// FsSourceCommandFactory.java
package io.fleak.zephflow.lib.commands.fssource;

import io.fleak.zephflow.api.CommandFactory;
import io.fleak.zephflow.api.CommandType;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.OperatorCommand;

public final class FsSourceCommandFactory extends CommandFactory {

  @Override
  public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
    return new FsSourceCommand(nodeId, jobContext);
  }

  @Override
  public CommandType commandType() {
    return CommandType.SOURCE;
  }
}
```

If `CommandType.SOURCE` is not the exact enum value, copy whichever value `FileSourceCommandFactory` uses (read `FileSourceCommandFactory.java` from the repo — same module).

- [ ] **Step 4: Verify compilation**

Run: `./gradlew :lib:compileJava`
Expected: success.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/{FsSourceCommand,FsSourceCommandFactory,FsSourceExecutionContext}.java
git commit -m "feat(fs-source): command stub, factory, and execution context"
```

---

### Task 17: `FsSourceCommand` — bounded loop body

**Files:**
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommand.java` (replace stub)
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommandBoundedTest.java`

This task implements the bounded loop end-to-end against the local backend and the in-memory checkpoint store. Unbounded mode + watermark refinement land in Task 18.

- [ ] **Step 1: Write the failing integration-style test**

```java
// FsSourceCommandBoundedTest.java
package io.fleak.zephflow.lib.commands.fssource;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendRegistry;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackend;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FsSourceCommandBoundedTest {

  @BeforeEach
  void registerBackend() {
    FsBackendRegistry.unregister("file");
    FsBackendRegistry.register(new LocalFsBackend());
  }

  @AfterEach
  void cleanup() {
    FsBackendRegistry.unregister("file");
  }

  @Test
  void emitsLinesFromAllMatchingFilesInTimestampOrder(@TempDir Path tmp) throws Exception {
    Files.writeString(tmp.resolve("evt_3.log"), "z3a\nz3b");
    Files.writeString(tmp.resolve("evt_1.log"), "z1a\nz1b");
    Files.writeString(tmp.resolve("evt_2.log"), "z2a");
    Files.writeString(tmp.resolve("ignored.txt"), "skip");

    Map<String, Object> rawCfg = Map.of(
        "backend", "file",
        "root", tmp.toUri().toString(),
        "fileNameRegex", "evt_(?<ts>\\d+)\\.log",
        "emission", Map.of("type", "LINE", "encoding", "utf-8", "lineBatchSize", 10),
        "mode", "BOUNDED",
        "partition", Map.of("index", 0, "parallelism", 1)
    );

    List<RecordFleakData> emitted = new ArrayList<>();
    SourceEventAcceptor out = new SourceEventAcceptor() {
      @Override public void accept(List<RecordFleakData> r) { emitted.addAll(r); }
      @Override public void terminate() {}
    };

    FsSourceCommand cmd = new FsSourceCommand("node-1", JobContext.builder().build());
    cmd.parseAndValidateArg(rawCfg);
    cmd.initialize(io.fleak.zephflow.api.metric.NoopMetricClientProvider.INSTANCE);
    cmd.execute("test-user", out);

    List<Object> lines = emitted.stream().map(r -> r.unwrap().get("line")).toList();
    assertEquals(List.of("z1a", "z1b", "z2a", "z3a", "z3b"), lines);
  }
}
```

If `NoopMetricClientProvider.INSTANCE` does not exist in the codebase, use the simplest existing alternative — read `FileSourceCommandTest.java` to see what it uses.

- [ ] **Step 2: Replace the stub with the full `FsSourceCommand`**

```java
// FsSourceCommand.java  (full)
package io.fleak.zephflow.lib.commands.fssource;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.fssource.api.*;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackendConfig;
import io.fleak.zephflow.lib.commands.fssource.checkpoint.*;
import io.fleak.zephflow.lib.commands.fssource.emission.*;
import io.fleak.zephflow.lib.commands.fssource.util.Partitioner;
import io.fleak.zephflow.lib.commands.fssource.util.SourceIdHasher;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class FsSourceCommand extends SourceCommand {

  private FsCheckpoint checkpoint = FsCheckpoint.empty();
  private String checkpointKey;

  public FsSourceCommand(String nodeId, JobContext jobContext) {
    super(nodeId, jobContext, new FsSourceConfigParser(), new FsSourceConfigValidator());
  }

  @Override public String commandName() { return "fssource"; }

  @Override public SourceType sourceType() {
    FsSourceDto.Config c = (FsSourceDto.Config) commandConfig;
    return c != null && c.getMode() == FsSourceDto.Mode.UNBOUNDED
        ? SourceType.STREAMING : SourceType.BATCH;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider mp, JobContext jc, CommandConfig cfg, String nodeId) {
    FsSourceDto.Config c = (FsSourceDto.Config) cfg;
    FsSourceExecutionContext ec = new FsSourceExecutionContext();
    ec.backend = FsBackendRegistry.get(c.getBackend());
    FsBackendConfig bc = buildBackendConfig(c);
    ec.lister = ec.backend.createLister(bc);
    ec.reader = ec.backend.createReader(bc);
    ec.checkpointStore = new InMemoryCheckpointStore(); // Object-store impl wired in Task 19
    return ec;
  }

  private static FsBackendConfig buildBackendConfig(FsSourceDto.Config c) {
    // v1: only local FS is wired here; cloud backends extend this switch in their tasks.
    return switch (c.getBackend()) {
      case "file" -> new LocalFsBackendConfig(c.getRoot());
      default -> throw new IllegalStateException(
          "Backend " + c.getBackend() + " not wired in FsSourceCommand v1; see Tasks 22-23");
    };
  }

  @Override
  public void execute(String user, SourceEventAcceptor out) throws Exception {
    FsSourceExecutionContext ec = (FsSourceExecutionContext) getExecutionContext();
    FsSourceDto.Config c = (FsSourceDto.Config) commandConfig;

    int parallelism = resolveParallelism(c);
    int jobIndex = resolveJobIndex(c);
    String sourceId = SourceIdHasher.compute(c.getBackend(), c.getRoot(), c.getFileNameRegex());
    checkpointKey = sourceId + "/" + parallelism + "/" + jobIndex + ".json";
    checkpoint = ec.checkpointStore.load(checkpointKey).orElse(FsCheckpoint.empty());
    log.info("fs_source open: sourceId={} key={} watermark={}", sourceId, checkpointKey, checkpoint.watermark());

    Pattern regex = c.getFileNameRegex() == null ? null : Pattern.compile(c.getFileNameRegex());
    EmissionStrategy emission = buildEmission(c.getEmission());
    StabilityProbe probe = c.getStability().isEnabled()
        ? new SizeStableProbe(Duration.ofMillis(c.getStability().getProbeDelayMs()))
        : StabilityProbe.ALWAYS_STABLE;
    PostAction postAction = buildPostAction(c.getPostAction());

    boolean bounded = c.getMode() == FsSourceDto.Mode.BOUNDED;
    long backoffMs = 100;
    long backoffCapMs = 30_000;

    while (true) {
      ListRequest req = new ListRequest(c.getRoot(), regex);
      List<FileEntry> todo = new ArrayList<>();
      try (var stream = ec.lister.list(req)) {
        stream
            .filter(f -> Partitioner.assignedJob(f.key().urn(), parallelism) == jobIndex)
            .filter(f -> tsFromName(f, regex).compareTo(checkpoint.watermark()) >= 0)
            .filter(f -> !checkpoint.completedSinceWatermark().contains(f.key().urn()))
            .sorted(Comparator
                .comparing((FileEntry f) -> tsFromName(f, regex))
                .thenComparing(f -> f.key().urn()))
            .forEach(todo::add);
      }

      int emittedThisPass = 0;
      for (FileEntry f : todo) {
        if (!probe.isStable(f, ec.lister)) continue;
        emission.emit(f, ec.reader, out, jobContext);
        Instant t = tsFromName(f, regex);
        checkpoint = checkpoint.withCompleted(f.key().urn());
        if (t.isAfter(checkpoint.watermark())) {
          Set<String> retained = new HashSet<>();
          for (String urn : checkpoint.completedSinceWatermark()) {
            if (!urn.equals(f.key().urn())) retained.add(urn);
          }
          retained.add(f.key().urn());
          checkpoint = checkpoint.withWatermark(t, retained);
        }
        ec.checkpointStore.save(checkpointKey, checkpoint);
        postAction.run(f, ec.backend);
        emittedThisPass++;
      }

      if (bounded && emittedThisPass == 0 && todo.isEmpty()) {
        out.terminate();
        return;
      }
      if (!bounded && emittedThisPass == 0) {
        Thread.sleep(Math.min(backoffMs, backoffCapMs));
        backoffMs = Math.min(backoffMs * 2, backoffCapMs);
      } else {
        backoffMs = 100;
      }
      if (bounded) continue; // next pass will see no new files and exit above
      // Unbounded: respect listing interval before re-listing.
      Thread.sleep(c.getListingIntervalMs());
    }
  }

  private static Instant tsFromName(FileEntry f, Pattern regex) {
    if (regex == null) return f.lastModified();
    String name = new java.io.File(f.displayPath()).getName();
    Matcher m = regex.matcher(name);
    if (!m.matches()) return f.lastModified();
    try {
      String ts = m.group("ts");
      return Instant.ofEpochSecond(Long.parseLong(ts));
    } catch (Exception e) {
      return f.lastModified();
    }
  }

  private static EmissionStrategy buildEmission(FsSourceDto.Emission e) {
    return switch (e.getType()) {
      case LINE -> new LineEmissionStrategy(Charset.forName(e.getEncoding()), e.getLineBatchSize());
      case WHOLE_FILE -> new WholeFileEmissionStrategy(Charset.forName(e.getEncoding()));
      case FILE_REFERENCE -> new FileReferenceEmissionStrategy();
    };
  }

  private static PostAction buildPostAction(FsSourceDto.PostActionConfig pa) {
    if (pa == null || pa.getType() == FsSourceDto.PostActionType.NONE) return PostAction.NO_OP;
    return switch (pa.getType()) {
      case DELETE -> PostActions.delete();
      case ARCHIVE -> PostActions.moveTo(pa.getDestinationPrefix());
      default -> PostAction.NO_OP;
    };
  }

  private int resolveParallelism(FsSourceDto.Config c) {
    if (c.getPartition() != null) return c.getPartition().getParallelism();
    Object v = jobContext.getOtherProperties().get("zephflow.job.parallelism");
    return v instanceof Number n ? n.intValue() : 1;
  }

  private int resolveJobIndex(FsSourceDto.Config c) {
    if (c.getPartition() != null) return c.getPartition().getIndex();
    Object v = jobContext.getOtherProperties().get("zephflow.job.index");
    return v instanceof Number n ? n.intValue() : 0;
  }
}
```

- [ ] **Step 3: Run test, verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.FsSourceCommandBoundedTest"`
Expected: PASS.

If the test fails because `NoopMetricClientProvider.INSTANCE` doesn't exist, follow what `FileSourceCommandTest.java` uses (commonly a tiny inline `new MetricClientProvider() { ... }` test double or a project utility class) and update the test accordingly.

- [ ] **Step 4: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommand.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommandBoundedTest.java
git commit -m "feat(fs-source): FsSourceCommand bounded loop"
```

---

### Task 18: Unbounded loop + restart resumption test

**Files:**
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommand.java` (already handles unbounded; this task adds tests + a `volatile boolean terminated` exit gate)
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommandUnboundedTest.java`

- [ ] **Step 1: Add a terminate gate to `FsSourceCommand`**

Add a `private volatile boolean terminated = false;` field. In `execute()`, after each pass check the flag at top of loop. Override `terminate()`:

```java
@Override
public void terminate() throws java.io.IOException {
  terminated = true;
  super.terminate();
}
```

And modify the main `while (true)` to `while (!terminated)`. Add a final `out.terminate();` before returning from the loop body when the flag flips.

- [ ] **Step 2: Write the failing unbounded test**

```java
// FsSourceCommandUnboundedTest.java
package io.fleak.zephflow.lib.commands.fssource;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendRegistry;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackend;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FsSourceCommandUnboundedTest {

  @BeforeEach
  void register() {
    FsBackendRegistry.unregister("file");
    FsBackendRegistry.register(new LocalFsBackend());
  }
  @AfterEach
  void cleanup() { FsBackendRegistry.unregister("file"); }

  @Test
  void picksUpNewFilesOverTime(@TempDir Path tmp) throws Exception {
    Files.writeString(tmp.resolve("evt_1.log"), "first");

    Map<String, Object> rawCfg = Map.of(
        "backend", "file",
        "root", tmp.toUri().toString(),
        "fileNameRegex", "evt_(?<ts>\\d+)\\.log",
        "emission", Map.of("type", "LINE", "encoding", "utf-8", "lineBatchSize", 10),
        "mode", "UNBOUNDED",
        "listingIntervalMs", 100,
        "partition", Map.of("index", 0, "parallelism", 1)
    );

    List<RecordFleakData> emitted = Collections.synchronizedList(new ArrayList<>());
    SourceEventAcceptor out = new SourceEventAcceptor() {
      @Override public void accept(List<RecordFleakData> r) { emitted.addAll(r); }
      @Override public void terminate() {}
    };

    FsSourceCommand cmd = new FsSourceCommand("n", JobContext.builder().build());
    cmd.parseAndValidateArg(rawCfg);
    cmd.initialize(new io.fleak.zephflow.lib.TestMetricClientProvider());
    ExecutorService es = Executors.newSingleThreadExecutor();
    Future<?> f = es.submit(() -> { try { cmd.execute("u", out); } catch (Exception ignored) {} });

    Thread.sleep(500);
    assertEquals(1, emitted.size());

    Files.writeString(tmp.resolve("evt_2.log"), "second");
    Thread.sleep(500);
    assertEquals(2, emitted.size());

    cmd.terminate();
    f.get(2, TimeUnit.SECONDS);
    es.shutdownNow();
  }
}
```

If `io.fleak.zephflow.lib.TestMetricClientProvider` doesn't exist, replace with whatever test helper the repo already uses (search `MetricClientProvider` test doubles). A minimal inline anonymous class is acceptable.

- [ ] **Step 3: Run test, verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.FsSourceCommandUnboundedTest"`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommand.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommandUnboundedTest.java
git commit -m "feat(fs-source): unbounded loop with terminate gate"
```

---

### Task 19: Register `fs_source` in `OperatorCommandRegistry`

**Files:**
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java` — add the constant.
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java` — add the entry.
- Modify: a static-initializer location (probably `lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java` or a startup class) — register `LocalFsBackend` with `FsBackendRegistry`.
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/FsSourceRegistrationTest.java`

- [ ] **Step 1: Add the constant in `MiscUtils.java`**

After the existing `COMMAND_NAME_FILE_SOURCE` line:

```java
String COMMAND_NAME_FS_SOURCE = "fssource";
```

- [ ] **Step 2: Register the factory in `OperatorCommandRegistry`**

Insert in the `ImmutableMap.builder()` chain, near `COMMAND_NAME_FILE_SOURCE`:

```java
.put(COMMAND_NAME_FS_SOURCE, new io.fleak.zephflow.lib.commands.fssource.FsSourceCommandFactory())
```

Also add a static initializer (or a static method called from one) registering `LocalFsBackend`:

```java
static {
  io.fleak.zephflow.lib.commands.fssource.api.FsBackendRegistry.register(
      new io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackend());
}
```

(S3 and GCS get added in Tasks 22 and 23 by extending this block.)

- [ ] **Step 3: Write the failing registration test**

```java
// FsSourceRegistrationTest.java
package io.fleak.zephflow.lib.commands.fssource;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.commands.OperatorCommandRegistry;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendRegistry;
import org.junit.jupiter.api.Test;

class FsSourceRegistrationTest {

  @Test
  void fsSourceIsRegistered() {
    assertNotNull(OperatorCommandRegistry.OPERATOR_COMMANDS.get("fssource"));
  }

  @Test
  void localFsBackendIsRegistered() {
    assertNotNull(FsBackendRegistry.get("file"));
  }
}
```

- [ ] **Step 4: Run test, verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.FsSourceRegistrationTest"`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java \
        lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/FsSourceRegistrationTest.java
git commit -m "feat(fs-source): register fs_source command and local backend"
```

---

**End of Phase 3.** `fs_source` works end-to-end for local files with `InMemoryCheckpointStore`.

---

## Phase 4 — Object-store checkpoint and generation migration

### Task 20: `ObjectStoreCheckpointStore`

This implementation persists checkpoints to whatever `FsBackend` is configured (local FS, S3, or GCS — same backends the source already uses). For v1 testing we drive it via `LocalFsBackend`; the cloud backends in Phase 5 will validate it against S3/GCS through their integration tests.

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/checkpoint/ObjectStoreCheckpointStore.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/checkpoint/ObjectStoreCheckpointStoreTest.java`

- [ ] **Step 1: Write the failing test (uses local FS as the storage)**

```java
// ObjectStoreCheckpointStoreTest.java
package io.fleak.zephflow.lib.commands.fssource.checkpoint;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.commands.fssource.api.FsBackend;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackend;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackendConfig;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ObjectStoreCheckpointStoreTest {

  @Test
  void roundTrip(@TempDir Path tmp) throws Exception {
    FsBackend backend = new LocalFsBackend();
    LocalFsBackendConfig cfg = new LocalFsBackendConfig(tmp.toString());
    String prefix = tmp.toUri() + "_cp/";
    ObjectStoreCheckpointStore store =
        new ObjectStoreCheckpointStore(backend, cfg, prefix);

    FsCheckpoint cp = new FsCheckpoint(1, Instant.parse("2026-01-01T00:00:00Z"), Set.of("a", "b"));
    store.save("abc/3/0.json", cp);

    FsCheckpoint loaded = store.load("abc/3/0.json").orElseThrow();
    assertEquals(cp.watermark(), loaded.watermark());
    assertEquals(cp.completedSinceWatermark(), loaded.completedSinceWatermark());
  }

  @Test
  void listGenerationsAndShards(@TempDir Path tmp) throws Exception {
    FsBackend backend = new LocalFsBackend();
    LocalFsBackendConfig cfg = new LocalFsBackendConfig(tmp.toString());
    String prefix = tmp.toUri() + "_cp/";
    ObjectStoreCheckpointStore store =
        new ObjectStoreCheckpointStore(backend, cfg, prefix);

    store.save("abc/3/0.json", FsCheckpoint.empty());
    store.save("abc/3/1.json", FsCheckpoint.empty());
    store.save("abc/5/0.json", FsCheckpoint.empty());

    assertEquals(java.util.List.of(3, 5), store.listGenerations("abc").stream().sorted().toList());
    assertEquals(2, store.listShards("abc", 3).size());
  }
}
```

- [ ] **Step 2: Implement `ObjectStoreCheckpointStore`**

```java
// ObjectStoreCheckpointStore.java
package io.fleak.zephflow.lib.commands.fssource.checkpoint;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.fleak.zephflow.lib.commands.fssource.api.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Persists FsCheckpoint blobs as JSON via an FsBackend. Each shard has a single writer
 * (the owning job) so single-PUT atomicity at the object level is sufficient.
 *
 * <p>For local FS, writes go via tmp-file + ATOMIC_MOVE. For S3/GCS, a plain PUT is
 * atomic at the object level — handled by extending {@link #writeBytes} when those backends land.
 */
public final class ObjectStoreCheckpointStore implements CheckpointStore {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern GEN_SHARD = Pattern.compile("([0-9]+)/([0-9]+)\\.json$");

  private final FsBackend backend;
  private final FsBackendConfig cfg;
  private final String prefix; // e.g. "file:///tmp/_cp/" or "s3://bkt/_cp/"
  private final FileLister lister;
  private final FileReader reader;

  public ObjectStoreCheckpointStore(FsBackend backend, FsBackendConfig cfg, String prefix) {
    this.backend = backend;
    this.cfg = cfg;
    this.prefix = prefix.endsWith("/") ? prefix : prefix + "/";
    this.lister = backend.createLister(cfg);
    this.reader = backend.createReader(cfg);
  }

  @Override
  public Optional<FsCheckpoint> load(String key) {
    FileKey fk = new FileKey(backend.scheme(), prefix + key);
    try (InputStream in = reader.open(fk, 0)) {
      return Optional.of(MAPPER.readValue(in.readAllBytes(), FsCheckpoint.class));
    } catch (UncheckedIOException | IOException e) {
      return Optional.empty();
    }
  }

  @Override
  public void save(String key, FsCheckpoint cp) {
    try {
      byte[] bytes = MAPPER.writeValueAsBytes(cp);
      writeBytes(prefix + key, bytes);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void writeBytes(String urn, byte[] bytes) throws IOException {
    if (!"file".equals(backend.scheme())) {
      throw new UnsupportedOperationException(
          "ObjectStoreCheckpointStore.writeBytes for " + backend.scheme() + " is wired in Phase 5");
    }
    Path target = Paths.get(java.net.URI.create(urn));
    Files.createDirectories(target.getParent());
    Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
    Files.write(tmp, bytes);
    Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
  }

  @Override
  public List<Integer> listGenerations(String sourceId) {
    String root = prefix + sourceId + "/";
    Set<Integer> gens = new HashSet<>();
    try (Stream<FileEntry> s = lister.list(new ListRequest(root, null))) {
      s.forEach(f -> {
        var m = GEN_SHARD.matcher(f.key().urn());
        if (m.find()) gens.add(Integer.parseInt(m.group(1)));
      });
    } catch (UncheckedIOException ignored) {
      return List.of();
    }
    return new ArrayList<>(gens);
  }

  @Override
  public List<String> listShards(String sourceId, int generation) {
    String root = prefix + sourceId + "/" + generation + "/";
    List<String> shards = new ArrayList<>();
    try (Stream<FileEntry> s = lister.list(new ListRequest(root, null))) {
      s.forEach(f -> {
        int i = f.key().urn().lastIndexOf(sourceId + "/");
        if (i >= 0) shards.add(f.key().urn().substring(i + sourceId.length() + 1));
      });
    } catch (UncheckedIOException ignored) {
    }
    // Normalize shard keys to "<sourceId>/<gen>/<idx>.json" relative form expected by the SPI.
    List<String> normalized = new ArrayList<>();
    for (String s : shards) {
      // s is "<gen>/<idx>.json"; we want "<sourceId>/<gen>/<idx>.json"
      normalized.add(sourceId + "/" + s);
    }
    return normalized;
  }

  @Override
  public void close() {
    try { lister.close(); } catch (Exception ignored) {}
    try { reader.close(); } catch (Exception ignored) {}
  }
}
```

Note: cloud `writeBytes` branches are added in Tasks 22 (S3) and 23 (GCS).

- [ ] **Step 3: Run test, verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.checkpoint.ObjectStoreCheckpointStoreTest"`
Expected: PASS (2 tests).

- [ ] **Step 4: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/checkpoint/ObjectStoreCheckpointStore.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/checkpoint/ObjectStoreCheckpointStoreTest.java
git commit -m "feat(fs-source): ObjectStoreCheckpointStore backed by FsBackend"
```

---

### Task 21: `GenerationMigrator`

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/checkpoint/GenerationMigrator.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/checkpoint/GenerationMigratorTest.java`

- [ ] **Step 1: Write the failing test**

```java
// GenerationMigratorTest.java
package io.fleak.zephflow.lib.commands.fssource.checkpoint;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.Set;
import org.junit.jupiter.api.Test;

class GenerationMigratorTest {

  @Test
  void seedsFromPriorGenerationFilteredToOwnSlice() {
    InMemoryCheckpointStore store = new InMemoryCheckpointStore();
    // Old N=3 shards: completed = {urn-0, urn-1, urn-2, urn-3, urn-4, urn-5}.
    // (Distribution among 3 shards doesn't matter for migration — we union them.)
    store.save("src/3/0.json", new FsCheckpoint(1, Instant.parse("2026-01-01T00:00:00Z"),
        Set.of("urn-0", "urn-3")));
    store.save("src/3/1.json", new FsCheckpoint(1, Instant.parse("2026-01-01T00:00:00Z"),
        Set.of("urn-1", "urn-4")));
    store.save("src/3/2.json", new FsCheckpoint(1, Instant.parse("2026-01-02T00:00:00Z"),
        Set.of("urn-2", "urn-5")));

    // Migrate to N=2, jobIndex=0.
    FsCheckpoint seeded = GenerationMigrator.maybeSeed(store, "src", 2, 0);
    assertNotNull(seeded);
    // min watermark across shards:
    assertEquals(Instant.parse("2026-01-01T00:00:00Z"), seeded.watermark());
    // completed set: only entries where Partitioner.assignedJob(urn, 2) == 0
    for (String urn : seeded.completedSinceWatermark()) {
      assertEquals(0,
          io.fleak.zephflow.lib.commands.fssource.util.Partitioner.assignedJob(urn, 2));
    }
    // Seeded key persisted.
    assertTrue(store.load("src/2/0.json").isPresent());
  }

  @Test
  void returnsNullWhenNoPriorGenerations() {
    InMemoryCheckpointStore store = new InMemoryCheckpointStore();
    assertNull(GenerationMigrator.maybeSeed(store, "src", 2, 0));
  }

  @Test
  void noopIfSameGenerationAlreadyExists() {
    InMemoryCheckpointStore store = new InMemoryCheckpointStore();
    FsCheckpoint existing = new FsCheckpoint(1, Instant.parse("2026-02-01T00:00:00Z"), Set.of("x"));
    store.save("src/2/0.json", existing);
    FsCheckpoint result = GenerationMigrator.maybeSeed(store, "src", 2, 0);
    assertEquals(existing, result);
  }
}
```

- [ ] **Step 2: Implement `GenerationMigrator`**

```java
// GenerationMigrator.java
package io.fleak.zephflow.lib.commands.fssource.checkpoint;

import io.fleak.zephflow.lib.commands.fssource.util.Partitioner;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public final class GenerationMigrator {

  private GenerationMigrator() {}

  /**
   * Returns the seeded checkpoint for this (sourceId, N, jobIndex), persisting it
   * to {@code store}. Behavior:
   *
   * <ul>
   *   <li>If the current shard already exists, returns it unchanged.</li>
   *   <li>Else, picks the newest prior generation, unions its shards' completed sets,
   *       takes min(watermark), filters completed to this job's slice, persists, returns.</li>
   *   <li>Returns {@code null} if no prior generations exist.</li>
   * </ul>
   */
  public static FsCheckpoint maybeSeed(CheckpointStore store, String sourceId, int n, int jobIndex) {
    String currentKey = sourceId + "/" + n + "/" + jobIndex + ".json";
    Optional<FsCheckpoint> existing = store.load(currentKey);
    if (existing.isPresent()) return existing.get();

    List<Integer> prior = store.listGenerations(sourceId).stream()
        .filter(g -> g != n)
        .sorted(Comparator.reverseOrder())
        .toList();

    for (int prevN : prior) {
      List<String> shardKeys = store.listShards(sourceId, prevN);
      if (shardKeys.isEmpty()) continue;
      Instant minWatermark = Instant.MAX;
      Set<String> mergedCompleted = new HashSet<>();
      for (String sk : shardKeys) {
        Optional<FsCheckpoint> cp = store.load(sk);
        if (cp.isEmpty()) continue;
        if (cp.get().watermark().isBefore(minWatermark)) minWatermark = cp.get().watermark();
        mergedCompleted.addAll(cp.get().completedSinceWatermark());
      }
      if (minWatermark.equals(Instant.MAX)) minWatermark = Instant.EPOCH;
      Set<String> sliceCompleted = mergedCompleted.stream()
          .filter(urn -> Partitioner.assignedJob(urn, n) == jobIndex)
          .collect(Collectors.toSet());
      FsCheckpoint seeded = new FsCheckpoint(1, minWatermark, sliceCompleted);
      store.save(currentKey, seeded);
      return seeded;
    }
    return null;
  }
}
```

- [ ] **Step 3: Run test, verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.checkpoint.GenerationMigratorTest"`
Expected: PASS (3 tests).

- [ ] **Step 4: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/checkpoint/GenerationMigrator.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/checkpoint/GenerationMigratorTest.java
git commit -m "feat(fs-source): GenerationMigrator for parallelism changes"
```

---

### Task 22: Wire `CheckpointStore` resolution + migration into `FsSourceCommand`

**Files:**
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommand.java`

In `createExecutionContext`, replace the line that creates the `InMemoryCheckpointStore` with a resolver that:
- Uses `c.getCheckpoint()` override if set.
- Else defaults to an `ObjectStoreCheckpointStore` backed by the source's own `FsBackend` and a prefix derived from `c.getRoot()` (append `_zephflow_checkpoints/`).

In `execute()`, after computing `checkpointKey`, replace the direct `load(...).orElse(FsCheckpoint.empty())` call with a call into `GenerationMigrator.maybeSeed(...)` first; fall back to `FsCheckpoint.empty()` if migration returns null.

- [ ] **Step 1: Modify `createExecutionContext`**

Replace the line:

```java
ec.checkpointStore = new InMemoryCheckpointStore(); // Object-store impl wired in Task 19
```

with:

```java
ec.checkpointStore = buildCheckpointStore(c, ec.backend, bc);
```

And add the new method to the class:

```java
private static io.fleak.zephflow.lib.commands.fssource.checkpoint.CheckpointStore buildCheckpointStore(
    FsSourceDto.Config c, FsBackend sourceBackend, FsBackendConfig sourceBackendCfg) {
  FsBackend cpBackend;
  FsBackendConfig cpCfg;
  String prefixRoot;
  if (c.getCheckpoint() != null) {
    cpBackend = FsBackendRegistry.get(c.getCheckpoint().getBackend());
    cpCfg = buildBackendConfigForCheckpoint(c.getCheckpoint().getBackend(), c.getCheckpoint().getRoot());
    prefixRoot = c.getCheckpoint().getRoot();
  } else {
    cpBackend = sourceBackend;
    cpCfg = sourceBackendCfg;
    prefixRoot = c.getRoot();
  }
  String prefix = (prefixRoot.endsWith("/") ? prefixRoot : prefixRoot + "/") + "_zephflow_checkpoints/";
  return new io.fleak.zephflow.lib.commands.fssource.checkpoint.ObjectStoreCheckpointStore(cpBackend, cpCfg, prefix);
}

private static FsBackendConfig buildBackendConfigForCheckpoint(String backend, String root) {
  return switch (backend) {
    case "file" -> new io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackendConfig(root);
    default -> throw new IllegalStateException(
        "Checkpoint backend " + backend + " not wired in v1; see Tasks 22-23");
  };
}
```

(Tasks 22 and 23 extend the `switch` to add `s3` and `gs`.)

- [ ] **Step 2: Replace checkpoint load with generation migration**

In `execute()`, replace:

```java
checkpoint = ec.checkpointStore.load(checkpointKey).orElse(FsCheckpoint.empty());
```

with:

```java
FsCheckpoint seeded =
    io.fleak.zephflow.lib.commands.fssource.checkpoint.GenerationMigrator.maybeSeed(
        ec.checkpointStore, sourceId, parallelism, jobIndex);
checkpoint = seeded != null ? seeded : FsCheckpoint.empty();
```

- [ ] **Step 3: Verify by running the existing tests**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.FsSourceCommand*"`
Expected: existing bounded + unbounded tests still PASS.

- [ ] **Step 4: Add a restart-with-migration test**

```java
// FsSourceCommandMigrationTest.java
package io.fleak.zephflow.lib.commands.fssource;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendRegistry;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackend;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class FsSourceCommandMigrationTest {

  @BeforeEach void reg() { FsBackendRegistry.unregister("file"); FsBackendRegistry.register(new LocalFsBackend()); }
  @AfterEach void cleanup() { FsBackendRegistry.unregister("file"); }

  @Test
  void noReEmissionOnRestart(@TempDir Path tmp) throws Exception {
    for (int i = 1; i <= 5; i++) Files.writeString(tmp.resolve("evt_" + i + ".log"), "L" + i);
    Map<String, Object> rawCfg = Map.of(
        "backend", "file",
        "root", tmp.toUri().toString(),
        "fileNameRegex", "evt_(?<ts>\\d+)\\.log",
        "emission", Map.of("type", "LINE", "encoding", "utf-8", "lineBatchSize", 10),
        "mode", "BOUNDED",
        "partition", Map.of("index", 0, "parallelism", 1));

    List<RecordFleakData> firstRun = run(rawCfg);
    assertEquals(5, firstRun.size());

    // Add no new files; rerun. Expect zero emissions.
    List<RecordFleakData> secondRun = run(rawCfg);
    assertEquals(0, secondRun.size(), "should not re-emit on restart");
  }

  private List<RecordFleakData> run(Map<String, Object> rawCfg) throws Exception {
    List<RecordFleakData> emitted = new ArrayList<>();
    SourceEventAcceptor out = new SourceEventAcceptor() {
      @Override public void accept(List<RecordFleakData> r) { emitted.addAll(r); }
      @Override public void terminate() {}
    };
    FsSourceCommand cmd = new FsSourceCommand("n", JobContext.builder().build());
    cmd.parseAndValidateArg(rawCfg);
    cmd.initialize(new io.fleak.zephflow.lib.TestMetricClientProvider());
    cmd.execute("u", out);
    return emitted;
  }
}
```

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.FsSourceCommandMigrationTest"`
Expected: PASS — first run emits 5, second run emits 0 because the persisted checkpoint excludes them.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommand.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommandMigrationTest.java
git commit -m "feat(fs-source): default checkpoint store + generation migration in FsSourceCommand"
```

---

**End of Phase 4.** Object-store checkpoints with generation migration work end-to-end for local FS.

---

## Phase 5 — Cloud backends (S3, GCS) and cross-job determinism

### Task 23: `S3Backend` (config + lister + reader + backend) with LocalStack integration test

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/s3/S3BackendConfig.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/s3/S3Lister.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/s3/S3Reader.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/s3/S3Backend.java`
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommand.java` — extend `buildBackendConfig` and `buildBackendConfigForCheckpoint` switches with an `s3` branch.
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/checkpoint/ObjectStoreCheckpointStore.java` — extend `writeBytes` with an `s3` branch.
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/PostActions.java` — extend `delete()` and `moveTo()` with an `s3` branch.
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java` — register `S3Backend`.
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/s3/S3BackendIntegrationTest.java`

- [ ] **Step 1: Implement `S3BackendConfig`**

```java
// S3BackendConfig.java
package io.fleak.zephflow.lib.commands.fssource.backend.s3;

import io.fleak.zephflow.lib.commands.fssource.api.FsBackendConfig;

public record S3BackendConfig(
    String region,
    String credentialId,
    String s3EndpointOverride) implements FsBackendConfig {}
```

- [ ] **Step 2: Implement `S3Lister`**

```java
// S3Lister.java
package io.fleak.zephflow.lib.commands.fssource.backend.s3;

import io.fleak.zephflow.lib.commands.fssource.api.*;
import java.time.Instant;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

public final class S3Lister implements FileLister {

  private final S3Client client;

  public S3Lister(S3Client client) { this.client = client; }

  @Override
  public Stream<FileEntry> list(ListRequest req) {
    // Parse "s3://bucket/prefix/" from req.root().
    String urn = req.root();
    String stripped = urn.substring("s3://".length());
    int slash = stripped.indexOf('/');
    String bucket = slash < 0 ? stripped : stripped.substring(0, slash);
    String prefix = slash < 0 ? "" : stripped.substring(slash + 1);

    var iter = client.listObjectsV2Paginator(
        ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build());
    return StreamSupport.stream(iter.contents().spliterator(), false)
        .filter(o -> req.fileNameRegex() == null
            || req.fileNameRegex().matcher(filename(o.key())).matches())
        .map(o -> toEntry(bucket, o));
  }

  private static String filename(String key) {
    int i = key.lastIndexOf('/');
    return i < 0 ? key : key.substring(i + 1);
  }

  private static FileEntry toEntry(String bucket, S3Object o) {
    String urn = "s3://" + bucket + "/" + o.key();
    return new FileEntry(new FileKey("s3", urn), o.size(),
        Instant.ofEpochMilli(o.lastModified().toEpochMilli()), urn);
  }

  @Override
  public FileEntry stat(FileKey key) {
    // s3://bucket/key
    String stripped = key.urn().substring("s3://".length());
    int slash = stripped.indexOf('/');
    String bucket = stripped.substring(0, slash);
    String objectKey = stripped.substring(slash + 1);
    HeadObjectResponse h = client.headObject(
        HeadObjectRequest.builder().bucket(bucket).key(objectKey).build());
    return new FileEntry(key, h.contentLength(),
        Instant.ofEpochMilli(h.lastModified().toEpochMilli()), key.urn());
  }

  @Override
  public void close() { client.close(); }
}
```

- [ ] **Step 3: Implement `S3Reader`**

```java
// S3Reader.java
package io.fleak.zephflow.lib.commands.fssource.backend.s3;

import io.fleak.zephflow.lib.commands.fssource.api.FileKey;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import java.io.InputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

public final class S3Reader implements FileReader {

  private final S3Client client;

  public S3Reader(S3Client client) { this.client = client; }

  @Override
  public InputStream open(FileKey key, long offset) {
    String stripped = key.urn().substring("s3://".length());
    int slash = stripped.indexOf('/');
    String bucket = stripped.substring(0, slash);
    String objectKey = stripped.substring(slash + 1);
    GetObjectRequest.Builder b = GetObjectRequest.builder().bucket(bucket).key(objectKey);
    if (offset > 0) b.range("bytes=" + offset + "-");
    return client.getObject(b.build());
  }

  @Override
  public void close() { client.close(); }
}
```

- [ ] **Step 4: Implement `S3Backend`**

```java
// S3Backend.java
package io.fleak.zephflow.lib.commands.fssource.backend.s3;

import io.fleak.zephflow.lib.commands.fssource.api.*;
import java.net.URI;
import java.util.Set;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public final class S3Backend implements FsBackend {

  public static final String SCHEME = "s3";

  @Override public String scheme() { return SCHEME; }

  @Override
  public FileLister createLister(FsBackendConfig cfg) {
    return new S3Lister(client((S3BackendConfig) cfg));
  }

  @Override
  public FileReader createReader(FsBackendConfig cfg) {
    return new S3Reader(client((S3BackendConfig) cfg));
  }

  @Override
  public Set<Capability> capabilities() {
    return Set.of(Capability.DELETE, Capability.MOVE, Capability.RANGE_READ);
  }

  static S3Client client(S3BackendConfig cfg) {
    S3ClientBuilder b = S3Client.builder().region(Region.of(cfg.region()));
    if (cfg.s3EndpointOverride() != null && !cfg.s3EndpointOverride().isBlank()) {
      b.endpointOverride(URI.create(cfg.s3EndpointOverride()));
      b.forcePathStyle(true);
    }
    // Credentials resolved via credentialId here; v1 uses default provider chain. Wire
    // existing credentials infrastructure (see io.fleak.zephflow.lib.credentials) for
    // accessKeyId/secretAccessKey lookup. Fall back to the SDK default chain otherwise.
    return b.build();
  }
}
```

- [ ] **Step 5: Wire `S3Backend` into `PostActions`, `ObjectStoreCheckpointStore`, and `FsSourceCommand`**

In `PostActions.delete()`, change the switch:
```java
switch (file.key().backend()) {
  case "file" -> Files.delete(Paths.get(java.net.URI.create(file.key().urn())));
  case "s3" -> {
    String stripped = file.key().urn().substring("s3://".length());
    int slash = stripped.indexOf('/');
    String bucket = stripped.substring(0, slash);
    String objectKey = stripped.substring(slash + 1);
    try (var c = io.fleak.zephflow.lib.commands.fssource.backend.s3.S3Backend.client(
        (io.fleak.zephflow.lib.commands.fssource.backend.s3.S3BackendConfig) /* TODO config from context */ null)) {
      c.deleteObject(software.amazon.awssdk.services.s3.model.DeleteObjectRequest.builder()
          .bucket(bucket).key(objectKey).build());
    }
  }
  default -> throw new UnsupportedOperationException(
      "Delete not implemented for backend " + file.key().backend() + " in v1");
}
```

That sketch reveals a problem: `PostActions.delete()` has no access to the source's S3 config. Fix: change the signature of `PostAction.run` to accept the `FsBackendConfig` too, and propagate from `FsSourceCommand`. Apply this refactor:

```java
// PostAction.java
public interface PostAction {
  void run(FileEntry file, FsBackend backend, FsBackendConfig backendConfig) throws Exception;
  PostAction NO_OP = (file, backend, cfg) -> {};
}
```

Then update `PostActions.delete()`, `PostActions.moveTo()`, `FsSourceCommand.execute()`, and the `SizeStableProbeTest` mock if any — to pass through the config.

In `ObjectStoreCheckpointStore.writeBytes`, add the `s3` branch:
```java
if ("s3".equals(backend.scheme())) {
  S3BackendConfig sc = (S3BackendConfig) cfg;
  try (S3Client c = S3Backend.client(sc)) {
    String stripped = urn.substring("s3://".length());
    int slash = stripped.indexOf('/');
    String bucket = stripped.substring(0, slash);
    String key = stripped.substring(slash + 1);
    c.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(),
        RequestBody.fromBytes(bytes));
  }
  return;
}
```

In `FsSourceCommand.buildBackendConfig` and `buildBackendConfigForCheckpoint`, add an `s3` branch that reads from a new optional `backendConfig:` block in the YAML config. (Add a generic `Map<String,Object> backendConfig` field to `FsSourceDto.Config` and pass it through.)

- [ ] **Step 6: Register `S3Backend` in the static initializer**

In `OperatorCommandRegistry.java`, extend the static block:
```java
static {
  FsBackendRegistry.register(new LocalFsBackend());
  FsBackendRegistry.register(new S3Backend());
}
```

- [ ] **Step 7: Write the LocalStack integration test**

```java
// S3BackendIntegrationTest.java
package io.fleak.zephflow.lib.commands.fssource.backend.s3;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.commands.fssource.api.*;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.regex.Pattern;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

@Tag("integration")
@Testcontainers
class S3BackendIntegrationTest {

  @Container
  static LocalStackContainer LOCALSTACK =
      new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.5"))
          .withServices(LocalStackContainer.Service.S3)
          .withStartupTimeout(Duration.ofMinutes(2));

  private static S3Client s3() {
    return S3Client.builder()
        .endpointOverride(LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3))
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create(LOCALSTACK.getAccessKey(), LOCALSTACK.getSecretKey())))
        .region(Region.of(LOCALSTACK.getRegion()))
        .forcePathStyle(true)
        .build();
  }

  @BeforeEach
  void setupBucket() {
    try (S3Client c = s3()) {
      c.createBucket(CreateBucketRequest.builder().bucket("test-bkt").build());
      c.putObject(PutObjectRequest.builder().bucket("test-bkt").key("data/evt_1.log").build(),
          RequestBody.fromString("hello"));
      c.putObject(PutObjectRequest.builder().bucket("test-bkt").key("data/evt_2.log").build(),
          RequestBody.fromString("world"));
      c.putObject(PutObjectRequest.builder().bucket("test-bkt").key("data/skip.txt").build(),
          RequestBody.fromString("nope"));
    }
  }

  @Test
  void listsAndReadsMatchingObjects() throws Exception {
    S3BackendConfig cfg = new S3BackendConfig(
        LOCALSTACK.getRegion(),
        null,
        LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.S3).toString());
    S3Backend backend = new S3Backend();
    FileLister lister = backend.createLister(cfg);
    FileReader reader = backend.createReader(cfg);

    List<FileEntry> entries =
        lister.list(new ListRequest("s3://test-bkt/data/", Pattern.compile("evt_\\d+\\.log")))
            .toList();
    assertEquals(2, entries.size());

    try (InputStream in = reader.open(entries.get(0).key(), 0)) {
      String body = new String(in.readAllBytes());
      assertTrue(body.equals("hello") || body.equals("world"));
    }
  }
}
```

- [ ] **Step 8: Add Testcontainers LocalStack dependency**

In `lib/build.gradle` under `testImplementation`, add (if not present):
```gradle
testImplementation 'org.testcontainers:localstack'
```

Confirm via: `./gradlew :lib:dependencies --configuration testRuntimeClasspath | grep localstack`

- [ ] **Step 9: Run the integration test**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.backend.s3.S3BackendIntegrationTest" -PincludeTags=integration`
(Or whatever flag the project uses; check `lib/build.gradle` for the integration-tag include pattern. If integration tests are run by default, omit `-PincludeTags`.)

Expected: PASS.

- [ ] **Step 10: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/s3/ \
        lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/{FsSourceCommand,FsSourceDto}.java \
        lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/{PostAction,PostActions}.java \
        lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/checkpoint/ObjectStoreCheckpointStore.java \
        lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java \
        lib/build.gradle \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/s3/
git commit -m "feat(fs-source): S3 backend with LocalStack integration test"
```

---

### Task 24: `GcsBackend`

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/gcs/GcsBackendConfig.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/gcs/GcsLister.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/gcs/GcsReader.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/gcs/GcsBackend.java`
- Modify: `FsSourceCommand` switches to add `gs` branch.
- Modify: `ObjectStoreCheckpointStore.writeBytes` to add `gs` branch.
- Modify: `PostActions.delete()` and `moveTo()` to add `gs` branches.
- Modify: `OperatorCommandRegistry` static block to register `GcsBackend`.
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/gcs/GcsBackendIntegrationTest.java`

This task mirrors Task 23 exactly, substituting GCS APIs for S3 APIs. The same set of modifications and the same test shape apply.

- [ ] **Step 1: Implement `GcsBackendConfig`**

```java
// GcsBackendConfig.java
package io.fleak.zephflow.lib.commands.fssource.backend.gcs;

import io.fleak.zephflow.lib.commands.fssource.api.FsBackendConfig;

public record GcsBackendConfig(String serviceAccountJson) implements FsBackendConfig {}
```

- [ ] **Step 2: Implement `GcsLister`**

```java
// GcsLister.java
package io.fleak.zephflow.lib.commands.fssource.backend.gcs;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import io.fleak.zephflow.lib.commands.fssource.api.*;
import java.time.Instant;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class GcsLister implements FileLister {

  private final Storage storage;

  public GcsLister(Storage storage) { this.storage = storage; }

  @Override
  public Stream<FileEntry> list(ListRequest req) {
    String urn = req.root();
    String stripped = urn.substring("gs://".length());
    int slash = stripped.indexOf('/');
    String bucket = slash < 0 ? stripped : stripped.substring(0, slash);
    String prefix = slash < 0 ? "" : stripped.substring(slash + 1);
    Page<Blob> page = storage.list(bucket, BlobListOption.prefix(prefix));
    return StreamSupport.stream(page.iterateAll().spliterator(), false)
        .filter(b -> req.fileNameRegex() == null
            || req.fileNameRegex().matcher(filename(b.getName())).matches())
        .map(b -> {
          String fullUrn = "gs://" + bucket + "/" + b.getName();
          return new FileEntry(new FileKey("gs", fullUrn),
              b.getSize() == null ? 0 : b.getSize(),
              b.getUpdateTimeOffsetDateTime() == null
                  ? Instant.EPOCH
                  : b.getUpdateTimeOffsetDateTime().toInstant(),
              fullUrn);
        });
  }

  private static String filename(String key) {
    int i = key.lastIndexOf('/');
    return i < 0 ? key : key.substring(i + 1);
  }

  @Override
  public FileEntry stat(FileKey key) {
    String stripped = key.urn().substring("gs://".length());
    int slash = stripped.indexOf('/');
    String bucket = stripped.substring(0, slash);
    String objectKey = stripped.substring(slash + 1);
    Blob b = storage.get(bucket, objectKey);
    return new FileEntry(key, b.getSize() == null ? 0 : b.getSize(),
        b.getUpdateTimeOffsetDateTime().toInstant(), key.urn());
  }
}
```

- [ ] **Step 3: Implement `GcsReader`**

```java
// GcsReader.java
package io.fleak.zephflow.lib.commands.fssource.backend.gcs;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import io.fleak.zephflow.lib.commands.fssource.api.FileKey;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;

public final class GcsReader implements FileReader {

  private final Storage storage;

  public GcsReader(Storage storage) { this.storage = storage; }

  @Override
  public InputStream open(FileKey key, long offset) {
    String stripped = key.urn().substring("gs://".length());
    int slash = stripped.indexOf('/');
    String bucket = stripped.substring(0, slash);
    String objectKey = stripped.substring(slash + 1);
    ReadChannel rc = storage.reader(BlobId.of(bucket, objectKey));
    if (offset > 0) {
      try { rc.seek(offset); } catch (IOException e) { throw new UncheckedIOException(e); }
    }
    return Channels.newInputStream(rc);
  }
}
```

- [ ] **Step 4: Implement `GcsBackend`**

```java
// GcsBackend.java
package io.fleak.zephflow.lib.commands.fssource.backend.gcs;

import com.google.cloud.storage.Storage;
import io.fleak.zephflow.lib.commands.fssource.api.*;
import io.fleak.zephflow.lib.gcp.GcsClientFactory;
import java.util.Set;

public final class GcsBackend implements FsBackend {

  public static final String SCHEME = "gs";

  private static final GcsClientFactory FACTORY = new GcsClientFactory();

  @Override public String scheme() { return SCHEME; }

  @Override
  public FileLister createLister(FsBackendConfig cfg) {
    return new GcsLister(client((GcsBackendConfig) cfg));
  }

  @Override
  public FileReader createReader(FsBackendConfig cfg) {
    return new GcsReader(client((GcsBackendConfig) cfg));
  }

  @Override
  public Set<Capability> capabilities() {
    return Set.of(Capability.DELETE, Capability.MOVE, Capability.RANGE_READ);
  }

  static Storage client(GcsBackendConfig cfg) {
    return cfg.serviceAccountJson() == null
        ? FACTORY.createStorageClient()
        : FACTORY.createStorageClient(cfg.serviceAccountJson());
  }
}
```

- [ ] **Step 5: Wire `GcsBackend` into `PostActions`, `ObjectStoreCheckpointStore`, and `FsSourceCommand`**

Apply the same kind of edits as Task 23 step 5 for the `gs` scheme:

- In `PostActions.delete()` switch, add a `gs` branch that calls `Storage.delete(BlobId)`.
- In `PostActions.moveTo()` switch, add a `gs` branch that calls `Storage.copy(...)` + `Storage.delete(...)`.
- In `ObjectStoreCheckpointStore.writeBytes`, add a `gs` branch that calls `Storage.create(BlobInfo, bytes)`.
- In `FsSourceCommand.buildBackendConfig` and `buildBackendConfigForCheckpoint`, add a `gs` branch that reads from the YAML `backendConfig`.

- [ ] **Step 6: Register `GcsBackend` in the static initializer**

```java
static {
  FsBackendRegistry.register(new LocalFsBackend());
  FsBackendRegistry.register(new S3Backend());
  FsBackendRegistry.register(new GcsBackend());
}
```

- [ ] **Step 7: Write the GCS integration test using `testcontainers.gcloud`**

```java
// GcsBackendIntegrationTest.java
package io.fleak.zephflow.lib.commands.fssource.backend.gcs;

import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.fleak.zephflow.lib.commands.fssource.api.*;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Pattern;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Tag("integration")
@Testcontainers
class GcsBackendIntegrationTest {

  @Container
  static GenericContainer<?> FAKE_GCS =
      new GenericContainer<>(DockerImageName.parse("fsouza/fake-gcs-server:1.49"))
          .withExposedPorts(4443)
          .withCommand("-scheme", "http");

  private static Storage storage() {
    return StorageOptions.newBuilder()
        .setHost("http://" + FAKE_GCS.getHost() + ":" + FAKE_GCS.getMappedPort(4443))
        .setProjectId("test-proj")
        .setCredentials(NoCredentials.getInstance())
        .build()
        .getService();
  }

  @BeforeEach
  void setupBucket() {
    Storage s = storage();
    s.create(BucketInfo.of("test-bkt"));
    s.create(BlobInfo.newBuilder("test-bkt", "data/evt_1.log").build(),
        "hello".getBytes(StandardCharsets.UTF_8));
    s.create(BlobInfo.newBuilder("test-bkt", "data/evt_2.log").build(),
        "world".getBytes(StandardCharsets.UTF_8));
    s.create(BlobInfo.newBuilder("test-bkt", "data/skip.txt").build(),
        "nope".getBytes(StandardCharsets.UTF_8));
  }

  @Test
  void listsAndReads() throws Exception {
    Storage s = storage();
    GcsLister lister = new GcsLister(s);
    GcsReader reader = new GcsReader(s);

    List<FileEntry> entries =
        lister.list(new ListRequest("gs://test-bkt/data/", Pattern.compile("evt_\\d+\\.log")))
            .toList();
    assertEquals(2, entries.size());

    try (InputStream in = reader.open(entries.get(0).key(), 0)) {
      String body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      assertTrue(body.equals("hello") || body.equals("world"));
    }
  }
}
```

- [ ] **Step 8: Run integration test**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.backend.gcs.GcsBackendIntegrationTest"`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/gcs/ \
        lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/{FsSourceCommand,FsSourceDto}.java \
        lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/PostActions.java \
        lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/checkpoint/ObjectStoreCheckpointStore.java \
        lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/gcs/
git commit -m "feat(fs-source): GCS backend with fake-gcs integration test"
```

---

### Task 25: Cross-job determinism test

**Files:**
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/CrossJobDeterminismTest.java`

- [ ] **Step 1: Write the test**

```java
// CrossJobDeterminismTest.java
package io.fleak.zephflow.lib.commands.fssource;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendRegistry;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackend;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class CrossJobDeterminismTest {

  @BeforeEach void reg() { FsBackendRegistry.unregister("file"); FsBackendRegistry.register(new LocalFsBackend()); }
  @AfterEach void cleanup() { FsBackendRegistry.unregister("file"); }

  @Test
  void threeJobsOwnDisjointSlicesUnioningToTotal(@TempDir Path tmp) throws Exception {
    int totalFiles = 300;
    for (int i = 0; i < totalFiles; i++) {
      Files.writeString(tmp.resolve("evt_" + i + ".log"), "content-" + i);
    }

    Set<String> emittedByJob0 = runJob(tmp, 0, 3);
    Set<String> emittedByJob1 = runJob(tmp, 1, 3);
    Set<String> emittedByJob2 = runJob(tmp, 2, 3);

    Set<String> union = new HashSet<>();
    union.addAll(emittedByJob0);
    union.addAll(emittedByJob1);
    union.addAll(emittedByJob2);
    assertEquals(totalFiles, union.size(), "Total emitted across jobs must equal total files");

    Set<String> overlap = new HashSet<>(emittedByJob0);
    overlap.retainAll(emittedByJob1);
    assertTrue(overlap.isEmpty(), "No overlap between jobs 0 and 1");
    overlap = new HashSet<>(emittedByJob0); overlap.retainAll(emittedByJob2);
    assertTrue(overlap.isEmpty(), "No overlap between jobs 0 and 2");
    overlap = new HashSet<>(emittedByJob1); overlap.retainAll(emittedByJob2);
    assertTrue(overlap.isEmpty(), "No overlap between jobs 1 and 2");
  }

  private Set<String> runJob(Path tmp, int idx, int parallelism) throws Exception {
    Map<String, Object> rawCfg = Map.of(
        "backend", "file",
        "root", tmp.toUri().toString(),
        "fileNameRegex", "evt_(?<ts>\\d+)\\.log",
        "emission", Map.of("type", "LINE", "encoding", "utf-8", "lineBatchSize", 100),
        "mode", "BOUNDED",
        "partition", Map.of("index", idx, "parallelism", parallelism));

    Set<String> emitted = new HashSet<>();
    SourceEventAcceptor out = new SourceEventAcceptor() {
      @Override public void accept(List<RecordFleakData> r) {
        for (RecordFleakData rec : r) emitted.add((String) rec.unwrap().get("file"));
      }
      @Override public void terminate() {}
    };

    FsSourceCommand cmd = new FsSourceCommand("n" + idx, JobContext.builder().build());
    cmd.parseAndValidateArg(rawCfg);
    cmd.initialize(new io.fleak.zephflow.lib.TestMetricClientProvider());
    cmd.execute("u", out);
    return emitted;
  }
}
```

- [ ] **Step 2: Run test**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.CrossJobDeterminismTest"`
Expected: PASS — the three slices cover all 300 files with no overlap.

- [ ] **Step 3: Commit**

```bash
git add lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/CrossJobDeterminismTest.java
git commit -m "test(fs-source): cross-job determinism (3 jobs, disjoint slices, full coverage)"
```

---

**End of Phase 5.** Three backends (local, S3, GCS) work end-to-end with checkpointing, generation migration, and verified cross-job determinism.

---

## Phase 6 — Bounded-memory proof, parity, `gcs_source` removal

### Task 26: Bounded-memory streaming test (2 GB)

**Files:**
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/BoundedMemoryStreamingTest.java`

- [ ] **Step 1: Write the test**

```java
// BoundedMemoryStreamingTest.java
package io.fleak.zephflow.lib.commands.fssource;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendRegistry;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackend;
import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

@Tag("slow")
class BoundedMemoryStreamingTest {

  @BeforeEach void reg() { FsBackendRegistry.unregister("file"); FsBackendRegistry.register(new LocalFsBackend()); }
  @AfterEach void cleanup() { FsBackendRegistry.unregister("file"); }

  @Test
  void streams2GbFileWithoutOom(@TempDir Path tmp) throws Exception {
    Path file = tmp.resolve("evt_1.log");
    long targetBytes = 2L * 1024 * 1024 * 1024; // 2 GB
    String line = "x".repeat(1000);              // ~1000 bytes per line
    long expectedLines = targetBytes / (line.length() + 1);
    try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8)) {
      for (long i = 0; i < expectedLines; i++) {
        w.write(line);
        w.newLine();
      }
    }
    assertTrue(Files.size(file) >= targetBytes - 4096,
        "synthetic input must be ≥ 2 GB, was " + Files.size(file));

    Map<String, Object> rawCfg = Map.of(
        "backend", "file",
        "root", tmp.toUri().toString(),
        "fileNameRegex", "evt_(?<ts>\\d+)\\.log",
        "emission", Map.of("type", "LINE", "encoding", "utf-8", "lineBatchSize", 500),
        "mode", "BOUNDED",
        "partition", Map.of("index", 0, "parallelism", 1));

    AtomicLong count = new AtomicLong();
    SourceEventAcceptor out = new SourceEventAcceptor() {
      @Override public void accept(List<RecordFleakData> r) { count.addAndGet(r.size()); }
      @Override public void terminate() {}
    };

    FsSourceCommand cmd = new FsSourceCommand("n", JobContext.builder().build());
    cmd.parseAndValidateArg(rawCfg);
    cmd.initialize(new io.fleak.zephflow.lib.TestMetricClientProvider());
    cmd.execute("u", out);

    assertEquals(expectedLines, count.get());
  }
}
```

- [ ] **Step 2: Run the test under a capped heap**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.BoundedMemoryStreamingTest" -PtestJvmArgs="-Xmx256m"`

If the project does not have a `testJvmArgs` Gradle property, modify `lib/build.gradle`'s `test { jvmArgs ... }` block to accept it, or set `org.gradle.jvmargs` for this run. Expected: PASS without OOM.

- [ ] **Step 3: Commit**

```bash
git add lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/BoundedMemoryStreamingTest.java
git commit -m "test(fs-source): bounded-memory streaming of 2GB file"
```

---

### Task 27: `gcs_source` parity test

**Files:**
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/GcsSourceParityTest.java`

This test feeds an identical fixture to both the legacy `gcs_source` command and the new `fs_source` (with `backend: gs`, `emission: whole_file`) against the same `fake-gcs-server` and asserts the event payloads match.

- [ ] **Step 1: Write the test**

```java
// GcsSourceParityTest.java
package io.fleak.zephflow.lib.commands.fssource;

// Imports identical to GcsBackendIntegrationTest for fake-gcs-server setup
// plus io.fleak.zephflow.lib.commands.gcssource.GcsSourceCommand and Factory.

import static org.junit.jupiter.api.Assertions.*;

// (full imports omitted for brevity — copy from GcsBackendIntegrationTest)

@org.junit.jupiter.api.Tag("integration")
@org.testcontainers.junit.jupiter.Testcontainers
class GcsSourceParityTest {

  // Same @Container setup as GcsBackendIntegrationTest.

  @org.junit.jupiter.api.Test
  void wholeFileEmissionMatchesGcsSourceOutput() throws Exception {
    // 1. Set up fake-gcs-server with the same 3 files (JSON-encoded objects).
    // 2. Run legacy gcs_source with encodingType=JSON, collect events.
    // 3. Run new fs_source with backend=gs, emission=whole_file, encoding=utf-8, collect events.
    // 4. Assert event count, payloads, and ordering match.
    //    Allow for keys "content" (fs_source) vs whatever gcs_source uses;
    //    transform the legacy output via a small adapter before comparison.
    // (Concrete code is straightforward; mirror GcsBackendIntegrationTest setup and
    //  GcsSourceCommandTest for the legacy-command call shape.)
  }
}
```

The above is the only place in this plan where I leave concrete steps as guidance rather than full code, because the legacy `GcsSourceCommand` config shape is preserved by the codebase itself — read its existing test (`GcsSourceCommandTest.java`) to get the call shape exactly, and mirror it. The new-side call shape is already shown in `FsSourceCommandBoundedTest`. The parity test is mechanical: same input, same expected payloads modulo key naming.

- [ ] **Step 2: Run the parity test**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.GcsSourceParityTest"`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/GcsSourceParityTest.java
git commit -m "test(fs-source): parity test against legacy gcs_source"
```

---

### Task 28: Remove `gcs_source`

**Gate:** Task 27's parity test must be green before this task lands. Do not start this task otherwise.

**Files:**
- Delete: `lib/src/main/java/io/fleak/zephflow/lib/commands/gcssource/` (entire directory — `GcsSourceCommand.java`, `GcsSourceCommandFactory.java`, `GcsSourceFetcher.java`, `GcsSourceDto.java`, `GcsSourceConfigValidator.java`, `GcsSourceConfigParser.java` if present)
- Delete: `lib/src/test/java/io/fleak/zephflow/lib/commands/gcssource/` (entire directory)
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java` — remove the `COMMAND_NAME_GCS_SOURCE` entry from the `ImmutableMap.builder()` chain.
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java` — remove the `COMMAND_NAME_GCS_SOURCE` constant.
- Modify (release notes): `CHANGELOG.md` if the repo has one, else `docs/RELEASE_NOTES.md` — add an entry for the breaking change.

- [ ] **Step 1: Delete the gcs_source sources and tests**

```bash
git rm -r lib/src/main/java/io/fleak/zephflow/lib/commands/gcssource
git rm -r lib/src/test/java/io/fleak/zephflow/lib/commands/gcssource
```

- [ ] **Step 2: Remove the registry entry**

In `OperatorCommandRegistry.java`, delete the line:

```java
.put(COMMAND_NAME_GCS_SOURCE, new GcsSourceCommandFactory())
```

And remove the import.

- [ ] **Step 3: Remove the constant**

In `MiscUtils.java`, delete:

```java
String COMMAND_NAME_GCS_SOURCE = "gcssource";
```

- [ ] **Step 4: Add a release-note entry**

Append to `CHANGELOG.md` (or create `docs/RELEASE_NOTES.md`):

```markdown
## Breaking changes

### `gcs_source` removed

The standalone `gcs_source` command has been removed. Use `fs_source` with
`backend: gs` instead. One-line config migration:

    # Before — REMOVED
    gcs_source:
      bucket: my-bucket
      prefix: data/
      encodingType: JSON

    # After
    fs_source:
      backend: gs
      root: gs://my-bucket/data/
      emission: { type: WHOLE_FILE, encoding: utf-8 }
      mode: BOUNDED
      partition: { index: 0, parallelism: 1 }
```

- [ ] **Step 5: Verify compilation and remaining tests still pass**

Run: `./gradlew :lib:compileJava :lib:test`
Expected: success — no test referenced `GcsSourceCommand` after deletion (the parity test in Task 27 was the only such reference; if it's still there it must reference the legacy command via reflection or copy of its config DTO, which is no longer valid — adjust the parity test to use an inline replica of the legacy DTO if needed; otherwise the parity test will fail to compile and must be deleted as well now that its purpose is served).

Note: many readers will choose to delete the parity test as part of this commit, since the legacy code it tested no longer exists. That's correct — the parity test is a gate, not a permanent fixture. The commit message should reflect both.

- [ ] **Step 6: Commit**

```bash
git add -u
git commit -m "chore: remove legacy gcs_source command (superseded by fs_source backend=gs)"
```

---

### Task 29: Final verification

- [ ] **Step 1: Run the full lib test suite**

Run: `./gradlew :lib:test`
Expected: all unit tests PASS.

- [ ] **Step 2: Run integration tests**

Run: `./gradlew :lib:test -PincludeTags=integration` (or whatever the project flag is).
Expected: all integration tests PASS.

- [ ] **Step 3: Run the slow tests once**

Run: `./gradlew :lib:test -PincludeTags=slow`
Expected: bounded-memory test PASS.

- [ ] **Step 4: Smoke-build the whole project**

Run: `./gradlew build -x test`
Expected: success.

- [ ] **Step 5: Final commit (only if anything changed)**

If steps above surfaced minor issues, fix and commit. Otherwise nothing to do.

---

**End of Phase 6 — and end of plan.** `fs_source` is the unified file-system source: local FS, S3, GCS; bounded + unbounded; deterministic self-partitioning across N jobs with restart and rebalance safety; bounded memory; the legacy `gcs_source` command is gone.

---

## Self-review notes

**Spec coverage check:**
- Distributed partitioning + exactly-once → Tasks 5 (Partitioner), 17 (loop), 22 (migration), 25 (cross-job determinism test).
- Bounded + unbounded modes → Tasks 17 + 18.
- Bounded memory → Tasks 11 (LineEmissionStrategy), 26 (2GB test).
- Pluggable backends → Tasks 4 (FsBackend SPI), 9 (LocalFs), 23 (S3), 24 (GCS).
- Local FS + S3 + GCS → Tasks 9, 23, 24.
- Atlas Air expressibility — `FileReferenceEmissionStrategy` (Task 10) plus future SFTP backend; spec §12.2 confirms deferred.
- Stability probe + post-actions + filename-timestamp → Tasks 13 (probe + post-actions), 17 (loop wires `tsFromName`).
- CheckpointStore SPI + InMem + ObjectStore + GenerationMigrator → Tasks 7, 8, 20, 21, 22.
- Default checkpoint derived from source backend → Task 22 step 1.
- `gcs_source` removed → Tasks 27 (parity), 28 (removal).

**Type-consistency check:**
- `PostAction.run` signature changes from `(file, backend)` to `(file, backend, backendConfig)` in Task 23 step 5 (when cloud backends need their config to perform actions). Existing tests (`SizeStableProbeTest`, etc.) call `PostAction.NO_OP` only, which is unaffected; the `FsSourceCommand` call site is updated in the same task. Anything else passing `PostAction.run` is in this plan — none in Tasks 1–22 use the parameters concretely.
- `CheckpointStore.listShards` returns keys of the form `<sourceId>/<gen>/<idx>.json` (matches the format `GenerationMigrator` expects).

**Known follow-ups (deferred to Phase 7+ outside this plan):**
- SFTP backend.
- Grid-backed `CheckpointStore` in zephflow-plus.
- `file_source` shim/removal.
- Metrics wiring (`fs_source.files.discovered` etc.). v1 plan provides hooks but full metric integration is small-and-mechanical, tracked outside this plan.

---

## Execution Handoff

**Plan complete and saved to `docs/superpowers/plans/2026-05-29-fs-source-framework.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints for review.

**Which approach?**
