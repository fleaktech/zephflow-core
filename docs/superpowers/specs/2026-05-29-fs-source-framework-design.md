# File-System Source Framework — Design

**Status:** Approved (brainstorming phase). Pending implementation plan.
**Date:** 2026-05-29
**Owner:** Dan
**Target:** zephflow-core v1 (single-effort scope)

---

## 1. Problem

`FileSourceCommand` reads a single local file fully into JVM heap and emits one event. It cannot scan directories, cannot stream large files, and cannot reach S3, GCS, or SFTP. Meanwhile, the Atlas Air ingest pipeline in zephflow-plus solves a closely related problem as a one-off pair of commands (`AtlasAirSftpSourceCommand` + `AtlasAirZipProcessorCommand`). Every new file-backed source today re-implements some subset of: list a remote directory, filter by name, order by filename-timestamp, emit, checkpoint progress, scale across N parallel jobs without overlap.

We need a single file-system source framework that:

- Runs across N parallel jobs with **deterministic, exactly-once partitioning** under normal operation, restarts, and rebalances.
- Supports **bounded** (drain a snapshot) and **unbounded** (continuous discovery) modes.
- Has **bounded memory** — source heap usage does not scale with file size.
- Is **pluggable**: a new backend requires only a `FileLister` + `FileReader` implementation and a registry entry.
- Subsumes the existing `gcs_source` command (which is removed in this effort).
- Is shaped so the Atlas Air two-stage pattern (file-reference emission + downstream processor) can be expressed on top of it later, when an SFTP backend lands.

## 2. Goals & non-goals

### v1 goals

- Ship a new `fs_source` command in zephflow-core, registered alongside `file_source`.
- Ship three backends: **Local FS**, **S3**, **GCS**.
- Ship three emission strategies: **line-by-line**, **whole-file**, **file-reference**.
- Ship deterministic self-partitioning with `hash(fileKey) % N == jobIndex`.
- Ship bounded + unbounded modes from one loop body.
- Ship the `CheckpointStore` SPI with two implementations: `InMemoryCheckpointStore` (test/dev) and `ObjectStoreCheckpointStore` (production default; uses the same `FsBackend` the source reads from).
- Ship **generation migration**: when `N` changes, the new generation seeds its checkpoint from the prior generation's shards, slicing by the new partition function. No re-emission under planned parallelism changes.
- Ship optional **size-stability probe**, **post-actions** (none/delete/move), and **filename-timestamp ordering**.
- **Remove `gcs_source`.** Its capabilities are a strict subset of `fs_source` with `backend: gs`; existing pipelines migrate by rewriting config.

### v1 non-goals (deferred)

- SFTP backend.
- Grid-backed `CheckpointStore` implementation (lives in zephflow-plus, requires a new Grid endpoint).
- Atlas Air migration onto `fs_source`. Blocked on SFTP backend + Grid `CheckpointStore`.
- Removal of `file_source`. Stays registered.
- Byte-range partitioning within a single file.
- Central SplitEnumerator service / Flink-style RPC orchestration.
- HDFS, Azure Blob, FTP backends. Anyone can add them post-v1 via the SPI without touching the framework.

---

## 3. Architecture

### 3.1 Component layout

```
┌─────────────────────────────────────────┐
│           FsSourceCommand               │
│  (extends SourceCommand; owns the loop) │
└──────────────────┬──────────────────────┘
                   │
  ┌────────────────┼─────────────────┬────────────────────┐
  │                │                 │                    │
  ▼                ▼                 ▼                    ▼
┌──────────┐  ┌────────────┐   ┌────────────────┐   ┌────────────────┐
│FileLister│  │ FileReader │   │EmissionStrategy│   │ CheckpointStore│
│(backend) │  │ (backend)  │   │  (built-in 3)  │   │(InMem / ObjStr)│
└──────────┘  └────────────┘   └────────────────┘   └────────────────┘
       \         /                    │
        \       /                     ▼
       ┌──────────┐            SourceEventAcceptor
       │FsBackend │
       │ registry │
       └──────────┘
```

All v1 code lives under `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/`:

```
fssource/
├── FsSourceCommand.java
├── FsSourceCommandFactory.java
├── FsSourceConfig.java
├── FsSourceConfigValidator.java
├── api/
│   ├── FileLister.java
│   ├── FileReader.java
│   ├── FileEntry.java
│   ├── FileKey.java
│   ├── EmissionStrategy.java
│   ├── StabilityProbe.java
│   ├── PostAction.java
│   ├── FsBackend.java
│   └── FsBackendRegistry.java
├── checkpoint/
│   ├── CheckpointStore.java
│   ├── FsCheckpoint.java
│   ├── InMemoryCheckpointStore.java
│   ├── ObjectStoreCheckpointStore.java
│   └── GenerationMigrator.java
├── emission/
│   ├── LineEmissionStrategy.java
│   ├── WholeFileEmissionStrategy.java
│   └── FileReferenceEmissionStrategy.java
└── backend/
    ├── local/
    │   ├── LocalFsBackend.java
    │   ├── LocalFsLister.java
    │   └── LocalFsReader.java
    ├── s3/
    │   ├── S3Backend.java
    │   ├── S3Lister.java
    │   └── S3Reader.java
    └── gcs/
        ├── GcsBackend.java
        ├── GcsLister.java
        └── GcsReader.java
```

### 3.2 SPIs

```java
public record FileKey(String backend, String urn) {}
//   backend: "file" | "s3" | "gs"   (matches FsBackend.scheme())
//   urn:     stable identifier — e.g. "s3://bucket/key", "file:///abs/path"

public record FileEntry(FileKey key, long size, Instant lastModified, String displayPath) {}

public interface FileLister extends AutoCloseable {
    /** Lazy, paginated iteration over all matching files. Must not materialize the
     *  full set in memory. Backends drive pagination internally. */
    Stream<FileEntry> list(ListRequest req);

    /** Re-stat a single file. Used by StabilityProbe. */
    FileEntry stat(FileKey key);
}

public interface FileReader extends AutoCloseable {
    /** Open a streaming InputStream. offset=0 for full read. Caller closes. */
    InputStream open(FileKey key, long offset);
}

public interface EmissionStrategy {
    /** Stream the file's content (or just metadata for file_reference) to `out`.
     *  Must be bounded-memory: never load the full file into heap for line or
     *  file_reference strategies. whole_file is the explicit exception. */
    void emit(FileEntry file, FileReader reader, SourceEventAcceptor out, JobContext ctx);
}

public interface StabilityProbe {
    /** If true, file is safe to emit. Default impl: SizeStableProbe — compares
     *  (size, lastModified) across two probes separated by probeDelay. */
    boolean isStable(FileEntry first, FileLister lister);
}

public interface PostAction {
    /** Run after a file is fully emitted AND committed. Built-ins:
     *  NoOpPostAction, DeletePostAction, MoveToPostAction(prefix). */
    void run(FileEntry file, FileLister lister, FileReader reader);
}

public interface FsBackend {
    String scheme();                              // "file" | "s3" | "gs"
    FileLister createLister(FsBackendConfig cfg);
    FileReader createReader(FsBackendConfig cfg);
    Set<Capability> capabilities();               // DELETE, MOVE, RANGE_READ
}

public final class FsBackendRegistry {
    public static void register(FsBackend backend);
    public static FsBackend get(String scheme);   // throws if unknown
}
```

Checkpoint SPI:

```java
public record FsCheckpoint(
    int version,                              // 1 in v1
    Instant watermark,                        // EPOCH for "no watermark yet"
    Set<String> completedSinceWatermark       // file URNs
) {}

public interface CheckpointStore {
    Optional<FsCheckpoint> load(String checkpointKey);
    void save(String checkpointKey, FsCheckpoint cp);          // atomic, single-writer

    // Used by GenerationMigrator on parallelism change:
    List<Integer> listGenerations(String sourceId);            // e.g. [3, 5]
    List<String>  listShards(String sourceId, int generation); // e.g. all jobIndex keys
}
```

`OperatorCommandRegistry` gets one new entry:

```java
COMMAND_NAME_FS_SOURCE → new FsSourceCommandFactory()
```

The `COMMAND_NAME_GCS_SOURCE` entry is removed.

---

## 4. Per-job lifecycle

### 4.1 The loop

```
open():
  cfg          := parsed config
  jobIndex, N  := JobContext.get("zephflow.job.index", 0),
                  JobContext.get("zephflow.job.parallelism", 1)
  backend      := FsBackendRegistry.get(cfg.backend)
  lister       := backend.createLister(cfg.backendConfig)
  reader       := backend.createReader(cfg.backendConfig)
  emissionStr  := build(cfg.emission)
  stabProbe    := cfg.stability.enabled ? new SizeStableProbe(cfg.stability.probeDelay) : DISABLED
  postAction   := build(cfg.postAction)
  cpStore      := buildCheckpointStore(cfg, backend)
  sourceId     := stableHash(cfg)
  cpKey        := "<sourceId>/<N>/<jobIndex>.json"

  cp := cpStore.load(cpKey).orElseGet(() ->
          GenerationMigrator.maybeSeed(cpStore, sourceId, N, jobIndex))
  if cp == null: cp = FsCheckpoint.empty()

  inFlightSeen := new HashMap<FileKey, FileEntry>()   // for stability probe state

loop until terminated:
  candidates := lister.list(ListRequest.fromConfig(cfg))     // lazy stream
  mine       := candidates.filter(f -> floorMod(murmur3_128(f.key.urn()), N) == jobIndex)
  todo       := mine
                 .filter(f -> tsFromName(f) >= cp.watermark)
                 .filter(f -> !cp.completedSinceWatermark.contains(f.key.urn()))
                 .sortedBy(f -> (tsFromName(f), f.key.urn()))    // deterministic tiebreak

  emittedThisPass := 0
  for f in todo:
    if stabProbe.isStable(f, lister):
      try {
        emissionStr.emit(f, reader, acceptor, ctx)
        commit(cp, f, cpStore, cfg.commitStrategy)
        postAction.run(f, lister, reader)
        emittedThisPass++
      } catch (Exception e) {
        dlqWriter.write(f, e)
        // Do NOT advance checkpoint past f if DLQ write also failed.
      }
    // else: leave for next pass

  if cfg.mode == BOUNDED && emittedThisPass == 0 && todo.isEmpty():
    flush(cp); acceptor.terminate(); break
  if cfg.mode == UNBOUNDED && emittedThisPass == 0:
    backoff.sleep()      // 100ms → 30s cap, exponential

close():
  flush(cp); close(lister); close(reader)
```

### 4.2 Partition function

`Math.floorMod(Hashing.murmur3_128().hashString(fileKey.urn(), UTF_8).asLong(), N)`.

- Murmur3 is on the classpath via Guava.
- `floorMod` (not `%`) is mandatory for correctness on negative hash values.
- The function is pure on `fileKey.urn()`. The same file always lands on the same job under fixed `N`.

### 4.3 Source identity (`sourceId`)

`sourceId` is a stable hash of the config fields that materially affect what files this source sees:

- `backend`
- `root` / `bucket` / `prefix`
- `fileNameRegex`

It is **NOT** affected by `partition`, `checkpoint`, `emission.lineBatchSize`, `listingInterval`, or `mode`. Two sources that read the same files share a `sourceId` and therefore share checkpoints — which is the desired property under restart.

Implementation: `Hashing.sha256().hashString(canonicalJson(sourceFields), UTF_8).toString().substring(0, 16)`.

### 4.4 Ordering rule

Files are emitted in `(tsFromName(file), file.key.urn())` order within a job's slice. Filename-timestamp comes from a named-group regex in config (e.g. `'invoice_(?<ts>\d+)\.json'`). If no `ts` group is configured, ordering falls back to `(lastModified, key.urn())`.

### 4.5 Watermark advancement

The loop emits files in `(tsFromName, urn)` ascending order within each pass (§4.4). This invariant makes watermark advancement simple:

After each successful emit+commit of file `f` with timestamp `t = tsFromName(f)`:

1. Add `f.key.urn()` to `cp.completedSinceWatermark`.
2. If `t > cp.watermark`, set `cp.watermark = t` and prune `cp.completedSinceWatermark` of entries whose timestamp is `< t`.

Because files are emitted in ascending-ts order within a pass, by the time `f@t` is committed, every previously-emitted file in the slice has timestamp `<= t` and is already in `completedSinceWatermark` (or has been pruned because watermark moved past it). The pruned set is bounded by "files at the current watermark boundary" — typically small.

**Late files below the watermark:** in unbounded mode, a file that appears in a later listing pass with `tsFromName(f) < cp.watermark` is filtered out by step 4.1's `tsFromName(f) >= cp.watermark` check and counted in `fs_source.files.skipped_below_watermark`. This is intentional: out-of-order arrival is treated as "missed the window." Operators who need strict capture of all files use `mode: bounded` for that snapshot.

**In-memory vs persisted watermark:** the rule above updates the in-memory `cp`. Persistence to `CheckpointStore` is governed by `commitStrategy` (per-record / batch / time). Between persistence points, watermark advances are at-risk on crash → at-least-once on the trailing batch, as already stated in §5.1.

### 4.6 Stability probe

Default `SizeStableProbe(Duration probeDelay)`:

```
isStable(file, lister):
  prior := inFlightSeen.get(file.key)
  if prior == null:
    inFlightSeen.put(file.key, file); return false
  if (now() - prior.lastModified) < probeDelay:
    return false
  current := lister.stat(file.key)
  if current.size == prior.size && current.lastModified == prior.lastModified:
    inFlightSeen.remove(file.key); return true
  inFlightSeen.put(file.key, current); return false
```

Disabled by default. Opt-in via config. Backend-agnostic — works for any backend whose `stat` returns size and lastModified.

### 4.7 Bounded vs unbounded

| Aspect | Bounded | Unbounded |
|---|---|---|
| Listing | One pass | Re-list every `listingInterval` (default 30s) |
| Exit | After first pass with `emittedThisPass == 0 && todo.isEmpty()` | Only on external `terminate()` |
| Stability probe | Behavior unchanged but rarely useful — bounded snapshots are taken at rest | Primary use case |
| Watermark | Advances normally; final state persisted before exit | Advances continuously |
| Retry budget on backend errors | 5 attempts then fail loudly | Unlimited; exponential backoff |

Both modes share **the same loop body** above; the only difference is the exit condition.

---

## 5. Distributed partitioning & exactly-once

### 5.1 Properties

The framework provides **"every file processed exactly once" under normal operation** through three independent guarantees:

1. **No overlap.** `floorMod(murmur3_128(urn), N) == jobIndex` is a partition function; two jobs cannot both think they own the same file. Eliminates the need for leases.
2. **No skip under restart.** Per-job checkpoints persist `(watermark, completedSinceWatermark)`. On restart, anything not in that set is re-considered. Worst case: at-least-once on the trailing commit interval.
3. **No skip under planned rebalance.** `GenerationMigrator` (§6.3) seeds a new generation's checkpoints from the prior generation's union.

### 5.2 `(jobIndex, N)` is externally supplied

The framework reads `zephflow.job.index` and `zephflow.job.parallelism` from `JobContext`. It does **not** elect, discover, or negotiate them. The orchestrator (Kubernetes StatefulSet ordinal, Nomad job index, the Fleak control plane) is the source of truth. Defaults: `(0, 1)`.

### 5.3 Failure modes (called out so they're not surprises)

| Scenario | Behavior |
|---|---|
| Single job dies | Its slice pauses until restart. Other jobs unaffected. |
| All jobs restart simultaneously | Each loads its own checkpoint; identical to one-at-a-time restarts. |
| Mismatched `N` (orchestrator misconfig) | New job reads checkpoint at the new `(N, jobIndex)` key, which doesn't exist → starts from empty. Logged at INFO. **Will cause double-processing.** |
| Same `(jobIndex, N)` launched twice | Both jobs write to the same checkpoint key; last-writer-wins; double-emission on overlap. **Operator-prevented, not framework-prevented.** Same property as Flink. |
| `CheckpointStore.save()` fails | Backoff and retry; if commits stall, the loop pauses emission. Metric: `fs_source.checkpoint.failures`. |

---

## 6. Checkpoint store

### 6.1 Defaults derived from source

The checkpoint store is **not a separate configuration in the common case**. It is derived from the source's `FsBackend`:

| Source backend | Default checkpoint location |
|---|---|
| `file` | `<root>/_zephflow_checkpoints/` (sibling directory on local disk) |
| `s3` | `s3://<source-bucket>/_zephflow_checkpoints/` |
| `gs` | `gs://<source-bucket>/_zephflow_checkpoints/` |

An optional `checkpoint:` block in the config overrides this for two cases:
- Read-only source bucket (job has list+get but not put).
- Multi-node deployment reading from local FS where each host's disk is not shared.

### 6.2 Key layout & payload

Key: `<checkpointPrefix>/<sourceId>/<N>/<jobIndex>.json`

```
_zephflow_checkpoints/
└── a1b2c3d4e5f6/          # sourceId (16-char SHA-256 prefix)
    ├── 3/                  # generation = parallelism
    │   ├── 0.json
    │   ├── 1.json
    │   └── 2.json
    └── 5/                  # newer generation after rebalance
        ├── 0.json
        ...
```

Payload:

```json
{
  "version": 1,
  "watermark": "2026-05-26T14:33:00Z",
  "completedSinceWatermark": [
    "s3://bkt/path/file-20260526T143300Z.json"
  ]
}
```

### 6.3 Atomicity

- **Object stores (S3, GCS):** `PUT` is atomic at the object level. Readers see either the prior generation or the new one, never a torn write. No multi-part, no temp-and-rename. No conditional writes needed because each shard has a single writer (the owning job).
- **Local FS:** write to a temp file in the same directory, `Files.move(temp, target, ATOMIC_MOVE)`. Atomic on POSIX.
- **In-memory:** trivial.

### 6.4 Generation migration

When `N` changes, the new generation's checkpoint key doesn't exist. `GenerationMigrator.maybeSeed()` runs at `open()`:

```
maybeSeed(store, sourceId, N, jobIndex):
  current := store.load("<sourceId>/<N>/<jobIndex>.json")
  if current.present(): return current

  generations := store.listGenerations(sourceId)
                       .filter(g -> g != N)
                       .sortedDesc()
  for prevN in generations:
    shards := store.listShards(sourceId, prevN)
    if shards.isEmpty(): continue

    merged := shards.map(store::load).reduce(emptyCheckpoint, (a, b) -> {
      watermark = min(a.wm, b.wm),
      completedSinceWatermark = a.completed ∪ b.completed
    })

    // Keep only files this new-N job now owns.
    seeded := merged.copy().filterCompleted(urn ->
                  floorMod(murmur3_128(urn), N) == jobIndex)
    store.save("<sourceId>/<N>/<jobIndex>.json", seeded)
    return Optional.of(seeded)

  return Optional.empty()    // truly fresh source
```

Properties:
- Each new-N job runs independently and writes only to its own key. No coordination.
- `min(watermark)` is the safe choice: we cannot advance past any prior shard's watermark without re-checking files between them.
- Union of `completedSinceWatermark` from all old shards is filtered to this job's slice. The total memory cost is bounded by the boundary-set across old shards — small for well-behaved sources.

Edge cases:
- **Old-N jobs still running while new-N jobs migrate** — operator-prevented. Orchestrator must guarantee old generation is shut down before new generation starts.
- **Multiple old generations present** (3 → 5 → 7 chain) — `sortedDesc()` picks the newest old generation. Older ones are effectively garbage; a future cleanup utility may prune them.
- **Stuck watermark in old generation** producing a huge `completedSinceWatermark` — degenerate case; would have been a problem in steady state too. Not addressed here.

---

## 7. Backends (v1)

Each backend ships two implementation classes plus a registry entry. None of them know about partitioning, ordering, stability, watermarks, or emission shape — all of that lives in `FsSourceCommand`.

### 7.1 `LocalFsBackend` (`scheme = "file"`)

- `LocalFsLister`: `Files.find(root, maxDepth, matcher)`. Lazy stream — driven by `Files.walk`. Regex filter applied per-entry. `stat` = `Files.readAttributes`.
- `LocalFsReader`: `Files.newInputStream(path)`. Offset via `FileChannel.position`.
- Capabilities: `DELETE`, `MOVE`, `RANGE_READ`.

### 7.2 `S3Backend` (`scheme = "s3"`)

- `S3Lister`: `ListObjectsV2` paginated. Returns lazy `Stream<FileEntry>` that paginates as it is consumed. Regex filter applied client-side after listing (no list-prefix on regex).
- `S3Reader`: `GetObject` with `Range` header derived from offset when offset > 0. Returns the SDK's `ResponseInputStream<GetObjectResponse>` directly — no buffering, no full read. Caller is responsible for `close()`.
- `stat` = `HeadObject`.
- Capabilities: `DELETE` (via `DeleteObject`), `MOVE` (via `CopyObject` + `DeleteObject`), `RANGE_READ`.

### 7.3 `GcsBackend` (`scheme = "gs"`)

- `GcsLister`: `Storage.list` with pagination. Lazy via `Page.iterateAll()`.
- `GcsReader`: `Storage.reader(blob)` returning a `ReadChannel`; wrapped as `InputStream`. Seek via `ReadChannel.seek(offset)`.
- `stat` = `Storage.get(blobId)`.
- Capabilities: `DELETE`, `MOVE` (via `copy` + `delete`), `RANGE_READ`.
- Reuses existing `GcsClientFactory`.

### 7.4 Credentials

Each backend's `FsBackendConfig` is a typed sub-record. The existing credential plumbing (e.g. `S3SinkCommand`'s region/credentials handling, `GcsClientFactory`'s auth path) is reused. No new credential infrastructure.

---

## 8. Emission strategies

```java
public interface EmissionStrategy {
  void emit(FileEntry file, FileReader reader, SourceEventAcceptor out, JobContext ctx);
}
```

| Strategy | Output per file | Memory profile |
|---|---|---|
| `LineEmissionStrategy(encoding, delimiter, batchSize)` | One `RecordFleakData` per line. `BufferedReader` over reader stream; emits in batches. | O(`batchSize × maxLineLen`). **Independent of file size.** |
| `WholeFileEmissionStrategy(encoding)` | One `RecordFleakData` per file with decoded contents. Uses `RawDataConverter`. | O(`fileSize`). The explicit exception to the bounded-memory promise; validator warns if no `maxFileSizeBytes` set. |
| `FileReferenceEmissionStrategy` | One `RecordFleakData` per file containing `{file, ts, size, lastModified}`. Reader stream never opened. | O(1) **per file.** This is the Atlas Air shape. |

Emission strategies are backend-agnostic. Any backend × any strategy is valid. `FileReferenceEmissionStrategy × SftpBackend` (post-v1) reproduces `AtlasAirSftpSourceCommand` exactly.

---

## 9. Full config shape

```yaml
fs_source:
  backend: s3                                # file | s3 | gs
  root: s3://my-bucket/data/
  fileNameRegex: 'invoice_(?<ts>\d+)\.json'  # ts group enables timestamp ordering & watermarking
  emission:
    type: line                               # line | whole_file | file_reference
    encoding: utf-8                          # line / whole_file only
    lineBatchSize: 500                       # line only
  mode: unbounded                            # bounded | unbounded
  listingInterval: 30s                       # unbounded only
  stability:
    enabled: false                           # opt-in
    probeDelay: 10s
  postAction: none                           # none | delete | archive:<urn-prefix>
  partition:                                 # optional override; usually JobContext supplies
    index: 0
    parallelism: 1
  # checkpoint:                              # optional override; default = derive from source
  #   backend: s3
  #   root: s3://ops-bucket/zephflow-cp/
  commitStrategy:                            # reused from existing CommitStrategy
    type: batch
    batchSize: 100
    intervalMs: 5000
```

Validation rules (enforced by `FsSourceConfigValidator`):

- `backend` must be a registered scheme.
- `root` URN must match the backend scheme.
- `fileNameRegex` must compile; if it contains a `(?<ts>...)` group, the group must capture a parseable Long timestamp.
- `mode == bounded` is incompatible with `listingInterval`.
- `emission.type == whole_file` without `maxFileSizeBytes` produces a WARN-level validation message.
- `postAction == delete | archive:...` requires the backend to advertise the `DELETE` / `MOVE` capability.
- `checkpoint.backend`, if set, must be a registered scheme.

---

## 10. Errors, DLQ, observability

### 10.1 Errors

- **Per-file errors** (read failure, decode error, emission failure): file goes to existing `DlqWriter`. Loop continues. Checkpoint is *not* advanced past the bad file unless the DLQ write succeeds — preserves at-least-once.
- **Backend errors** (auth, network, throttling): exponential backoff at the listing layer (100ms → 30s cap). After retry budget (unlimited unbounded; 5 attempts bounded) the command fails loudly via the existing error-propagation path.
- **Validation errors**: surfaced at `parseAndValidateArg` time, same as today.

### 10.2 Metrics (per existing `MetricClient` infra)

| Metric | Type | Tags |
|---|---|---|
| `fs_source.files.discovered` | counter | backend, sourceId |
| `fs_source.files.emitted` | counter | backend, sourceId, emission |
| `fs_source.files.skipped_partition` | counter | backend, sourceId |
| `fs_source.files.skipped_stability` | counter | backend, sourceId |
| `fs_source.files.skipped_below_watermark` | counter | backend, sourceId |
| `fs_source.files.dlq` | counter | backend, sourceId, errorClass |
| `fs_source.checkpoint.advances` | counter | backend, sourceId |
| `fs_source.checkpoint.failures` | counter | backend, sourceId |
| `fs_source.list.duration` | histogram | backend |
| `fs_source.emit.duration` | histogram | backend, emission |

### 10.3 Logging

- INFO: one line per checkpoint advance, one per command start (with resolved `(jobIndex, N, sourceId)`), one per generation migration.
- WARN: one per stability skip past a threshold, one per backend retry storm, one per DLQ write.
- DEBUG: per-file emission, per-listing-page-fetch.

---

## 11. Testing

### 11.1 Unit tests

- Loop behavior driven against `FakeFileLister` + `FakeFileReader`.
- Partition math: assert `floorMod(murmur3_128, N)` distribution properties on synthetic URNs; assert that for a fixed set, the union of slices over `0..N-1` equals the full set and pairwise intersections are empty.
- Ordering: assert deterministic tiebreak by URN under shared timestamps.
- Watermark: assert advancement only when all `tsFromName <= t` in the slice are completed.
- Stability probe: assert it skips on size/mtime changes and emits on stability.
- Restart resumption: persist a checkpoint, restart, assert no re-emission of completed files.
- Generation migration: seed three `(N=3)` shards, start two `(N=2)` jobs against the same `CheckpointStore`, assert (a) each new-N job's seed contains only its own slice and (b) zero re-emission of files in the union of completed sets.

### 11.2 Backend integration tests

- **Local FS**: temp directories. Existing pattern in the repo.
- **S3**: LocalStack via Testcontainers. New `lib-its/` module if S3 tests aren't already housed there.
- **GCS**: `google-cloud-storage` library's local fake (same pattern as existing GCS source tests).

### 11.3 Cross-job determinism test

In-process harness instantiates three `FsSourceCommand` instances with `(0,3), (1,3), (2,3)` against a shared `FakeBackend` containing 1000 synthetic files. Assert:
- Total emitted = 1000.
- Pairwise emitted sets are disjoint.
- Per-job emission is in timestamp order.

### 11.4 Bounded-memory test

`@Tag("slow")` integration test: synthesize a 2 GB single file, stream through `LineEmissionStrategy` under `-Xmx256m`, assert no OOM and correct line count. Counterpart smoke for `FileReferenceEmissionStrategy` on the same file (constant memory regardless of size).

### 11.5 Parity test for `gcs_source` removal

Before deletion, an integration test fixture replays the scenarios covered by the existing `GcsSourceCommandTest` against `fs_source` (`backend: gs`, `emission: whole_file`, `partition: (0,1)`, `mode: bounded`) and asserts equivalent event output. This is the gate on the `gcs_source` deletion landing in the same effort.

---

## 12. Migration plan

### 12.1 v1 — landed in this effort

| Command | Action | Notes |
|---|---|---|
| `fs_source` | **New.** Registered in `OperatorCommandRegistry`. | Local FS, S3, GCS backends. Line, whole-file, file-reference emission. |
| `gcs_source` | **Removed.** Command, factory, fetcher, config, validator, tests deleted. Registry entry removed. | Gated on §11.5 parity test green. Release notes call this out as a breaking change with a one-line config migration. |
| `file_source` | **Unchanged.** Stays registered. | Not in scope for removal this round. |

Config rewrite reference (release notes):

```yaml
# Before — REMOVED in v1
gcs_source:
  bucket: my-bucket
  prefix: data/
  encodingType: JSON

# After
fs_source:
  backend: gs
  root: gs://my-bucket/data/
  emission: { type: whole_file, encoding: JSON }
  mode: bounded
  partition: { index: 0, parallelism: 1 }
```

### 12.2 Deferred to v1.1+

- **SFTP backend** (`SftpBackend`, `SftpLister`, `SftpReader`). Re-uses existing `SftpUtil` in zephflow-plus.
- **Grid-backed `CheckpointStore`** in zephflow-plus, wrapping `CheckpointClient`. Requires a new Grid HTTP endpoint that supports prefix listing (`listGenerations`, `listShards`).
- **Atlas Air migration**: `AtlasAirSftpSourceCommand` → delegating shim over `fs_source { backend: sftp, emission: file_reference, stability: enabled, checkpoint: grid }`. `AtlasAirZipProcessorCommand` is unchanged — it is a downstream scalar command, not a source. Blocked on the two items above.
- **`file_source` removal**. Replace with a delegating shim or delete outright once no pipelines reference it.

---

## 13. Open questions / explicitly deferred

- Byte-range partitioning within a single file (parallel read of one giant file). Out of scope; requires either format-aware splitting or sacrificing line boundaries.
- Central `SplitEnumerator` service. Not adopted because deterministic self-partitioning is sufficient under the orchestration we have.
- HDFS / Azure Blob / FTP backends. SPI permits them without framework changes; not in v1 scope.
- Cleanup of stale old generations under `CheckpointStore`. Today, old generations sit in the store until manually deleted. Operator concern, not a correctness concern. A cleanup utility may land later.
- Behavior of `tsFromName` parse failures: v1 logs WARN and falls back to `lastModified` for that file. Open to tightening (drop file, send to DLQ) if operator feedback says we should be stricter.
