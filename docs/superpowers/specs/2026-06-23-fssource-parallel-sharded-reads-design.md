# Design: Parallel sharded reads from a shared FS source

**Date:** 2026-06-23
**Status:** Approved (pending spec review)
**Repos:** `zephflow-core` (source command), `grid` (jobmaster fan-out)

## Problem

Today an `fssource` task cannot be scaled horizontally. If Grid runs N replicas of
the same task, every replica lists the same root and processes every file —
duplicating all reads N times. There is no mechanism for a replica to process only
the subset of files it is responsible for.

## Goal

Let Grid configure an `fssource` task with a replica count, and have each replica
independently process a disjoint subset of files such that:

- The union of all replicas' subsets is the full file set (no gaps).
- The subsets are pairwise disjoint (no duplicate reads).
- Assignment is stateless, deterministic, and requires no cross-replica
  coordination.
- Existing single-replica jobs are completely unaffected (no checkpoint
  invalidation, no behavior change).

## Approach

Each replica lists the **same** root but keeps only the files it owns, decided by a
stable hash of the file URN:

```
owns(urn) == ( floorMod( sha256(urn), replicaCount ) == replicaIndex )
```

Sharding is fully stateless: no jobmaster file assignment, no replica-to-replica
communication. Files in the FS source are immutable once written, so the URN is a
stable partition key.

Grid injects the replica identity into the DAG at fan-out time; zephflow-core reads
it and applies the shard filter.

## Contract: two new jobContext keys

Two new well-known keys in `jobContext.otherProperties`, following the existing
`CHECKPOINT_URL` convention:

| Key             | Value    | Meaning                              |
|-----------------|----------|--------------------------------------|
| `REPLICA_INDEX` | `0..N-1` | This replica's ordinal               |
| `REPLICA_COUNT` | `N`      | Total replicas for this task         |

- Defined as constants on `io.fleak.zephflow.api.JobContext` (zephflow-core).
- Grid hardcodes the matching string literals, exactly as it does for
  `CHECKPOINT_URL`. The binding between the two repos is the string literal; the
  Grid side carries a comment requiring the literal to match the `JobContext`
  constant.

## Grid side (jobmaster — inject at fan-out)

`JobService` already fans out replicas:

```java
for (var i = 0; i < replicas; i++) {
  // each iteration creates and persists one task with its own DAG copy
  createAndInitiateTask(jobTaskSubmit, job.getJobId());
}
```

`i` and `replicas` are both in scope, and each task persists its own DAG copy
(`Task.dag`, jsonb). This is the injection site.

### Shared injector (no schema/DTO changes)

`injectMetricTags` and `injectCheckpointUrl` in `TaskSupervisor` currently each
duplicate the same logic: locate-or-create `jobContext`, locate-or-create
`otherProperties` (or `metricTags`), then `put`. Replica injection would be a third
copy. Instead, extract one shared helper and route all three through it.

**`DagJobContextInjector`** — a new utility in the shared `:lib` module
(`io.fleak.grid.lib`), which both `jobmaster` and `workeragent` already depend on
(`implementation project(':lib')`). It owns the `ObjectNode`-walking:

```java
public final class DagJobContextInjector {
  private DagJobContextInjector() {}

  /** Locate-or-create jobContext.otherProperties on the DAG and put key=value. */
  public static void putOtherProperty(JsonNode dag, String key, String value) { ... }

  /** Locate-or-create jobContext.metricTags on the DAG and put key=value. */
  public static void putMetricTag(JsonNode dag, String key, String value) { ... }
}
```

Each method does the locate-or-create walk once; `TaskSupervisor` and `JobService`
call it instead of carrying their own copies.

### Two principled call sites

The two remaining injection sites are kept, but justified by a real distinction
rather than convenience:

- **Intrinsic task identity** — `REPLICA_INDEX` / `REPLICA_COUNT`. Injected into the
  *persisted* DAG at **fan-out** (`JobService`), because the replica index exists
  only there and is part of what the task *is*. The stored `Task.dag` then records
  which shard the task represents.
- **Runtime environment** — `worker_id` (metric tag) and `CHECKPOINT_URL`
  (`masterUrl`). Late-bound at the **worker** (`TaskSupervisor`) at launch, because
  those values exist only in the worker's runtime (`workerConfig`).

This is the persisted-identity vs. late-bound-runtime boundary, not an arbitrary
split.

### Changes

- `JobService` fan-out loop: call
  `DagJobContextInjector.putOtherProperty(dag, REPLICA_INDEX, ...)` and
  `putOtherProperty(dag, REPLICA_COUNT, ...)` on each per-task DAG, **before**
  `createAndInitiateTask` persists it. **Only when `replicas > 1`** — single-replica
  jobs get neither key and behave exactly as today.
- `TaskSupervisor.injectMetricTags` / `injectCheckpointUrl`: reimplemented to call
  the shared `DagJobContextInjector` instead of their inline walks. Behavior
  unchanged.

### What does NOT change

- No new column on the `Task` entity.
- No changes to `WorkerTaskSubmit` or any DTO.
- No DB migration.
- Worker-side injection behavior (`worker_id`, `CHECKPOINT_URL`) is byte-for-byte
  identical; only the implementation is refactored onto the shared helper.

## zephflow-core side

### 1. `Partitioner` utility — `fssource/util/Partitioner.java`

```java
public final class Partitioner {
  private Partitioner() {}

  /** True if the given file URN is owned by replicaIndex out of replicaCount. */
  public static boolean owns(String urn, int replicaIndex, int replicaCount) {
    if (replicaCount <= 1) return true;
    int bucket = Math.floorMod(hash(urn), replicaCount);
    return bucket == replicaIndex;
  }

  private static int hash(String urn) {
    // sha256 for distribution consistency with SourceIdHasher; reduce digest to an int.
    return Hashing.sha256().hashString(urn, StandardCharsets.UTF_8).asInt();
  }
}
```

- Stateless, deterministic.
- `Math.floorMod` (never `%`) so a negative hash never produces a negative bucket.
- `replicaCount <= 1` short-circuits to "owns everything".

### 2. `FsSourceCommand` reads the keys

In `createExecutionContext`, parse the keys from `otherProperties` the same way
`CHECKPOINT_URL` is read:

- `REPLICA_INDEX` → int, default `0`.
- `REPLICA_COUNT` → int, default `1`.
- Absent, blank, or unparseable values fall back to the defaults (`0 / 1`).
- Store `replicaIndex` and `replicaCount` on `FsSourceExecutionContext`.

### 3. Shard filter in `execute()`

Add one filter to the existing stream chain, alongside the watermark and
`isCompleted` filters:

```java
.filter(pending -> Partitioner.owns(
    pending.entry().key().urn(), replicaIndex, replicaCount))
```

The shard key is `fileEntry.key().urn()` — stable and immutable per file.

### 4. `SourceIdHasher` — replica-aware checkpoint key (back-compatible)

Each replica must checkpoint to an isolated key, or replicas would clobber each
other's watermark/completed-set state.

```java
// unchanged signature; canonical string identical to today
compute(backend, root, regex)

// new overload; folds index+count into the hash ONLY when count > 1
compute(backend, root, regex, replicaIndex, replicaCount)
```

- When `replicaCount <= 1`, the canonical string is byte-for-byte identical to the
  current implementation → existing single-replica checkpoints remain valid. No
  migration, no re-scan.
- When `replicaCount > 1`, each replica derives a distinct `sourceId`, so its
  checkpoint is isolated.
- Changing the replica count changes every replica's `sourceId`, which triggers a
  clean re-scan. This is correct: changing N also changes the shard assignment, so
  old per-shard checkpoints are no longer meaningful.

`FsSourceCommand` calls the new overload, passing the values read in step 2.

## Data flow

```
Job(replicas=3) ── JobService loop i = 0,1,2
   └─ per-task DAG copy + putOtherProperty(dag, REPLICA_INDEX=i, REPLICA_COUNT=3)
        └─ persisted (Task.dag) ── worker ── injectCheckpointUrl ── FsSourceCommand
             ├─ sourceId = hash(backend, root, regex, i, 3)        (isolated per replica)
             └─ list(root)
                  → filter timestamp >= watermark
                  → filter Partitioner.owns(urn, i, 3)
                  → filter !checkpoint.isCompleted(urn)
                  → read + emit + checkpoint
```

## Edge cases

| Case                          | Behavior                                                        |
|-------------------------------|-----------------------------------------------------------------|
| Keys missing / `count <= 1`   | Owns all files; legacy `sourceId`; identical to today.          |
| Negative hash value           | `Math.floorMod` yields a valid bucket in `[0, count)`.          |
| Unparseable key values        | Fall back to defaults `0 / 1`.                                  |
| Replica count change          | New `sourceId` per replica → clean re-scan; no stale shards.    |
| Coverage                      | Every URN maps to exactly one bucket → no gaps, no overlap.     |

## Testing

**zephflow-core**

- `PartitionerTest`:
  - Determinism (same urn + params → same result).
  - Coverage: over a fixed URN set across `count` replicas, the union of owned URNs
    equals the full set and the per-replica subsets are pairwise disjoint.
  - Even-ish distribution across buckets for a large URN sample.
  - `floorMod` correctness for URNs that hash to negative ints.
  - `count <= 1` → owns everything.
- `SourceIdHasherTest`:
  - `count <= 1` overload result equals the legacy 3-arg result (back-compat).
  - `count > 1` produces distinct ids per `replicaIndex`.
- `FsSourceCommand` test: 3 replicas over a fixed file set process disjoint
  subsets whose union is the whole set; each replica checkpoints to its own
  `sourceId`.

**grid**

- `DagJobContextInjectorTest`:
  - `putOtherProperty` / `putMetricTag` locate-or-create `jobContext` and the
    target map when absent, and preserve existing entries when present.
- `JobService` test:
  - `replicas > 1` injects both replica keys into each task's DAG with correct
    per-replica values.
  - `replicas == 1` injects neither key.
- `TaskSupervisor` tests (existing): unchanged behavior for `worker_id` /
  `CHECKPOINT_URL` after refactoring onto the shared injector.

## Out of scope

- Dynamic rescaling of a running job (changing N mid-run). Handled only via
  restart, which re-shards cleanly through the new `sourceId`.
- Rebalancing in-flight work when a replica dies. A dead replica's unprocessed
  shard is picked up only on the next run with the same N (its checkpoint is
  isolated and resumes from its own watermark).
- Range/locality-aware partitioning. Hash partitioning is chosen for even
  distribution and zero coordination.
