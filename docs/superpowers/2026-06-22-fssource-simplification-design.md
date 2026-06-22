# Spec: simplify `fssource` — single bounded, encoding-driven, resume-capable scan

**Date:** 2026-06-22
**Status:** Ready for review
**Repo:** `zephflow-core`
**Audience:** the engineer/agent implementing the change. Assumes familiarity with how zephflow source commands are structured.

---

## 1. Problem / motivation

The Abstract FS source (`fssource`) has accreted three subsystems that we no longer want:

1. **Two modes** — `BOUNDED` (batch, terminates) and `UNBOUNDED` (streaming, infinite poll loop with
   exponential backoff). We only want **bounded**.
2. **A bespoke content model** (`EmissionType`: `LINE` / `WHOLE_FILE` / `FILE_REFERENCE`) with its own charset
   and line-batching config. Every *other* source in the codebase consumes data through the shared
   `EncodingType` → `DeserializerFactory` → `FleakDeserializer` pipeline (`sqssource`, `kinesissource`, …).
   `fssource` should consume data the same way.
3. **Consumer-side parallelism / partitioning** — `Partitioner` hashes each file's URN to a `jobIndex` out of
   `parallelism`, the checkpoint key embeds `parallelism/jobIndex`, and `GenerationMigrator` migrates
   checkpoints when parallelism changes. We want a single consumer, no partitioning.

Separately, checkpointing today writes JSON files into the object store alongside the data
(`ObjectStoreCheckpointStore`). We are replacing that with an HTTP-backed `CheckpointClient` (the same contract
already used in zephflow-plus), so run state is reported to / recovered from a configurable external endpoint.

The result is a much smaller command: **list once, read+decode each file via `encodingType`, emit bare records,
checkpoint progress over HTTP, terminate.**

---

## 2. Goals & non-goals

### Goals
- `fssource` is always a **bounded batch source** (`SourceType.BATCH`). One listing pass, then `terminate()`.
- Content is consumed via the standard **`EncodingType`** mechanism: `JSON_OBJECT_LINE`, `JSON_OBJECT`,
  `JSON_ARRAY`, `STRING_LINE`, `TEXT`, `CSV`, `XML` — emitting **bare** deserialized records (no envelope).
- **Automatic decompression** by content sniffing (gzip today), transparent to the user.
- **Resume-capable checkpointing** via a ported `CheckpointClient` (in-mem + HTTP), keyed by `sourceId`, with a
  fully configurable URL sourced from `JobContext`. Absent URL → in-memory (non-durable) checkpointing.
- Remove parallelism/partitioning, stability probing, and post-actions.

### Non-goals
- Line-by-line / bounded-memory streaming of very large files. We now **buffer each file whole** in memory
  (consistent with how the shared deserializers and other sources operate on a full payload). The previous 2 GB
  streaming guarantee is intentionally dropped.
- Re-partitioning / multi-consumer coordination. Single consumer only.
- Stability detection (reading files mid-write) and post-actions (delete/archive). Removed.
- Charset configuration. Standard deserializers handle bytes (UTF-8); the old `encoding` field is removed.
- Adding new compression formats. Core supports `GZIP` only today; detection is extensible but we wire gzip only.

---

## 3. Target behavior

### 3.1 Config (`FsSourceDto.Config`)

**Keep:** `backend`, `root`, `fileNameRegex`, `backendConfig`.

**Add:** `encodingType` (`io.fleak.zephflow.lib.serdes.EncodingType`), **required**.

**Remove (fields):** `mode`, `emission`, `listingIntervalMs`, `stability`, `postAction`, `partition`.

**Remove (nested types / enums):** `Mode`, `EmissionType`, `Emission`, `Stability`, `PostActionType`,
`PostActionConfig`, `PartitionConfig`.

Resulting config:

```java
class Config implements CommandConfig {
  private String backend;                 // "file" | "s3" | "gs" | "azblob"
  private String root;
  private String fileNameRegex;           // optional; named group "ts" still honored for ordering
  private EncodingType encodingType;      // required
  private Map<String, Object> backendConfig;
}
```

Example:

```json
{
  "backend": "s3",
  "root": "s3://my-bucket/logs/",
  "fileNameRegex": "evt_(?<ts>\\d+)\\.jsonl",
  "encodingType": "JSON_OBJECT_LINE",
  "backendConfig": { "region": "us-east-1", "credentialId": "my-cred" }
}
```

### 3.2 Validator (`FsSourceConfigValidator`)

- `backend` non-blank.
- `root` non-blank.
- `fileNameRegex`, if present, compiles.
- `encodingType` non-null **and** `DeserializerFactory.validateEncodingType(encodingType)` passes (rejects
  `PARQUET`, which is sink-only).
- Drop all `emission` / `partition` / `postAction` validation.

### 3.3 Content consumption

For each selected file:

1. Read the **whole** object into `byte[]` (`reader.open(key, 0)` → `readAllBytes()`).
2. **Auto-decompress:** sniff magic bytes. If the buffer begins with the gzip magic (`0x1f 0x8b`), gunzip via
   `io.fleak.zephflow.lib.utils.CompressionUtils.gunzip(bytes)` (equivalently `GzipDecompressor`). Otherwise pass
   through. No config field; extensible to other formats later.
3. Deserialize:
   `DeserializerFactory.createDeserializerFactory(encodingType).createDeserializer()
      .deserialize(new SerializedEvent(null, bytes, null))` → `List<RecordFleakData>`.
4. Emit those records **as-is** (bare — no `file`/`line`/`content` fields added). `out.accept(records)`.

The deserializer (built once per `execute`) determines fan-out: `JSON_OBJECT_LINE`/`STRING_LINE`/`TEXT`/`CSV`
→ N records; `JSON_OBJECT` → 1; `JSON_ARRAY` → N; `XML` → per its deserializer.

### 3.4 Execution (`FsSourceCommand.execute`)

Single pass, no polling, no backoff, no partition filtering:

```
sourceId   = SourceIdHasher.compute(backend, root, fileNameRegex)
checkpoint = loadCheckpoint(sourceId)            // via CheckpointClient; empty if none
regex      = compile(fileNameRegex)
deser      = DeserializerFactory.createDeserializerFactory(encodingType).createDeserializer()

list once (try-with-resources over lister.list(req)):
  sort entries by (ts, urn)                       // ts from tsFromName(): named "ts" group or lastModified
  for each entry:
    if ts < checkpoint.watermark            -> skip
    if checkpoint.isCompleted(urn)          -> skip
    bytes   = readAllBytes(entry)
    bytes   = maybeGunzip(bytes)
    records = deser.deserialize(new SerializedEvent(null, bytes, null))
    out.accept(records)
    checkpoint = checkpoint.withEmitted(urn, ts)
    saveCheckpoint(sourceId, checkpoint)          // CheckpointClient.checkpoint(id, json)

out.terminate()
```

- `sourceType()` returns `SourceType.BATCH` unconditionally.
- `terminate()` flag handling stays (cooperative early exit mid-loop).
- Per-file read/decode exceptions: keep current behavior of the source loop (a failing file must not silently
  corrupt the watermark). At minimum, do **not** mark a file completed if its read/deserialize throws; log and
  continue. (Match existing error-handling conventions in sibling source commands.)

### 3.5 Checkpointing — `CheckpointClient`

Port the contract from
`zephflow-plus/.../commands/utils/CheckpointClient.java` into **zephflow-core**, with two changes:

- **No "Grid" naming and no hardcoded path.** Provide `CheckpointClient.InMemCheckpointClient` and
  `CheckpointClient.HttpCheckpointClient`. The HTTP client takes a **fully configurable base URL** and targets
  `{configuredUrl}/{id}` for both `POST` (checkpoint) and `GET` (load) — **not** `{baseUrl}/api/v1/state/{id}`.
- Interface unchanged in spirit:
  ```java
  interface CheckpointClient {
    void checkpoint(String id, String data);
    Optional<CheckpointData> loadCheckpoint(String id);
    record CheckpointData(long ts, String data) {}
  }
  ```

**State payload.** Reuse the existing `FsCheckpoint` record (`{version, watermark, completedSinceWatermark}`) as
the checkpoint *content*: serialize to JSON for the `data` string on save; parse back on load
(`CheckpointData.data` → `FsCheckpoint`; empty/absent → `FsCheckpoint.empty()`). `withEmitted` already compacts
the completed-set against the watermark, keeping the payload bounded across resumes.

**`id`.** The self-contained `sourceId` = `SourceIdHasher.compute(backend, root, fileNameRegex)`. No
`parallelism`/`jobIndex` component.

**URL source.** Read the base URL from `JobContext.getOtherProperties()` under a new core constant
`JobContext.JOB_MASTER_URL` (`"JOB_MASTER_URL"`). The grid populates it. When blank/absent →
`InMemCheckpointClient` (checkpointing is process-local and non-durable; a fresh process re-emits everything).
Build the client once in `createExecutionContext`.

### 3.6 Execution context

`FsSourceExecutionContext` swaps its `CheckpointStore checkpointStore` field for `CheckpointClient
checkpointClient`. `HttpCheckpointClient` holds a `java.net.http.HttpClient`; closing the context releases it if
applicable (InMem is a no-op). Keep `backend`, `backendConfig`, `lister`, `reader`.

---

## 4. Files

### Modify
| File | Change |
|---|---|
| `commands/fssource/FsSourceDto.java` | Strip removed fields/enums; add `encodingType`. |
| `commands/fssource/FsSourceConfigValidator.java` | Drop emission/partition/postAction checks; add `encodingType` required + `validateEncodingType`. |
| `commands/fssource/FsSourceCommand.java` | `sourceType()`→`BATCH`; rewrite `execute()` to the §3.4 single-pass loop; build deserializer + checkpoint client; remove `resolveParallelism`/`resolveJobIndex`, emission/stability/postAction builders, bounded/unbounded branching. Keep `tsFromName`. |
| `commands/fssource/FsSourceExecutionContext.java` | Replace `checkpointStore` with `checkpointClient`. |
| `api/src/main/java/io/fleak/zephflow/api/JobContext.java` | Add `public static final String JOB_MASTER_URL = "JOB_MASTER_URL";`. |

### Add
| File | Responsibility |
|---|---|
| `commands/fssource/checkpoint/CheckpointClient.java` | Ported contract: interface + `InMemCheckpointClient` + `HttpCheckpointClient` (configurable URL, `{url}/{id}`). |

### Delete
- `commands/fssource/emission/` — `LineEmissionStrategy`, `WholeFileEmissionStrategy`, `FileReferenceEmissionStrategy`.
- `commands/fssource/api/EmissionStrategy.java`.
- `commands/fssource/api/StabilityProbe.java`, `commands/fssource/api/SizeStableProbe.java`.
- `commands/fssource/api/PostAction.java`, `commands/fssource/api/PostActions.java`.
- `commands/fssource/util/Partitioner.java`.
- `commands/fssource/checkpoint/CheckpointStore.java`, `ObjectStoreCheckpointStore.java`,
  `InMemoryCheckpointStore.java`, `GenerationMigrator.java`.
- Keep `commands/fssource/checkpoint/FsCheckpoint.java` (now the JSON payload model) and
  `commands/fssource/util/SourceIdHasher.java`.

> Before deleting `PostActions`/`Partitioner`/`StabilityProbe`/the checkpoint-store classes, grep for references
> outside `fssource` (e.g. zephflow-plus) to confirm nothing else depends on them. If something does, surface it
> rather than breaking it.

---

## 5. Testing

### Remove
- `FsSourceCommandUnboundedTest` (unbounded mode gone).
- `BoundedMemoryStreamingTest` (whole-file buffering; 2 GB streaming no longer a guarantee).
- `CrossJobDeterminismTest` (parallelism gone).
- `FsSourceCommandMigrationTest` (`GenerationMigrator` gone).

### Update / add
1. **Per-encoding deserialization** (LocalFs backend, temp dir): one test each for `JSON_OBJECT_LINE`,
   `JSON_OBJECT`, `JSON_ARRAY`, `TEXT`/`STRING_LINE`, `CSV`, `XML` — assert bare records, correct fan-out, no
   `file`/`line` fields.
2. **Auto-decompression** — gzip a `JSON_OBJECT_LINE` file; assert it's transparently gunzipped and decoded.
   Negative control: a non-gzip file with a `.gz` name still parses (detection is by magic bytes, not extension).
3. **Single-pass termination** — directory with N files → all emitted once, then `terminate()`; no re-listing.
4. **Ordering & regex `ts`** — files ordered by named `ts` group, falling back to `lastModified`.
5. **Resume via checkpoint** (`InMemCheckpointClient`) — run once over 3 files, pre-seed checkpoint marking one
   completed (and/or run twice) → completed files skipped, watermark honored.
6. **HttpCheckpointClient** — stand up a lightweight in-process HTTP stub (e.g. `com.sun.net.httpserver` or the
   existing test HTTP utilities) at a base URL; assert `POST {url}/{id}` on save and `GET {url}/{id}` on load,
   and that supplying the URL via `JobContext.JOB_MASTER_URL` selects the HTTP client (blank → InMem).
7. **Validator** — blank `backend`, blank `root`, null `encodingType`, `PARQUET` encodingType, invalid
   `fileNameRegex` all rejected.
8. **`sourceType()`** returns `BATCH`.

Run: `./gradlew :lib:compileJava :lib:test`; `spotlessApply` clean.

---

## 6. Acceptance criteria

- [ ] `fssource` config no longer accepts `mode`/`emission`/`partition`/`postAction`/`stability`/`listingIntervalMs`;
      accepts and requires `encodingType`.
- [ ] `sourceType()` is always `BATCH`; `execute()` lists once and terminates.
- [ ] Files are read whole, auto-gunzipped when gzip-magic is present, and decoded via the configured
      `encodingType` into bare records matching the standard deserializers.
- [ ] Resume works: a completed file (per loaded checkpoint) is not re-emitted; progress is saved per file.
- [ ] With `JobContext.JOB_MASTER_URL` set, checkpoints `POST`/`GET` to `{url}/{sourceId}`; absent → in-memory.
- [ ] All removed subsystems (unbounded loop, emission strategies, partitioner, stability probe, post-actions,
      object-store checkpoint store + migrator) and their tests are gone; no dangling references.
- [ ] `:lib:compileJava` + `:lib:test` green; `spotlessApply` clean.

---

## 7. Reference index

- Command + loop to rewrite: `lib/.../commands/fssource/FsSourceCommand.java`
- Config DTO / validator / context: `lib/.../commands/fssource/{FsSourceDto,FsSourceConfigValidator,FsSourceExecutionContext}.java`
- Encoding → deserializer: `lib/.../serdes/EncodingType.java`, `lib/.../serdes/des/DeserializerFactory.java`,
  `FleakDeserializer.deserialize(SerializedEvent)`; usage pattern in `commands/sqssource/SqsSourceCommand.createRawDataConverter`
- Decompression: `lib/.../serdes/compression/GzipDecompressor.java`, `lib/.../utils/CompressionUtils.gunzip`,
  `lib/.../serdes/CompressionType.java`
- CheckpointClient to port: `zephflow-plus/.../commands/utils/CheckpointClient.java`;
  consumer pattern: `zephflow-plus/.../commands/atlasair/sftpsource/AtlasAirSftpSourceCommandPartsFactory.java`
- Checkpoint payload + id: `lib/.../commands/fssource/checkpoint/FsCheckpoint.java`,
  `lib/.../commands/fssource/util/SourceIdHasher.java`
- Job properties seam: `api/.../JobContext.java` (`otherProperties`, existing `DATA_KEY_PREFIX`/`FLAG_TEST_MODE`)
