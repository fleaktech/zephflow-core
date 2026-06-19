# Spec: `s3filereader` command

**Date:** 2026-06-19
**Status:** Ready for implementation
**Repo:** `zephflow-core`
**Audience:** the engineer/agent implementing this command. Assumes familiarity with how zephflow commands are structured but not with this specific feature.

---

## 1. Problem

We want event-driven ingestion of files that land in S3, specifically (first use case) gzipped AWS VPC flow logs in
`s3://dev-private-vpc-flow-logs/AWSLogs/<acct>/vpcflowlogs/<region>/<yyyy>/<mm>/<dd>/...log.gz`.

The event-driven plumbing already exists:

- S3 is configured to publish an **S3 event notification** to SQS whenever a new object is created.
- The **`sqssource`** command already consumes those messages and emits one `RecordFleakData` per message. With `encodingType: json`, the emitted record is the parsed S3 event notification, whose payload contains the bucket name and object key:

  ```json
  {
    "Records": [
      {
        "s3": {
          "bucket": { "name": "dev-private-vpc-flow-logs" },
          "object": { "key": "AWSLogs/679890967376/vpcflowlogs/us-east-1/2026/03/19/...log.gz", "size": 51365 }
        }
      }
    ]
  }
  ```

**The missing piece:** there is no node that takes a file path **from an incoming record**, reads that (gzipped) object from S3, and emits its content downstream. Every existing file-reading command (`fssource`, `reader`) reads from a **statically configured** path — none of them accept a path that arrives dynamically in an event. Decompression in the codebase today only exists as a *source-ingestion* serdes step (wired into `kinesissource` via `compressionTypes` and the generic bytes converter), not as a mid-DAG operation.

So the pipeline the user wants —

```
sqssource (S3 event JSON)  ->  extract s3 path  ->  [s3filereader]  ->  parser/eval/...  ->  sink
```

— is blocked on one new node: **`s3filereader`**.

---

## 2. Goal & non-goals

### Goal
A new **intermediate (mid-DAG) command** named `s3filereader` that:

1. Reads an `s3://bucket/key` path from a configurable field of the incoming record.
2. Downloads that object from S3 (with optional credential lookup, region, and endpoint override for LocalStack).
3. Transparently decompresses gzip (auto-detected by `.gz` suffix, overridable).
4. Emits the file content **downstream as multiple records** — one record per line by default — fanning out 1 input record → N output records.
5. Per-record failures (missing field, malformed path, S3 404, decompress error) are routed to the standard failure-event path, not fatal.

### Non-goals (out of scope for this spec)
- Parsing VPC-flow-log fields — that's a downstream `parser`/`eval` concern. This node emits raw lines.
- Streaming files too large to hold in memory (see §8 memory note). First use case is ~20–55 KB gzipped objects.
- Listing/discovery, checkpointing, post-actions, dedup — those belong to `fssource`. This node is a stateless per-event reader.
- Writing back to S3, deletion, or moving objects.
- Non-S3 backends (local fs, GCS, Azure). S3 only.

---

## 3. Where it fits — architecture

This is a `ScalarCommand` (mid-DAG), exactly like `parser`. Its factory reports `CommandType.INTERMEDIATE_COMMAND`.

The key capability that makes this work: `ScalarCommand.processOneEvent(event, callingUser, context)` returns
`List<RecordFleakData>`. The base class documents this as "may be empty for filtering, or **multiple for flattening**"
(`api/src/main/java/io/fleak/zephflow/api/ScalarCommand.java`). One input event (carrying a path) therefore legally fans
out to N output events (the file's lines). Per-event exceptions are caught by the base `process(...)` loop and converted
to `ErrorOutput` failure events automatically — the implementation just throws.

### Recommended division of labor
Keep `s3filereader` a clean primitive: **"given an s3 path in a field, emit the file's lines."** Do the S3-event-JSON
navigation **upstream** with an `eval` node that pulls `Records[0].s3.bucket.name` + `Records[0].s3.object.key` into a
single field. This keeps `s3filereader` decoupled from the S3-notification envelope (works equally for SNS-wrapped
notifications, hand-built paths, test fixtures, etc.).

Example DAG:

```
sqssource
  -> eval   (build "s3Path" = "s3://" + bucket.name + "/" + object.key  -- exact FEEL syntax is the DAG author's concern, illustrative only)
  -> s3filereader  (pathField: "s3Path", ...)
  -> parser  (split VPC flow log fields)
  -> sink
```

---

## 4. Files to create / change

Mirror the `parser` command package layout (`lib/src/main/java/io/fleak/zephflow/lib/commands/parser/`). New package:
`lib/src/main/java/io/fleak/zephflow/lib/commands/s3filereader/`.

| File | Responsibility | Template to copy from |
|---|---|---|
| `S3FileReaderDto.java` | Config record/POJO (interface w/ inner `Config implements CommandConfig`, Lombok `@Data @Builder ...`) | `commands/fssource/FsSourceDto.java`, `commands/reader/ReaderDto.java` |
| `S3FileReaderCommandFactory.java` | `extends CommandFactory`; builds `JsonConfigParser<S3FileReaderDto.Config>` + validator + command; `commandType()` returns `INTERMEDIATE_COMMAND` | `commands/parser/ParserCommandFactory.java` |
| `S3FileReaderConfigValidator.java` | Validate config (see §6) | `commands/parser/ParserConfigValidator.java` (find it next to the command) |
| `S3FileReaderCommand.java` | `extends ScalarCommand`; `createExecutionContext(...)` builds the S3 client + counters; `processOneEvent(...)` does the read/decompress/emit | `commands/parser/ParserCommand.java` |
| `S3FileReaderExecutionContext.java` | Holds the `S3Client`, counters, parsed config; implement `close()` to close the client | `commands/parser/ParserExecutionContext.java` |

**Changes to existing files:**

1. `lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java` — add constant near the other command names (~line 84):
   ```java
   String COMMAND_NAME_S3_FILE_READER = "s3filereader";
   ```
2. `lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java` — add one builder line:
   ```java
   .put(COMMAND_NAME_S3_FILE_READER, new S3FileReaderCommandFactory())
   ```

No new dependencies — `software.amazon.awssdk:s3` is already an `implementation` dep of `lib`.

---

## 5. Reusable building blocks (do NOT reinvent)

- **S3 client construction:** copy the pattern in
  `commands/fssource/backend/s3/S3Backend.client(S3BackendConfig)`. It already handles: region, optional static
  credentials, and `endpointOverride` + `forcePathStyle(true)` for LocalStack. Build the client once in
  `createExecutionContext` and reuse it across events; close it in the execution context's `close()`.
- **Reading an object:** `S3Reader.open(...)` shows the `getObject` call shape; you can reuse `S3Reader` directly or
  inline a `GetObjectRequest`.
- **Credential lookup from `credentialId`:** `MiscUtils.lookupUsernamePasswordCredentialOpt(jobContext, credentialId)`
  returns an `Optional<UsernamePasswordCredential>` where `getUsername()` = access key id and `getPassword()` = secret
  access key. See `commands/fssource/FsSourceCommand.s3BackendConfig(...)` for the exact resolution (including the
  "credentialId set but not found" error case).
- **Gzip decompression:** wrap the object `InputStream` in `java.util.zip.GZIPInputStream`, OR reuse
  `serdes/compression/GzipDecompressor` / `DecompressorFactory.getDecompressor(CompressionType.GZIP)`. Prefer streaming
  (`GZIPInputStream` over the S3 stream) so you don't buffer the whole compressed blob.
- **Line → record encoding (optional `DESERIALIZE` mode, see §6):**
  `DeserializerFactory.createDeserializerFactory(encodingType).createDeserializer()` returns a `FleakDeserializer`;
  call `deserialize(new SerializedEvent(null, bytes, null))`. See `SqsSourceCommand.createRawDataConverter(...)`.

---

## 6. Config schema (`S3FileReaderDto.Config`)

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `pathField` | String | **yes** | — | Name of the field in the incoming record holding the full `s3://bucket/key` URI. (Keep it a simple top-level field name; the upstream `eval` is responsible for producing it.) |
| `credentialId` | String | no | null | Credential to look up for static AWS keys. If null, fall back to the default AWS provider chain (env / instance profile / SSO) — same behavior as `S3Backend.client` when keys are blank. |
| `region` | String | **yes** | — | AWS region of the bucket (e.g. `us-east-1`). |
| `s3EndpointOverride` | String | no | null | For LocalStack/MinIO; when set, also enable path-style access. |
| `compression` | enum `AUTO \| NONE \| GZIP` | no | `AUTO` | `AUTO` = gunzip iff key ends in `.gz`; `GZIP` = always; `NONE` = never. |
| `emission` | enum `LINE \| DESERIALIZE` | no | `LINE` | `LINE`: emit one record per text line (raw). `DESERIALIZE`: parse each line with `encodingType`. |
| `encodingType` | `EncodingType` | only if `emission=DESERIALIZE` | `JSON_OBJECT_LINE` | How to parse each line into structured fields. |
| `urlDecodeKey` | boolean | no | `true` | URL-decode the object key before fetching. **S3 event notifications URL-encode keys** (spaces→`+`, `%XX`), so decoding is required for the primary use case. Set `false` if the upstream already produced a decoded key. Only decode the key portion, never the bucket. |
| `batchSize` | int | no | `500` | Reserved for future chunked emission; for now controls nothing functional but keep it for forward-compat / parity with `fssource`. (Implementer may omit if unused — note in PR.) |

### Validator rules (`S3FileReaderConfigValidator`)
- `pathField` non-blank.
- `region` non-blank.
- If `emission == DESERIALIZE`, `encodingType` must be set.
- `compression` / `emission` parse to known enum values.

### Example config JSON
```json
{
  "pathField": "s3Path",
  "region": "us-east-1",
  "credentialId": "vpc-flow-logs-rw",
  "compression": "AUTO",
  "emission": "LINE"
}
```

---

## 7. `processOneEvent` algorithm

```
input: RecordFleakData event

1. pathStr = event.getPayload().get(config.pathField)   // String
   - if missing/blank/not a string -> throw IllegalArgumentException("...pathField '<x>' not found...")
     (base class converts to a failure event)

2. parse pathStr:
   - require prefix "s3://"; strip it
   - bucket = substring up to first '/', key = remainder
   - if config.urlDecodeKey: key = URLDecoder.decode(key, UTF_8)   // key only, not bucket

3. shouldGunzip =
     compression==GZIP ? true
   : compression==NONE ? false
   : /* AUTO */ key.endsWith(".gz")

4. open stream:
   InputStream in = s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build());
   if (shouldGunzip) in = new GZIPInputStream(in);

5. read content:
   try (BufferedReader br = new BufferedReader(new InputStreamReader(in, UTF_8))) {
     for each line:
       LINE mode        -> record = { "line": <line>, "file": pathStr }   // mirror fssource LineEmissionStrategy keys
       DESERIALIZE mode -> records += deserializer.deserialize(new SerializedEvent(null, line.getBytes(UTF_8), null))
   }
   increment input/output/error counters (basicCommandMetricTags pattern from ParserCommand)

6. return List<RecordFleakData> (all lines)
```

**Output record shape (LINE mode)** — reuse the field names the FS source already uses
(`commands/fssource/emission/LineEmissionStrategy`) for consistency:
```json
{ "line": "<one line of the file>", "file": "s3://dev-private-vpc-flow-logs/AWSLogs/.../x.log.gz" }
```

---

## 8. Edge cases & gotchas

1. **URL-encoded keys (most important).** S3 event notification object keys are URL-encoded. The literal key in the
   JSON for `.../2026/03/19/foo bar.log.gz` arrives as `.../2026/03/19/foo+bar.log.gz`. Fetching without decoding →
   `NoSuchKey` 404. Hence `urlDecodeKey` defaults to `true`. Decode **only the key**, never the bucket.
2. **Memory / fan-out.** `processOneEvent` returns a materialized `List`, so the whole file's lines are held in memory
   at once. Fine for the ~20–55 KB gz VPC-log objects (verified against the live bucket). Document this limit in the
   class Javadoc; a future streaming/`batchSize`-chunked emission is explicitly out of scope here.
3. **Region required.** Unlike the SDK default-region behavior, require `region` in config to avoid surprising
   `us-east-1` fallbacks.
4. **Credential fallthrough.** If `credentialId` is null/blank, do NOT fail — let the default AWS provider chain apply
   (matches `S3Backend.client`). If `credentialId` is set but not resolvable, throw a clear config error at context
   creation, mirroring `FsSourceCommand`.
5. **Non-`s3://` path.** Throw a descriptive error (failure event), don't silently skip.
6. **Empty file / zero lines.** Return an empty list — valid, emits nothing downstream.
7. **Client lifecycle.** One `S3Client` per command instance (built in `createExecutionContext`, closed in context
   `close()`), not one per event.

---

## 9. Testing plan

Follow the existing S3 test approach in the repo — **LocalStack via Testcontainers** (`libs.testcontainers.localstack`
is already a test dep of `lib`; see `commands/fssource/backend/s3/S3BackendIntegrationTest.java` and
`commands/s3/S3SinkCommandTest.java` for the container setup + `endpointOverride` wiring).

Required cases:

1. **Plain text file, LINE mode** — upload an uncompressed multi-line object; assert N records out with correct
   `line`/`file` fields.
2. **Gzip file, AUTO mode** — upload a `.gz` object; assert it's transparently decompressed and line count matches.
3. **`compression: NONE` over a `.gz` key** — assert it does NOT gunzip (negative control).
4. **URL-encoded key** — object whose real key contains a space; pass the `+`-encoded path in the record with
   `urlDecodeKey=true`; assert the object is found and read.
5. **Missing `pathField`** — assert a failure event is produced (and `process(...)` does not throw out).
6. **Non-`s3://` path / 404 key** — assert failure event, not crash.
7. **DESERIALIZE mode** — `JSON_OBJECT_LINE` content → structured records.
8. **Validator** — blank `pathField`, blank `region`, `DESERIALIZE` without `encodingType` all rejected.
9. **Registry wiring** — `OperatorCommandRegistry` returns the factory for `"s3filereader"` and it reports
   `INTERMEDIATE_COMMAND`.

Run with the local clistarter build path already wired into grid (`ZEPHFLOW_LOCAL_DIR`) for an end-to-end smoke test:
`sqssource -> eval -> s3filereader -> stdout` against the real `dev-private-vpc-flow-logs` bucket.

---

## 10. Acceptance criteria

- [ ] `s3filereader` registered and resolvable as an `INTERMEDIATE_COMMAND`.
- [ ] Given a record with an `s3://` path field, downloads + (auto-)gunzips + emits one record per line with
      `{line, file}`.
- [ ] URL-encoded keys resolve correctly with `urlDecodeKey=true` (default).
- [ ] Credential lookup via `credentialId` works; default chain used when absent; clear error when set-but-missing.
- [ ] LocalStack `s3EndpointOverride` path works (covered by tests).
- [ ] Per-event failures become failure events; the command never aborts the batch.
- [ ] All §9 tests pass; `spotlessApply` clean; `./gradlew :lib:compileJava :lib:test` green.

---

## 11. Reference index (exact paths)

- Base class / fan-out contract: `api/src/main/java/io/fleak/zephflow/api/ScalarCommand.java`
- Command type enum: `api/src/main/java/io/fleak/zephflow/api/CommandType.java` (`INTERMEDIATE_COMMAND`)
- Mid-DAG command template: `lib/src/main/java/io/fleak/zephflow/lib/commands/parser/{ParserCommand,ParserCommandFactory}.java`
- S3 client + creds + endpoint override: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/s3/S3Backend.java`
- Object read shape: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/s3/S3Reader.java`
- Credential resolution from `credentialId`: `MiscUtils.lookupUsernamePasswordCredentialOpt(...)` +
  `FsSourceCommand.s3BackendConfig(...)`
- Line-emission output field names to mirror: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/emission/LineEmissionStrategy.java`
- Encoding → deserializer: `serdes/des/DeserializerFactory` + `SqsSourceCommand.createRawDataConverter(...)`
- Gzip: `serdes/compression/{GzipDecompressor,DecompressorFactory}.java`
- Registry + name constant: `lib/.../commands/OperatorCommandRegistry.java`, `lib/.../utils/MiscUtils.java`
- S3 test pattern (LocalStack): `lib/src/test/.../fssource/backend/s3/S3BackendIntegrationTest.java`
```
