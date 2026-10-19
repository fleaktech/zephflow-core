# S3 Realtime Source on the Generic Decoder

The S3 realtime source (`s3rtsource`) reads the objects named by S3 event notifications from SQS.
This change makes it decode them with the generic decoder (`docs/spec/generic-decoder.md`) instead
of the legacy `FleakDeserializer`. Each object is downloaded to local disk and decoded from there as
a stream, instead of being held in memory.

## 1. Why

`S3RealtimeRawDataConverter` holds everything in memory, several times over:

- `readAllBytes()` downloads the whole object, `CompressionUtils.gunzip` decompresses all of it into
  a second array, and the deserializer parses all of it at once.
- `maxObjectSizeBytes` (default 256 MiB) caps only the compressed size. A 200 MiB gzip log that
  decompresses to several GiB is accepted, and the process runs out of memory.
- `convert()` returns every record of every object of a message in one list, and
  `SimpleSourceCommand` converts all fetched messages (up to 10) before it emits any of them.

It also reads only gzip and the seven legacy encodings: no parquet, no multiline events (stack
traces), no zstd or other codecs, no CSV options.

## 2. Goals and non-goals

**Goals**
- Memory bounded by fixed limits, not by object size: an object is downloaded to local disk, read
  from there as a stream, and its records are emitted in bounded batches.
- Every format, codec and framing the generic decoder supports, through a new `decoding` field.
- Existing configurations (`encodingType`, `compressionType`) keep working without change, so the
  webapp form, data assets and dynamicapi stay as they are. They are translated into a
  `DecodingConfig`.
- Bad records no longer cost the whole object: each one is dead-lettered on its own, and the good
  records of the object are emitted.
- One public entry point decodes one S3 object, so dynamicapi's sampler can preview what a pipeline
  would emit (5).

**Non-goals**
- Other sources. FsSource and the message sources move over in their own changes.
- The webapp and dynamicapi. The sampler (`S3RealtimeDataSampler`) moves to the entry point of 5 in
  a follow-up there after this ships in a release; self-healing's handling of undecodable DLQ
  entries is FLE-3136 (6). Until the webapp follows, `decoding` is for API and SDK users (6).
- Configurable limits. They are fixed (3.4); fields can be added when a pipeline needs them.
- Copying legacy quirks. Where the decoder behaves differently, the difference is listed in 4.2.
- Extending the SQS visibility timeout while an object is processed.

## 3. Design

### 3.1 Per message

`SimpleSourceCommand` gains an additive path for converters that emit their own batches. It applies
only to such a converter, so every other source, including those in zephflow-plus, runs exactly as
before. On this path the fetched messages are sampled to the raw-data sampler as today, then handled
one at a time: a message's objects are decoded and emitted, and the message is committed (deleted
from SQS) before the next one starts. `SimpleSourceCommand` decides when a message is done and
acknowledges it to the converter; the converter queues its receipt handle only then, never while
emitting.

For each object of a message, in order:

1. The object is downloaded to a local file (3.2). `NoSuchKey` skips it as today (counter
   `missing`, no DLQ entry). A stored size over `maxObjectSizeBytes` aborts the response,
   dead-letters the notification and skips the object as today (counter `oversized`). A failed
   download is a read failure (3.3).
2. The local file is decoded by the compiled `RecordDecoder` with the fixed limits (3.4).
3. `Decoded` records are enriched with `__s3_bucket` / `__s3_key` when `addS3Metadata` is set, and
   collected into a batch. A batch is emitted downstream when it reaches its size cap (3.4) or 1,000
   records, and when the object ends.
4. Other outcomes are handled as in 3.3. A terminal outcome or a read failure ends the object; the
   records already emitted from it stand, and the next object starts.
5. The local file is deleted, whatever the outcome.

The message is committed once all its objects are handled. Nothing is retried in-process apart from
the download (3.2), and a crash before the commit lets SQS redeliver the message (at-least-once,
with the already emitted records emitted again).

**Stop.** The path keeps its own stop signal, set only by `terminate()` (not by a fetcher reporting
exhaustion, which sets `finished` before its last fetch is processed, `SimpleSourceCommand`'s
contract for every fetcher). It is checked before each object, between batches and before the
commit. `terminate()` also closes the S3 and SQS clients and the DLQ writer, so a failure seen after
the stop counts as the stop: no DLQ entry, no counter, and the message is not committed, so SQS
delivers it again after its visibility timeout.

### 3.2 Downloading an object

The object is downloaded whole to local disk before anything is decoded, so nothing is emitted
while the network is involved: a broken connection costs a new download, never a duplicate or a lost
record, and the downstream can take as long as it needs without holding a connection open.

- It is opened with one plain `GetObject`, as today, so errors surface as today (`NoSuchKey`,
  `NoSuchBucket`, access denied). A stored length over `maxObjectSizeBytes` aborts the response
  (`ResponseInputStream.abort()`), so the object is not downloaded; a response without a length
  is a read failure, as it is today.
- The body is copied to a new file in the source's spool directory, and the copy must reach the
  stored length; a premature end is a failure.
- A failure while reading the body (the connection closed, a premature end) deletes the partial
  file and starts the download again from the beginning, up to 3 attempts in all. `NoSuchKey` on a
  later attempt is a `missing` skip. A failure to write the local file (a full disk) is not retried.
  An interrupt is not retried either. A download that does not succeed is a read failure: an
  object summary with the notification as its value (3.3), as today's download failures are.
- The decoder reads the file through a new `ByteInput.ofFile(Path)` in the decoder, which streams
  the file and offers random access to it, so parquet reads its footer and row groups from disk.
- `maxObjectSizeBytes` is also the disk cap: the source handles one object at a time, so it needs
  at most that much disk. There is no fallback to decoding straight from S3.
- The spool directory is a new directory under the JVM's working directory, created when the source
  starts and deleted when it closes; when it cannot be created there, it goes under
  `java.io.tmpdir`. In grid the working directory is the task's directory on the worker's data
  volume, which grid deletes when the task exits, so a killed pipeline leaves nothing behind and the
  download uses the disk grid provisions. Elsewhere a killed process can leave one object's file.

### 3.3 Outcomes and the DLQ

An outcome becomes a record entry only when it is a `Failed` record that carries its bytes and is at
most 128 KiB; everything else is summarized per object, so a DLQ value is always either a whole
record or a notification, never a cut-off record.

| Outcome | Action |
|---|---|
| `Decoded` | Batched and emitted (3.1); counted in `inputEventCount` |
| `Failed`, not terminal, with bytes of at most 128 KiB | A record entry; decoding goes on |
| `Failed`, not terminal, without bytes (avro `ocf`, parquet, xml `elements`, yaml `multi_document`) or with more than 128 KiB | Added to the object's summary for its kind; decoding goes on |
| `Oversized`, not terminal | Added to the object's summary for its kind; decoding goes on |
| `Failed` or `Oversized`, terminal | Added to the object's summary for its kind; the object ends |
| A failed download, `SourceReadException`, or any other exception while handling the object (including resolving its region's client) | Added to the object's summary; the object ends |
| Exception from downstream (`accept`) | Added to the object's downstream summary for the failing node; decoding goes on with the next batch |

So I3 holds for the bad records whose bytes the decoder hands over and that fit in 128 KiB; the
others are listed in their object's summary by index and position, and recovered from the object.
`Oversized` never becomes a record entry, because its 4 KiB preview would replay as a complete
record (6).

A failed write of one of the converter's own entries ends the source as well: the converter throws
`DeadLetterWriteException`, and `SimpleSourceCommand` neither dead-letters nor acknowledges the
message, so SQS delivers it again.

An exception that escapes the converter is a bug, outside these rows: `SimpleSourceCommand`
dead-letters the notification with the stack trace and the failing node's id, in the framework's
shape of today, and a failed DLQ write ends the source, as it does on the other path.

Every outcome other than `Decoded` is counted in `deserializeFailureCount`. A failed batch is not:
the runner already counts the failing node's records, as today.

dynamicapi groups DLQ entries by the first line of their error text and the node id, and its repair
agent sees only that line, so the first line of a source entry is a fixed template per kind that
names the cause's exception class, and everything that varies (the decoder's message, the object,
sizes, counts, indexes) comes after it:

| Entry | First line |
|---|---|
| Record entry | `MALFORMED <format> record: <exception class>` |
| Object summary | `<MALFORMED or CORRUPT_COMPRESSION> <format> object: <exception class>`, `OVERSIZED <format> object`, or `READ_FAILURE s3 object: <exception class>` |
| Object over `maxObjectSizeBytes` | `OBJECT_TOO_LARGE s3 object` |
| Downstream summary | Today's: the first line of the `NodeExecutionException`'s stack trace, which carries the failing node's error |

`<format>` is the decoding format's type (`json`, `csv`, `text`, ...). The exception class is the
simple name of the innermost cause that is not a `SourceReadException` (`SourceReadException` when
the chain has no other), followed by the error code for an AWS service exception that has one
(`S3Exception AccessDenied`), so an access-denied object, a missing bucket and a full disk fall into
different groups.

- A **record entry**'s value is the record's bytes, whole. Unlike FsSource's entries, it has no
  metadata, because dynamicapi's replay merges an entry's metadata into the replayed record's
  fields. Its error text is the first line, the decoder error's stack trace, and a line naming the
  object, the `recordIndex` when it is at least 1, and the span when there is one. Its node id is
  the source's.
- An **object summary** is one entry per object and outcome kind, written when the object ends. It
  keeps today's shape: the notification as the value, so the object can be replayed by sending the
  notification again, and the source's node id. Its error text is the first line, the first error's
  stack trace (or, for `Oversized`, `over the limit (maxRecordBytes <limit>, maxDocumentBytes
  <limit>, maxCodecMemoryBytes <limit>)`, or for parquet `over the parquet decoder's heap budget`,
  with the size reached), the object with its version id, how many outcomes it covers, their
  `recordIndex` ranges with spans ("the document" for a whole-document format, "the header" for a
  csv header; adjacent ranges merged, the first 100 kept and the rest counted), and, when the object
  ended on this kind, that it ended (at which `recordIndex`, for a record; after which, for a read
  failure). Indexes and spans refer to that version; a
  notification sent again reads the key's current version.
- A **downstream summary** is one entry per object and failing node, written when the object ends.
  It keeps today's shape: the notification as the value, the failing node's id from
  `NodeExecutionException`, and that exception's stack trace, followed by the object, how many
  batches failed and their record index ranges (adjacent ranges merged, at most 100 listed). The
  runner reports only the first failing node and its error, not which records failed, so the records
  of a failed batch cannot be dead-lettered on their own; recovery is sending the notification
  again, which emits the object's other records again too. With a DLQ configured, the runner throws
  only after the whole batch ran (siblings of a failing record still reach the sinks), so going on
  with the next batch loses nothing else, as today. Storing the failed batch's records as JSON
  (generic-decoder Appendix A) is deferred: it needs a format marker that dynamicapi,
  zephflow-plus's replay and the DLQ schema understand first.

Without a configured DLQ, failures are logged and dropped, as today.

### 3.4 Limits and memory

- The limits are fixed proportions of a heap size, the first of:
  - the size a caller of 5 passes (at least 1 MiB);
  - the JVM's last `-Xmx` (or `-XX:MaxHeapSize=`) input argument, as given;
  - max(`TASK_MAX_MEMORY`, 1024 MiB), parsed as grid parses it, which is the `-Xmx` grid gives a
    pipeline from the environment variable it sets;
  - `Runtime.maxMemory()`.

  The nominal `-Xmx` is preferred to `Runtime.maxMemory()` because some collectors report less
  than `-Xmx`, and to `TASK_MAX_MEMORY` because grid also starts other JVMs with that variable
  and a smaller `-Xmx`.

  | Format | `maxRecordBytes` | `maxDocumentBytes` | At 2 GiB |
  |---|---|---|---|
  | json, text, csv, xml, yaml | heap/128 | heap/64 | 16 / 32 MiB |
  | avro | heap/128 | heap/512 | 16 / 4 MiB |
  | protobuf | heap/1024 | heap/1024 | 2 / 2 MiB |
  | parquet | 0 | 0 | the decoder's own budget |

  Parquet passes no limits, so the decoder bounds footer, row group and row by half the heap with
  room reserved for the row (decoder spec, 3.4); its row limit is the same heap/128. Avro and
  protobuf get smaller limits, as the user decided, because the decoder lets an avro block build up
  to 150–250 times `maxDocumentBytes` and a protobuf message about 300 times its limit (decoder
  spec, 7).
- The memory the outer compression codec may use (xz's dictionary) is capped at 128 MiB or half the
  heap, whichever is smaller, which is the decoder's own rule without limits, instead of
  `maxDocumentBytes`, so the smaller avro and protobuf limits do not reject ordinary xz files. This
  needs a third limit in `DecodeLimits` (`maxCodecMemoryBytes`; the two-argument constructor stays
  and sets it to 0, today's rule). Avro's own block codecs keep the decoder's rule.
- A batch is capped at 4 MiB or heap/512, whichever is smaller, of estimated record size. The size
  is estimated per record as the decoder estimates parquet rows: 8 bytes per value and per field,
  plus the bytes of its strings and binaries. Records without a span (parquet, avro, xml elements,
  yaml) are covered the same way.
- What is in memory at a time: one batch (4 MiB estimated, about 16–56 MiB of heap at the 4–14
  times the decoder measures for rows), one record being decoded, and the decoder's own unit (a
  document, an avro block, a parquet row group). The largest unit is a tree document at the limit
  (json `object`, xml `document`, yaml, text `whole`), which takes 10–50 times its bytes, between
  16% and 78% of the heap, plus the batch; an avro block at its limit stays under half the heap, a
  protobuf message under a third. Downstream nodes copy records instead of changing them, so a
  document near the limit also needs room for its copy (9).
- DLQ entries wait in the DLQ writer's buffer until its batch is full or its timer fires; when full,
  the source's thread uploads the batch, and the timer's thread can upload one batch while the next
  fills. Each entry holds at most 128 KiB of record value or one notification, plus its error text
  (one stack trace and the bounded lines above). At the default batch of 100 a batch is at most
  about 13 MiB; serializing and uploading it makes up to three more copies, and two batches can be
  in flight, so the writer can hold about 100 MiB, plus a base64 copy of each batch whose upload
  fails (it is logged). This follows the DLQ batch size, not the heap (9).
- `maxObjectSizeBytes` keeps its meaning (the stored size, checked before downloading) and its
  256 MiB default. It no longer protects memory; it bounds the disk the source needs. It does not
  bound the processing time of a compressed object (9).

## 4. Configuration

A new field on `S3RealtimeSourceDto.Config`:

| Field | Meaning |
|---|---|
| `decoding` | A `DecodingConfig` (decoder spec, 4) |

`encodingType` is no longer required. Exactly one of `encodingType` and `decoding` must be set, and
`compressionType` is allowed only with `encodingType` (`decoding` carries its own compression). The
validator compiles the decoding configuration, so a bad one (an invalid regex, parquet with
compression) fails validation, not the first object.

### 4.1 Legacy mapping

| `encodingType` | `format` |
|---|---|
| `JSON_OBJECT` | `{"type": "json", "mode": "object"}` |
| `JSON_ARRAY` | `{"type": "json", "mode": "array"}` |
| `JSON_OBJECT_LINE` | `{"type": "json", "mode": "lines"}` |
| `STRING_LINE` | `{"type": "text", "framing": {"type": "line", "skipBlank": false}}` |
| `TEXT` | `{"type": "text", "framing": {"type": "whole"}}` |
| `CSV` | `{"type": "csv", "ignoreSurroundingSpaces": true}` |
| `XML` | `{"type": "xml", "mode": "document"}` |

`compressionType` null maps to `"auto"`, the same gzip detection by magic bytes; `GZIP` maps to
`"gzip"`. `PARQUET` stays rejected, as `DeserializerFactory.validateEncodingType` rejects it today;
parquet is available through `decoding`.

`skipBlank: false` and `ignoreSurroundingSpaces: true` keep the legacy output: STRING_LINE emits
blank lines, and legacy CSV trims spaces around names and values.

### 4.2 Behavior changes for legacy configurations

Compared with feeding the same bytes to both paths (committed as `LegacyDifferentialTest`, 8). With
no limits hit, the records of valid input are the same, except:

- **Bad data loses only itself.** Legacy dead-letters the whole object on the first bad line,
  element or row and emits nothing from it. Now the good records are emitted and each bad one is
  dead-lettered (3.3). A pretty-printed file read as JSON_OBJECT_LINE gives one DLQ entry per line.
  In JSON_ARRAY a malformed element ends the object, after the elements before it.
- **JSON_OBJECT_LINE: a line holding an array is a failed record**, where legacy emitted one record
  per element (decided by the user).
- **CSV: a row with more columns than the header is a failed record**, where legacy silently
  truncated it. A row with fewer columns, or a bad quote, now fails alone instead of the object; a
  quote that is never closed takes the rest of the object with it and ends it.
  Duplicate header names end the object with no records (legacy kept the last column), and an empty
  header name becomes `_c<i>` (legacy failed the object).
- **A UTF-8 byte order mark is removed** in STRING_LINE, TEXT and CSV (legacy kept it in the first
  line, text or header name) and in JSON_OBJECT_LINE (legacy failed the first line).
- **An empty object gives no records and no DLQ entry.** Legacy dead-lettered it for JSON_OBJECT,
  JSON_ARRAY, XML and a forced GZIP, and emitted `{"__raw__": ""}` for TEXT, also when the object
  is a gzip of nothing.
- **gzip: bytes after the last member are `CORRUPT_COMPRESSION`**, after the records before them;
  legacy ignored them. A truncated gzip object emits the records decoded before the cut.
- **XML: any `DOCTYPE`, and an element or text after the root, fail the object**; legacy accepted a
  DOCTYPE without entities and ignored trailing content.
- **JSON_OBJECT_LINE: a line with invalid UTF-8 is a failed record.** Legacy decoded the line to a
  string first, turning the bad bytes into U+FFFD, and emitted the record; the decoder parses the
  bytes, which Jackson rejects. JSON_OBJECT and JSON_ARRAY already parsed bytes, and text and CSV
  still replace bad bytes.
- **JSON_OBJECT_LINE: a line of other whitespace is a failed record.** Only spaces and tabs count as
  blank now; legacy skipped any line `String.isBlank()` accepts (also form feeds, vertical tabs,
  U+001C–U+001F and Unicode space separators such as U+3000). UTF-16 and UTF-32 input now decodes.
- **Limits are new**: a record or document over the limits of 3.4 is `Oversized`. Legacy had no
  limit but `maxObjectSizeBytes`.
- **The object is downloaded to local disk**, which needs up to `maxObjectSizeBytes` of free space
  in the working directory (3.2). A body that breaks mid-download is downloaded again, up to 3
  attempts, where legacy dead-lettered the notification at once.
- **A failed DLQ write ends the source.** Legacy logged a failed write of its own entry and
  acknowledged the message; now the converter throws, the source ends, and SQS delivers the message
  again (3.3).
- **Messages are deleted one by one.** Legacy converted all messages of a receive first and deleted
  every one of them after the first message's records were accepted, so a crash could lose the rest.
  Now each message is deleted after its own records, which closes that loss but keeps the later
  messages of a receive in flight longer (9).
- **Downstream failures are per batch.** Legacy ran all records of a message in one `accept` and
  wrote one framework DLQ entry per failing message. Now the remaining batches still run, and each
  object writes one summary per failing node (3.3), so a message can give several entries.
- **DLQ error texts of decode, read and too-large-object failures.** Legacy texts start with
  `failed to process s3://<bucket>/<key>: <exception class>: <message>`, so self-healing grouped by
  object and its repair agent saw the message. Now the first line is the template of 3.3: the
  entries of many objects that fail the same way form one group, and the agent sees the kind and
  the exception class (and the format for decode failures) but not the message, sizes or limits,
  which follow on the next lines. Downstream entries keep today's first line.
- **Metrics.** `deserializeFailureCount` counts failed records and failed objects, where it counted
  only failed objects. `inputEventSizeCount` still approximates the decompressed bytes: it is the
  end of the object's last span of known length, so line terminators and bytes after the last
  record (a closing `]`, a trailing newline) are not counted, and an object without records counts
  0. Every legacy mapping gives spans. For a `decoding` format without spans (avro `ocf`, parquet,
  xml `elements`, yaml `multi_document`) it counts the stored bytes, except when the object ends on
  a read failure, which counts none.
- **DLQ values.** A bad record's entry holds its bytes, which do not decode; legacy's entries held
  the notification (6).

## 5. Shared entry point

`S3ObjectDecoder` (public, in the source's package) decodes one S3 object; the source and, in the
follow-up, dynamicapi's sampler both use it:

- It is built from `S3Decoding`, a record of only the decoding inputs (`encodingType`,
  `compressionType`, `decoding`, `addS3Metadata`), not the source `Config`, whose queue fields are
  required, and from the heap size the limits are computed from (3.4). The entry point applies 4.1,
  the validation of 4 and the `addS3Metadata` enrichment, and compiles the decoder once.
- The source passes its heap size from 3.4. A sampler passes the pipeline's (2048 MiB unless it
  knows the pipeline's `TASK_MAX_MEMORY`), so a preview hits the pipeline's limits when the pipeline
  runs with the default; one that runs with a different `TASK_MAX_MEMORY` gets different limits.
  Parquet is the other exception: the decoder sizes its budget from the heap of the JVM it runs in,
  so a sampler with a different heap can accept or skip a row group the pipeline would not. The
  sampler's own heap (1 GiB in dynamicapi) is smaller than a 2 GiB pipeline's, so a tree document at
  the pipeline's limit can take more of it than 3.4 allows; that is no worse than today's sampler,
  which reads the whole object.
- `decode(S3Client, S3ObjectRef, maxObjectSizeBytes, spoolDir, visitor)` downloads the object as in
  3.2 and hands each `DecodeOutcome` to the visitor until the visitor asks to stop or the object
  ends, then deletes the local file, whatever the outcome. The caller owns `spoolDir`: the source
  passes its spool directory, and a sampler passes a directory it creates and removes itself. It
  returns whether the object was missing, too large, finished or stopped, and the stored length,
  ETag and version id from the response (also for a too large object). A failed download throws
  `SourceReadException`.
- `recordDecoder()` and `limits()` expose the compiled decoder and limits, for a caller that holds
  an object's bytes itself (the sampler's dataset mode, which shows each record's raw bytes). Spans
  index the stream after decompression and transcoding, so they slice the stored bytes only for an
  uncompressed UTF-8 object; for any other object such a caller shows the whole object as each
  record's raw bytes, as the sampler does today.
- Batching, the DLQ and counters stay in the source's converter; a sampler collects records until it
  has enough and then stops.

## 6. Compatibility

- Every public class of `lib.commands.source` (`SimpleSourceCommand`, its fetcher, converter,
  encoder and commit-strategy types), of `lib.serdes.des` (the legacy deserializers the other
  sources keep using), `S3EventNotificationParser`, `S3ObjectRef`, `S3RegionalClientProvider` and
  `DecodeLimits`'s two-argument constructor keep their signatures; dynamicapi and zephflow-plus
  import them. `DecodeOutcome.Failed` and `Oversized` gain components (PR 3b); no code outside core
  uses the decoder yet. No code outside core uses `S3RealtimeSourceDto`, `S3RealtimeRawDataConverter` or
  `S3RealtimeSourceCommand`.
- DLQ values were never guaranteed to decode: message sources dead-letter a message they cannot
  deserialize with its raw bytes (`BytesRawDataConverter`, `SimpleSourceCommand`), and FsSource
  dead-letters each bad record's bytes. S3 realtime record entries do the same. Self-healing replay
  and zephflow-plus's `dlqsource` decode a value with the source's `encodingType` and the legacy
  deserializers; a value the live decoder rejected mostly fails again and is skipped, but some
  replay differently: a JSON_OBJECT_LINE array line is flattened into records (D1), a line with
  invalid UTF-8 gets U+FFFD, and a CSV row is read as a header line and gives no records. dynamicapi
  also decodes entry values with `JSON_OBJECT` when there is no `encodingType` (so for `decoding`
  configurations), and its self-healing preprocessing discards the whole DLQ sample of an analysis
  when one entry fails to decode. That affects every source today and is fixed in dynamicapi
  (FLE-3136), not here. Object and downstream summaries keep today's notification value, recovered
  by sending the notification again. One object can now give several such entries (one per kind
  and per failing node) with the same key, the message id, where legacy wrote at most one per
  object; nothing deduplicates them, so whoever resends notifications has to send each message id
  once. Self-healing replay through zephflow-plus's `dlqsource` decodes a notification value as a
  record instead of reading the object, once per entry (FLE-3137).
- `decoding` is for API and SDK users until the webapp follows. The webapp's inline-connection
  deploy rebuilds the node from `encodingType` (defaulting to `JSON_OBJECT`) and keeps the other
  fields, so a node with `decoding` gets both and fails validation; its test-data preview samples
  with `encodingType` only.

## 7. Delivery

Small PRs into the integration branch `feature/s3rt-decoder`; the user merges it into main.

| PR | Content |
|---|---|
| 1 | This spec; `ByteInput.ofFile` in the decoder, with the decoder spec's `ByteInput` paragraph; the download to the spool directory, with its tests |
| 2 | `SimpleSourceCommand`'s path for converters that emit their own batches, with tests |
| 3 | `DecodeLimits.maxCodecMemoryBytes` in the decoder, with its tests and the decoder spec's xz row |
| 3b | The decoder's `Oversized` names the limit it hit and its value, and `Failed` and `Oversized` carry how many records they stand for, so the DLQ can say what to change and count a lost avro block or parquet row group whole; with tests and the decoder spec's 3.2 and 3.4 |
| 4a | `S3Decoding`, the legacy mapping, the limits, `S3ObjectDecoder`, with their tests and the differential test |
| 4b | The converter on `S3ObjectDecoder`, the `decoding` field and validator, the source tests and the low-memory test; the decoder spec's "not used by any source yet" is updated |

## 8. Testing

- The download against a mocked `S3Client`: a body that breaks at any offset, or with any of the
  errors a body read throws, is downloaded again, with the partial file deleted before the next
  attempt; 3 failed attempts are a read failure; `NoSuchKey` on a later attempt is `missing`; a
  premature end; a failed write is not retried; an interrupt is not retried; a response without a
  length; an oversized object is aborted, not downloaded; a zero-length object; the local file is
  deleted on every outcome. The spool directory is created under the working directory, falls back
  to `java.io.tmpdir`, and is deleted on close. `ByteInput.ofFile`: streaming, ranges, and a
  zero-length range.
- The existing LocalStack integration test issues only the plain `GetObject` the download uses. Its
  assertion on the DLQ entry of `bad.jsonl` (`failed to process s3://…`) is updated in PR 4b to the
  record entry of 3.3; no cases are added.
- `SimpleSourceCommand`: a converter that emits its own batches gets each message committed after
  its last batch and not before; a fetcher that reports exhaustion still gets its last messages
  handled and committed; `terminate()` between batches, and a client failure after it, leave the
  message uncommitted with no DLQ entry; raw-data sampling still runs; other converters are
  unaffected.
- The S3 realtime unit tests are rewritten for the new path. The behaviors the current tests pin
  stay required: missing object, oversized object, a message with good and bad objects, an invalid
  or empty region dead-lettered without losing its sibling, client failures closing the created
  clients and keeping the good sibling, the object's region, gzip detected and forced,
  `addS3Metadata`, the DLQ entry of a failed object. New cases, for each legacy encoding and for
  parquet, multiline and zstd: whole expected record lists, batch boundaries, each entry shape of
  3.3 with its node id and error text, a bad record of just over 128 KiB summarized, an
  `Oversized` record summarized, an object over `maxObjectSizeBytes`, counters,
  `inputEventSizeCount` for a gzip object (the end of its last span), records before a failure
  still emitted, a downstream failure followed by the next batch, half a million bytes-less
  failures, none adjacent, giving one summary that keeps at most 100 ranges.
- `S3ObjectDecoder`: a body that breaks once is decoded once (no record twice); the visitor
  stopping early, and a stop, delete the local file; each returned status,
  the length, ETag and version id returned, `addS3Metadata`, and the limits for a given heap size.
- A differential test feeds the same bytes to the legacy deserializers and to `S3ObjectDecoder`
  with the 4.1 mapping, with at least one case per bullet of 4.2 that concerns decoding, and asserts
  both results; the legacy deserializers stay in core for the other sources.
- Configuration: the validator and DTO cases for each rule of 4 (exactly one of `encodingType` and
  `decoding`, `compressionType` only with `encodingType`, a bad regex and parquet with compression
  failing validation, `PARQUET` rejected, a nested `decoding` parsed by the config parser), and the
  whole `DecodeLimits` computed for a given heap size, per format, including the codec memory cap
  and the batch cap, with `TASK_MAX_MEMORY` set and unset.
- `DecodeLimits.maxCodecMemoryBytes`: an xz stream within it decodes and one over it is
  `Oversized`, with `maxDocumentBytes` below the stream's need; 0 keeps today's rule.
- DLQ error texts: two records that fail with the same exception class but different messages (two
  JSON syntax errors at different characters, two CSV rows with different column counts) give the
  same first line; an access-denied download gives `READ_FAILURE s3 object: S3Exception
  AccessDenied`; a downstream summary's first line is the `NodeExecutionException`'s; object and
  downstream summaries over many failures list at most 100 merged ranges; an object summary names
  the index the object ended at.
- A low-memory test (tag `lowMemory`, 64 MiB heap, so a 128 KiB batch cap) drives the converter
  through the `SimpleSourceCommand` path with a fake S3 body and a real buffered DLQ writer (batch
  size 10) over a fake upload: objects of hundreds of MB, gzip and plain, of JSON lines with 20
  short string fields, and of 32–256 KiB text records (so the batch cap is what fires), and an
  object of mostly bad records of up to 128 KiB; each decodes completely without OOM.

## 9. Known limitations

- A record already emitted from an object stays emitted when the object later fails; the DLQ entry
  says where it failed. A crash or stop before the commit emits the message's records again.
- One receive takes up to `maxNumberOfMessages` messages under one visibility timeout, and they are
  downloaded and processed one after another at the speed of the downstream. The timeout has to
  cover the whole receive, or the later messages are received again while waiting, and repeated
  expiries can reach `maxRetries` and dead-letter a message that never failed. The stored size does
  not bound the time either: a 200 MiB gzip object can hold many GiB of records. For large objects,
  raise `maxObjectSizeBytes` and `visibilityTimeoutSeconds` together, set `maxNumberOfMessages` to
  1, and give the pipeline disk for one object.
- A tree document near `maxDocumentBytes` (json `object`, xml `document`, yaml, text `whole`) can
  take most of the heap, and the first downstream node that copies it needs as much again; such
  objects need a larger `TASK_MAX_MEMORY`.
- A file with many bad records that carry bytes writes one DLQ entry per record, as FsSource does.
  When most of a DLQ batch of 100 are records near 128 KiB, its file passes 10 MB, and dynamicapi's
  self-healing skips DLQ files over 10 MB with every entry in them.
- A downstream failure cannot be recovered record by record: the notification is sent again and
  the whole object runs again (3.3).
- The DLQ writer's memory (about 100 MiB at the default batch of 100, 3.4) is a tenth of grid's
  minimum 1 GiB heap and does not shrink with it; a smaller `DLQ_BATCH_SIZE` lowers it.
- Outside grid nothing calls `terminate()` when the process is stopped, so every stopped run leaves
  its spool directory, with up to one object's file, in the working directory or `java.io.tmpdir`.
