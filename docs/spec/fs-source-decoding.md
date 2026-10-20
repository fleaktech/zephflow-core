# FsSource on the Generic Decoder

FsSource (`fssource`) reads the files of a folder on a schedule, from local disk, S3, GCS, Azure Blob
or SFTP. This change makes it decode them with the generic decoder (`docs/spec/generic-decoder.md`)
through the code the S3 realtime source already uses (`docs/spec/s3-realtime-source-decoding.md`,
"the S3 realtime spec" below), made backend-neutral. How a run picks files and records progress
does not change.

## 1. Why

`FsPayloadReader` has three read paths chosen by `encodingType`:

- JSON_OBJECT, TEXT and XML read the whole file into memory (up to `maxFileBytes`, default 256 MiB)
  and parse it at once, so a large file takes many times its size in heap.
- JSON_OBJECT_LINE and STRING_LINE are parsed in chunks of `chunkSizeBytes`; CSV and JSON_ARRAY are
  streamed. A line or record over `maxFileBytes` drops the whole file, checkpointed as done, and the
  records after it are lost.
- Only gzip and the seven legacy encodings: no parquet, avro, protobuf, yaml, multiline events, or
  zstd/xz.

Its DLQ entries hold a full stack trace with no hint of what to do, and their record index counts
from the start of a chunk, not of the file.

## 2. Goals and non-goals

**Goals**
- Memory bounded by fixed limits, not by file size, for every format: remote files are downloaded
  to local disk and decoded from there as a stream; records are emitted in bounded batches.
- Every format, codec and framing of the generic decoder, through a new `decoding` field.
- Existing configurations keep working (the legacy mapping of the S3 realtime spec 4.1).
- DLQ entries that say what failed and what to do (the S3 realtime spec 3.3), and file-relative
  record indexes.
- One copy of the decoding, limits, DLQ texts, batching and download, shared by both sources.

**Non-goals**
- The run model: schedule, listing, oldest-first selection, `maxFilesPerRun`, the 64 virtual
  buckets, per-bucket checkpoints, replicas, checkpoint scope, per-file completion, at-least-once
  re-reads, and how a downstream failure is handled (the user's decision F8) all stay as they are.
- The backends' listing, authentication and configuration.
- Resuming a file in the middle; a file that is read again is read from the start.
- Follow-ups after a core release, not here: zephflow-doc's FsSource node page (`maxFileBytes`,
  `chunkSizeBytes`, `file_too_large`, `nothing_deserialized`), dynamicapi's `FsDataSampler` and the
  webapp's FsSource form.
- Any change to the S3 realtime source's behavior.
- `exactObjectKey`'s mismatch with dynamicapi's translator (a separate ticket).

## 3. Design

### 3.1 Per file

A run lists, filters and selects files exactly as today, and checks `terminate()` before each file
as today. Each selected file is handled in order, on the replica's task process:

1. Its bytes are made available (3.2). A failure is a read failure (3.3).
2. The bytes are decoded by the compiled `RecordDecoder` with the limits of 3.4.
3. `Decoded` records are collected into a batch, emitted downstream when the batch reaches 1,000
   records or its size cap (3.4), and when the file ends. Records are not enriched (no path fields),
   as today.
4. Other outcomes are handled as in 3.3.
5. When the file ends, its summaries are written to the DLQ, the downloaded copy is deleted, and the
   file is checkpointed as done.

A file ends early, without its summaries and without a checkpoint, on a read failure or a downstream
failure (3.3); its bucket's watermark is held and the next run reads it again from the start.

### 3.2 Getting the bytes

- **`file` backend**: decoded in place (`ByteInput.ofFile`), with random access.
- **Remote backends**: the file is copied from `FileReader.open(key, 0)` into the spool directory and
  decoded from the copy (`ByteInput.ofFile`), by the download loop shared with the S3 realtime
  source (3.5):
  - A failure while reading the body deletes the partial copy and starts again from the beginning,
    up to 3 attempts in all. A failure of `open`, a failure to write the local copy (a full disk) and
    an interrupt are not retried. GCS opens lazily, so its open errors (403, 404) surface on the
    first read and are retried like body failures.
  - A copy shorter than the listed size is a premature end and a failed attempt. A copy longer than
    the listed size is accepted (the file changed after listing).
  - The copy never grows past `localSpoolMaxBytes` (default 1 GiB). A file listed over it is not
    copied; one that reaches it while copying (it grew, or GCS/Azure listed its size as 0) has its
    copy deleted. Either way the file is decoded straight from `FileReader.open(key, 0)`, wrapped so
    that an end before the listed size throws (no backend stream does this itself). A format that
    needs random access (parquet) cannot be streamed: the file is not read and gets a
    `FILE_TOO_LARGE` entry (3.3).
- The spool directory is the S3 realtime source's (its 3.2), with its own prefix: under the JVM's
  working directory (in grid the task's directory, deleted when the task exits), else under
  `java.io.tmpdir`; created on the first download and deleted when `execute` returns or throws.
  Files are handled one at a time, so it holds at most one copy. Leftovers are not swept at startup
  (6.1).

### 3.3 Outcomes and the DLQ

Decoder outcomes are handled as in the S3 realtime spec 3.3, through the same shared code, with
these differences:

| Case | FsSource |
|---|---|
| Unit named in texts | `file <urn>` instead of `object s3://…`; first lines say `file` instead of `object` |
| Record entry | Value: the record's bytes. Metadata: `__md_fs_source_urn` and, when at least 1, `__md_fs_source_record_index` (the decoder's `recordIndex`: line number for line framing, else record ordinal, counted from the start of the file). Node: the source |
| Terminal `Failed` of a whole document (`recordIndex` -1: json `object`, xml `document`, yaml `document`, protobuf `single`, avro `datum`; as `encodingType`, JSON_OBJECT and XML) with 1 byte to 128 KiB of bytes | A record entry, as above (without the index), so a small bad document keeps its bytes as it does today. Other terminal `Failed`s, such as a CSV header with duplicate names or an unclosed CSV quote, are summaries, as in the S3 realtime source |
| File summary (`MALFORMED`, `OVERSIZED`, and a `CORRUPT_COMPRESSION` that is not terminal, i.e. an avro block's own codec) | Value: empty. Metadata: `__md_fs_source_urn`. Node: the source |
| Terminal `CORRUPT_COMPRESSION`: the file's compression ends it (cut short, e.g. still being written; damaged; bytes after its end; a wrong compression setting) | A read failure (the user's decision F10) |
| Read failure: a failed download or open, `SourceReadException`, a terminal `CORRUPT_COMPRESSION`, any other exception while reading the file | No DLQ entry. Logged at error with what to check; the file ends early (3.1), counted as `read_error` |
| Downstream failure: any exception from `accept` (a node failing a record with a DLQ configured, or a node failing to initialize, with or without one) | No DLQ entry, as today. Logged at error; the file ends early (3.1), counted as `downstream_error`. Without a DLQ the runner drops and counts failed records without throwing, as today |
| File that needs random access and cannot be copied (3.2) | One entry `FILE_TOO_LARGE <format> file`, value empty, metadata `__md_fs_source_urn`; the file is checkpointed as done |

The shared run is parameterized so that the S3 realtime source keeps summarizing read failures,
downstream failures and terminal outcomes as it does today, while FsSource ends the file early on
read and downstream failures: the open batch is dropped, not emitted, and none of the file's
summaries are written; they are found again when the file is read again. Batches already emitted
stand, and they and the record entries already written are emitted and written again on the next
run.

Empty values are skipped by self-healing and by zephflow-plus's DLQ replay, so summaries are never
replayed as records. The `Action:` texts are the S3 realtime spec's, except:

- every file summary ends: "then upload the corrected file under a new name that matches
  `fileNameRegex` (with a `ts` no older than the files already read, if the regex has one); a name
  already read is not read again";
- when `maxFileBytes` is set, the `maxRecordBytes` and `maxDocumentBytes` actions say to raise
  `maxFileBytes` (and the task's heap to match) instead of only the heap; for parquet, the
  `maxDocumentBytes` action says to write smaller row groups (fewer columns if the footer is too
  large) or raise `maxFileBytes` and the heap;
- `FILE_TOO_LARGE`: "raise `localSpoolMaxBytes` above the file's size, with that much free disk where
  the task runs", followed by the rename text above;
- a non-terminal `CORRUPT_COMPRESSION` (an avro block's own codec): "a block's compressed data is
  damaged; rewrite the avro file", followed by the rename text above;
- a read failure's log line says what to check: the backend's read permission, the network, free
  disk where the task runs; for a compression failure, whether the file was still being written,
  the compression setting (`decoding.compression`, or gzip detected automatically for
  `encodingType`), or whether the compressed data is damaged or has bytes after its end, in which
  case the file has to be rewritten. It says that the file is read again on every run until then.

Each file read is logged at info once: the urn, the listed size, and whether it was read in place,
copied (with the spool file and the bytes copied), or streamed.

**Without a DLQ**, decoder failures are dropped and nothing is retried for them: record failures are
counted into the file's summaries instead of record entries, and each summary is logged once at
error, per file and kind. This is FsSource's policy only. A `FILE_TOO_LARGE` file is logged at error
with its urn, listed size and the cap, with or without a DLQ, as legacy logged a dropped file; the
run's closing error that counted files "over maxFileBytes" now counts them as over
`localSpoolMaxBytes`.

A DLQ write that throws is logged and the file goes on (the S3 realtime spec 3.3).

Counters: `inputEventCount` and `deserializeFailureCount` as in the S3 realtime source, except that
read failures are counted only in `skippedFileCount`, as today, not in `deserializeFailureCount`;
`inputEventSizeCount` from spans, as there; `skippedFileCount` keeps its `reason` tag with
`read_error`, `downstream_error` and `file_too_large`. As today, a run fails when it checkpointed no
file as emitted and skipped at least one (`read_error` or `downstream_error`).

### 3.4 Limits and memory

- **`maxFileBytes` set**: it is both `maxRecordBytes` and `maxDocumentBytes`, for every format
  (parquet included: the decoder then bounds the footer and row groups by it). **Unset**: the heap
  proportions of the S3 realtime spec 3.4, from the same nominal heap size.
- The codec memory cap (xz), the batch cap (min(4 MiB, heap/512) of estimated size, at most 1,000
  records) and the in-memory accounting are the S3 realtime spec's.
- `chunkSizeBytes` is accepted and ignored.
- Disk: at most one copy of up to `localSpoolMaxBytes` per task process.

### 3.5 Shared code

The S3-independent parts of `s3realtimesource` move to a shared package and both sources use them:

- the decoding inputs and legacy mapping (`S3Decoding` without `addS3Metadata`);
- the heap-derived limits (`S3DecodeLimits`), with the `maxFileBytes` override;
- the download loop of `S3ObjectDownloader` over an opener of `InputStream`s: attempts, the partial
  copy, the no-retry rules, the length check (exact for S3, at least the listed size for FsSource),
  the cap, and aborting a stream left early when it supports it (the AWS SDK's `Abortable`, as the
  FsSource S3 backend's stream does); the S3 `GetObject`, `NoSuchKey` and too-large handling stay in
  `s3realtimesource`. The streamed input of 3.2 aborts the same way when the file ends early;
- the per-unit run: batching, outcome handling, summaries, record ranges, DLQ texts and writes
  (`S3RealtimeRawDataConverter.ObjectRun` and its helpers), parameterized by the unit's name and
  noun, the values and metadata of entries, the retry phrase, the limit setting named in actions,
  the emitter, the stop signal, and the policies of 3.3 for read failures, downstream failures,
  terminal `CORRUPT_COMPRESSION`, terminal `Failed` with bytes, and the no-DLQ case;
- `SpoolDirectory`, with a prefix parameter.

`S3ObjectDecoder`, `S3Decoding`, `S3ObjectRef` and `S3RegionalClientProvider` stay in
`s3realtimesource` with their public signatures and delegate to the shared code. So do the
package-private members the S3 realtime tests use (`S3DecodeLimits`, `SpoolDirectory`,
`S3ObjectDownloader`, and the converter's `MAX_RECORD_ENTRY_BYTES`, `estimate`, `causeName` and
`IndexRanges`), as thin delegates with today's signatures and the S3 prefix, so every S3 realtime
test stays unchanged. The shared code gets its own tests.

## 4. Configuration

| Field | Change |
|---|---|
| `encodingType` | No longer required; mapped as in the S3 realtime spec 4.1 (gzip auto-detected, as today) |
| `decoding` | New: a `DecodingConfig` (decoder spec, 4). Exactly one of `encodingType` and `decoding` |
| `maxFileBytes` | Now the record and document limit (3.4), not a cap on what is buffered |
| `chunkSizeBytes` | Ignored (3.4) |
| `localSpoolMaxBytes` | New, optional, positive; default 1 GiB (3.2) |

The validator compiles the decoding configuration, so a bad one fails validation, not the first
file. `PARQUET` as `encodingType` stays rejected; parquet is available through `decoding`.

### 4.1 Behavior changes for legacy configurations

There are no production FsSource pipelines (the user's decision F4), so these change without
compatibility switches:

1. **An oversized line or CSV record no longer drops the file.** It becomes an `OVERSIZED` summary
   and the rest of the file is read; the records before it are emitted. Line formats and CSV check
   the record itself, with no read-ahead slack. Legacy's caps were looser: it checked a
   JSON_OBJECT_LINE or STRING_LINE line against `maxFileBytes` only once `chunkSizeBytes` (default
   16 MiB) was buffered without a newline, so a longer line in a smaller file was emitted, and it let
   a CSV record run 64 KiB past `maxFileBytes`; such lines and records are now `OVERSIZED` summaries. In JSON_ARRAY (and the library-parsed modes of
   `decoding`: xml `elements`, yaml `multi_document`) an oversized element still ends the file after
   the elements before it, as in legacy, with the decoder's 64 KiB slack; the file is done. A CSV
   quote left open to the end of the file ends it too (4.1.6).
2. **JSON_OBJECT, TEXT and XML are no longer read into memory whole.** A document over
   `maxDocumentBytes` is `OVERSIZED`.
3. **Without `maxFileBytes`, the limits come from the heap**, not 256 MiB: at grid's default 2 GiB
   heap a record may be 16 MiB and a document 32 MiB. A pipeline that reads larger documents sets
   `maxFileBytes` (and a heap to match).
4. **Record indexes count from the start of the file** (`__md_fs_source_record_index`), not from the
   start of a chunk.
5. **A CR-only file larger than `maxFileBytes` is no longer dropped as one line**: legacy chunked
   only at `\n`, so such a file was one oversized chunk; now its CR-only lines split in
   JSON_OBJECT_LINE and STRING_LINE whatever its size.
6. **The decoding differences of the S3 realtime spec 4.2** that concern decoding apply:
   - a JSON_OBJECT_LINE line holding an array is a failed record;
   - JSON_ARRAY: a malformed element ends the file after the elements before it with a terminal
     `MALFORMED` summary, where legacy wrote an empty-valued entry with the element's index;
   - CSV: a quote left open to the end of the file ends it after the rows before it, with a
     `MALFORMED` summary (no bytes), or an `OVERSIZED maxRecordBytes` summary when the rest of the
     file is over that limit, and the file is done, as legacy ended it;
   - CSV: other quoting errors (text after a closing quote) fail only their row, which becomes a
     record entry, and the rows after it are read; legacy ended the file at the first one with an
     empty-valued entry. A row of more than 16,384 columns fails, and a header of more than 16,384
     columns ends the file with no records, as a terminal `MALFORMED` summary; legacy accepted both.
     A short row's entry holds the row's bytes, where legacy re-formatted its values;
   - CSV: a row with more columns than the header fails; duplicate header names end the file with no
     records (legacy kept the last column), as a terminal `MALFORMED` summary, and the file is done;
     an empty header name becomes `_c<i>` (legacy failed the file);
   - a UTF-8 BOM is removed; UTF-16 and UTF-32 input decodes;
   - an empty file gives no records and no DLQ entry;
   - XML `DOCTYPE` and trailing content fail the file;
   - JSON_OBJECT_LINE lines with invalid UTF-8, or of whitespace other than spaces and tabs, fail.

   - gzip: bytes after the last member are `CORRUPT_COMPRESSION`, a read failure (F10), so the file
     is read again on every run until it is rewritten, and a run with no other file emitted fails
     (section 8); legacy ignored them and read the file once.

   Apart from CSV quoting, FsSource already dead-lettered bad lines and records one by one, so "bad
   data loses only itself" is not a change. A cut-short or damaged gzip is a read failure and read again next run, as in
   legacy, but the batches decoded before the failure are emitted on every run, where legacy emitted
   nothing from a file under `chunkSizeBytes` and the chunks before the failure from a larger one.
7. **DLQ texts** have a fixed first line and an `Action:` line before the details, instead of only a
   stack trace. A bad record over 128 KiB is listed in its file's summary instead of an entry with
   its bytes. Files now get summary entries with empty values (3.3).
8. **Without a DLQ, a file whose records all fail is done**, instead of being read again every run
   (`nothing_deserialized` is gone), and it counts as emitted, so a run whose files all fail to
   decode succeeds where it failed before.
9. **Remote files are downloaded to local disk** first (3.2), with up to 3 attempts.
10. **`chunkSizeBytes` has no effect**; batches are bounded as in 3.4.
11. **`skippedFileCount`** no longer has `nothing_deserialized`, and `file_too_large` means only a
    random-access file that cannot be copied. **`deserializeFailureCount`** counts every outcome
    other than `Decoded` and a terminal `CORRUPT_COMPRESSION` (a read failure), so an oversized line or a CSV header failure adds to it, where an oversized
    line was a `file_too_large` skip.
12. **`inputEventSizeCount`** is the end of the last record's span (the stored size for formats
    without spans, 0 for a file without records), where it was every decompressed byte read.

## 5. Compatibility

- No code outside core uses the `fssource` classes; dynamicapi builds FsSource configurations as
  JSON (`FsSourceTranslator`), which stay valid.
- The S3 realtime source's behavior and public classes do not change.
- DLQ values follow the S3 realtime spec 6: record entries hold bytes, summaries are empty.

## 6. Delivery

Small PRs into the integration branch `feature/fs-decoder`; the user merges it into main.

| PR | Content |
|---|---|
| 0 | This spec |
| 1 | The shared code of 3.5, with the S3 realtime source on it; its tests pass unchanged |
| 2 | FsSource's byte access of 3.2 (in place, capped download with retries, streaming fallback with the length check), with tests; not yet used. The snapshot test of 7, run against the current read path |
| 3 | FsSource on the shared run (3.1, 3.3, 3.4), `FsPayloadReader` removed; the snapshot test's expected results change only for the cases of 4.1 |
| 4 | `decoding`, `localSpoolMaxBytes` and the validator (4) |
| 5 | fleak-integration-tests: an FsSource test written like the S3 realtime source test there. Verified before the core merge with the suite's local clistarter option (`-PzephflowCoreLocalClistarter`) on a build of the integration branch; merged only after a core release with this change and the zephflow-plus bump, like the S3 realtime test |

### 6.1 Deviations from the agreed plan

- The agreed split was spec → shared pieces → FsSource read path → `decoding` → downstream/no-DLQ
  handling → integration test (F7). The read path is split into byte access (PR 2) and the run
  (PR 3) to keep each small, and there is no downstream PR: F8 keeps today's downstream handling, and
  the no-DLQ policy is part of the run in PR 3.
- plan-v5 §6.4 sweeps leftover spool files at startup. That is not done: two processes sharing a
  working directory would delete each other's copies, and in grid the task directory is deleted
  when the task exits. Outside grid a killed run can leave one copy (8).
- F1 (downstream summaries, file done) was replaced by F8 during the review, and F9 (only a
  cut-short compressed file is retried) by F10 (every compression failure that ends a file is).

## 7. Testing

- **Snapshot**: PR 2 records, with the current read path, what FsSource emits, checkpoints and
  dead-letters (values and metadata; texts are checked by the source tests) for a fixed set of
  files, as the expected results: each legacy encoding; valid, with bad records, gzip, cut-short
  gzip, gzip with trailing bytes, empty; CSV with duplicate and with empty header names, with a
  quote left open, with text after a closing quote; a JSON_OBJECT_LINE array line; CR-only lines;
  an oversized line; a JSON_OBJECT document over the document limit. PR 3 asserts the new path
  against them and changes the expected result only of the cases 4.1 names, so its diff shows each
  listed change. The other decoding differences of 4.1.6 (BOM, UTF-16/32, XML DOCTYPE, invalid
  UTF-8 and whitespace lines, extra CSV columns) are the legacy mapping's and are covered by the S3
  realtime source's `LegacyDifferentialTest` on the same deserializers and mapping.
- **Byte access** against fake `FileReader`s: a body that breaks at any offset is downloaded again
  with the partial copy deleted; 3 failed attempts; `open` failing is not retried; a full disk and an
  interrupt are not retried; a short copy; a longer copy accepted; a copy reaching
  `localSpoolMaxBytes` deleted and streamed; a file listed over it streamed; a file listed as 0 bytes;
  the streamed input throwing on a premature end for every backend's stream; parquet over the cap
  giving `FILE_TOO_LARGE`; the copy deleted on every outcome; the `file` backend read in place.
- **Source**: whole expected record lists per encoding (and, in PR 4, for parquet, avro and
  multiline through `decoding`); batch boundaries; each entry of 3.3 with value, metadata, node and text; file-relative
  indexes past what used to be chunk boundaries; an oversized record followed by good ones; actions
  with `maxFileBytes` set and unset (and, in PR 4, parquet's); a small malformed JSON_OBJECT and
  XML file dead-lettered with its bytes and one over 128 KiB summarized (and, in PR 4, a small bad
  yaml `document`, protobuf `single` and avro `datum`, each a record entry); CSV duplicate header
  names and an unclosed quote summarized, and an unclosed quote over `maxRecordBytes` as
  `OVERSIZED`; a JSON_ARRAY oversized element ending the file; a read failure, a cut-short gzip,
  gzip trailing bytes and (in PR 4) a cut-short bzip2 and snappy file each holding the
  watermark, dropping the open batch and the file read again next run, with its summaries written
  once and the whole counter set asserted (`deserializeFailureCount` only for the record outcomes
  before the failure, `skippedFileCount{reason=read_error}` +1); (in PR 4) an avro block with a
  damaged codec summarized; the info line per file; no DLQ; the run
  failing when no file was emitted.
- **Downstream**, through a real `DagExecutor`: a node failing a record with a DLQ (file held, read
  again), the same without a DLQ (file done, failure counted by the runner), and a node failing to
  initialize, with and without a DLQ (file held, run fails).
- **S3 realtime**: its tests pass after PR 1 unchanged.
- **Configuration**: exactly one of `encodingType` and `decoding`; a bad `decoding` failing
  validation; `PARQUET` rejected; `localSpoolMaxBytes` positive; `chunkSizeBytes` accepted.
- **Low memory**: the S3 realtime low-memory test's shapes, run through FsSource with the `file`
  backend at a 64 MiB heap, decode completely without OOM.
- **Integration** (PR 5, fleak-integration-tests, written like `s3realtime/S3RealtimeSourceTest`:
  LocalStack S3, a grid worker, Kafka output, a LocalStack DLQ bucket): two pipelines, each over its
  own prefix of one bucket and with a fixed `CHECKPOINT_SCOPE` in the job context, one with
  `encodingType` JSON_OBJECT_LINE over a gzipped file holding a bad line, one with `decoding` parquet
  over a parquet file. The good records reach Kafka; the bad line is dead-lettered with its `Action:`
  line and file-relative index; the worker's task log has the info line showing each file was
  copied to the spool; a second run of each pipeline, through the grid-injected checkpoint, reads
  nothing new. It runs grid without the appserver, so it replaces a local-stack run (F12).
- **On dev** (after the release and the dynamicapi bump): a QA ticket covers what the integration
  test does not, the pipeline built through the UI and dynamicapi's translator, with a DLQ through
  self-healing.

## 8. Known limitations

- A file over `localSpoolMaxBytes` is streamed from the backend, so a broken connection is a read
  failure and the whole file is read again on the next run, with its records emitted again.
- A parquet file over `localSpoolMaxBytes` is not read: it is done, so it needs the cap raised and
  the file uploaded again under a new name.
- Files are assumed complete when listed, apart from a compressed stream that fails (3.3). These
  are done as read so far: an uncompressed file still being written (a partial last record becomes
  a `MALFORMED` record entry, as today); a compressed file cut exactly between gzip members, zstd or
  lz4 frames, or snappy chunks, which ends cleanly; an avro or parquet file still being written,
  which ends with a `MALFORMED` summary. Writers should write under a temporary name and rename, or
  `fileNameRegex` should exclude files in progress.
- A compressed file that never decodes (damaged, bytes after its end, a wrong compression setting)
  is read again on every run and holds its bucket's watermark until it is rewritten (F10), shown by
  the error log and `read_error`. When no other file of the run is emitted, the run fails, and grid
  retries the task (dynamicapi submits it with 3 retries), so the batches decoded before the failure are emitted and
  its record entries written again on each attempt. Legacy did the same for its read errors.
- Outside grid, a run killed while copying leaves one copy of up to `localSpoolMaxBytes` in the
  working directory or `java.io.tmpdir`; nothing removes it (6.1).
- DLQ replay (zephflow-plus's `dlqsource`, self-healing) skips the empty-valued summaries but deletes
  the DLQ files holding them, so the summaries of files already done have to be acted on before a
  replay.
- A read or downstream failure that repeats every run (a corrupt gzip, a node that always fails a
  record with a DLQ configured) holds its bucket's watermark, and its file's records and record
  entries are emitted again on every run, as today.
- With `maxFileBytes` set, avro, protobuf and parquet lose the bounds the heap rule gives them: an
  avro block can need 150–250 times `maxDocumentBytes`, a protobuf message about 300 times its limit,
  and parquet's footer and row group budget becomes `maxFileBytes` with rows up to it, so a large
  `maxFileBytes` needs a heap to match.
- A file is checkpointed while its DLQ entries may still sit in the DLQ writer's buffer; a crash in
  that window loses them, as today.
- Several replicas on one worker each use up to `localSpoolMaxBytes` of its disk.
- The memory limits of the S3 realtime spec 9 (tree documents near the limit, the DLQ writer's
  buffer, DLQ files over 10 MB skipped by self-healing) apply here too.
