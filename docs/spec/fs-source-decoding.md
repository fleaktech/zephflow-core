# FsSource on the Generic Decoder

FsSource (`fssource`) reads the files of a folder on a schedule, from local disk, S3, GCS, Azure Blob
Storage or SFTP. This change makes it decode files with the generic decoder
(`docs/spec/generic-decoder.md`). It does so through the code the S3 realtime source already uses
(`docs/spec/s3-realtime-source-decoding.md`, called "the S3 realtime spec" below). That code moves to
a shared package, so both sources run one copy of it. How a run selects files and records its
progress does not change.

## 1. Why

Before this change, `FsPayloadReader` read a file in one of three ways, chosen by `encodingType`:

- **JSON_OBJECT, TEXT and XML** read the whole file into memory (up to `maxFileBytes`, default
  256 MiB) and parsed it at once. A large file could take several times its size in heap.
- **JSON_OBJECT_LINE and STRING_LINE** were parsed in chunks of `chunkSizeBytes`. **CSV and
  JSON_ARRAY** were streamed. In all four, a line or record over `maxFileBytes` made the source skip
  the whole file and checkpoint it as done, so the records after it were never read.
- **Formats:** only the seven legacy encodings and gzip. No parquet, avro, protobuf, yaml, multiline
  events, zstd or xz.

Its DLQ entries held a stack trace with no hint of what to do. Their record index counted from the
start of a chunk, not from the start of the file.

## 2. Goals and non-goals

**Goals**
- Memory is bounded by fixed limits for every format, whatever the file's size. Remote files are
  copied to local disk and decoded from there as a stream, and records are emitted in bounded
  batches.
- Every format, compression and framing of the generic decoder is available through a new
  `decoding` field.
- Existing configurations keep working, through the `encodingType` mapping of the S3 realtime
  spec 4.1.
- DLQ entries say what failed and what to do (the S3 realtime spec 3.3), and record indexes count
  from the start of the file.
- The decoding inputs, limits, DLQ texts, batching and download exist once, shared by both sources.

**Non-goals**
- The run model does not change. This covers the schedule, the listing, oldest-first selection,
  `maxFilesPerRun`, the 64 virtual buckets and their checkpoints, replicas and checkpoint scope. A
  file is still marked done only after all of it is handled, and a file not marked done is read
  again on a later run (at-least-once).
- Downstream failures are handled as before (3.3).
- The backends' listing, authentication and configuration do not change.
- A file is never resumed in the middle. A file that is read again is read from its start.
- The S3 realtime source's behavior does not change.

## 3. Design

### 3.1 Per file

A run lists, filters and selects files as before. It checks `terminate()` before each file, as
before; a file already being read when the run is told to terminate is read to its end. The selected
files are handled one at a time, in order:

1. The file's bytes are made available (3.2). A failure here is a read failure (3.3).
2. The compiled `RecordDecoder` decodes the bytes, with the limits of 3.4.
3. Decoded records are collected into a batch. The batch is emitted downstream when it reaches
   1,000 records or its size cap (3.4), and at the end of the file. Records are not enriched with
   path fields, as before.
4. The decoder's other outcomes (bad records, oversized records, damaged data) are handled as in
   3.3.
5. At the end of the file, its summary entries (3.3) are written to the DLQ, the local copy is
   deleted, and the file is checkpointed as done.

A read failure or a downstream failure (3.3) ends the file early. The batch being collected is
dropped, not emitted. No summary entries are written. The file is not checkpointed, so its bucket's
watermark stays where it was, and the next run reads the file again from the start.

### 3.2 Getting the bytes

**`file` backend:** the file is decoded where it is (`ByteInput.ofFile`), with random access.

**Remote backends:** the file is copied from `FileReader.open(key, 0)` into a local spool directory
and decoded from the copy (`ByteInput.ofFile`). The copy uses the download loop shared with the S3
realtime source (3.5):

- **Retries.** If reading the body fails, the partial copy is deleted and the copy starts again from
  the beginning, up to 3 attempts in all. A failure of `open`, a failure to write the local copy
  (for example a full disk) and an interrupt are not retried. GCS opens a file lazily, so its open
  errors (403, 404) appear on the first read; they are retried like body failures.
- **Length.** A copy shorter than the listed size is a failed attempt. A copy longer than the listed
  size is accepted, because the file changed after it was listed.
- **Size cap.** The copy never grows past `localSpoolMaxBytes` (default 1 GiB). A file that does not
  fit is not kept on disk. That is a file listed over the cap, which is not copied at all, or a file
  whose copy reaches the cap because the file grew or because GCS or Azure listed its size as 0, in
  which case the partial copy is deleted.
- **A file that does not fit** is decoded from a stream opened with `FileReader.open(key, 0)`, in the
  same bounded memory. The stream is wrapped so that ending before the listed size throws, because
  no backend stream checks this itself. If the stream breaks, the file cannot resume where it
  stopped: it is a read failure, and the next run reads it again from the start.
- **Parquet** needs random access, so it cannot be decoded from a stream. A parquet file that does
  not fit is not read; it gets a `FILE_TOO_LARGE` entry (3.3) and is checkpointed as done.

**Spool directory.** It works like the S3 realtime source's (its 3.2), with its own name prefix
(`fs-spool-`). It is created under the JVM's working directory, or under `java.io.tmpdir` when the
working directory is not writable. It is created on the first copy and deleted when `execute`
returns or throws. Files are handled one at a time, so it holds at most one copy. Leftover
directories from a killed process are not removed at startup (6.1).

### 3.3 Outcomes and the DLQ

The decoder's outcomes are handled as in the S3 realtime spec 3.3, by the same shared code, with
the differences below.

**Two kinds of DLQ entries.** A **record entry** is written as soon as a bad record is read; its
value is the record's bytes. A **summary entry** is written at the end of the file; there is one per
file and kind of problem, it lists every place the problem occurred, and its value is empty.

| Case | FsSource |
|---|---|
| How the file is named in texts | `file <urn>` instead of `object s3://…`; first lines say `file` instead of `object` |
| Record entry (a bad record of at most 128 KiB) | Value: the record's bytes. Metadata: `__md_fs_source_urn`, and `__md_fs_source_record_index` when the record's index is at least 1. The index is the decoder's `recordIndex`, counted from the start of the file: the line number for line framing, else the record's ordinal. Node: the source |
| A terminal `Failed` of a whole document of 1 byte to 128 KiB | A record entry, as above, without the index, so a small bad document keeps its bytes as before. A whole document is a decoder outcome with `recordIndex` -1: json `object`, xml `document`, yaml `document`, protobuf `single`, avro `datum`, and the `encodingType`s JSON_OBJECT and XML. Other terminal `Failed`s, such as a CSV header with duplicate names or an unclosed CSV quote, are summary entries, as in the S3 realtime source |
| Summary entry: `MALFORMED`, `OVERSIZED`, and a `CORRUPT_COMPRESSION` that does not end the file (an avro block's own codec) | Value: empty. Metadata: `__md_fs_source_urn`. Node: the source |
| `CORRUPT_COMPRESSION` that ends the file: the file's compression is cut short (for example still being written), damaged, followed by extra bytes, or set wrongly | A read failure |
| Read failure: a failed copy or open, a `SourceReadException`, a `CORRUPT_COMPRESSION` that ends the file, or any other exception while reading the file | No DLQ entry. Logged at error with what to check. The file ends early (3.1) and is counted as `read_error` |
| Downstream failure: any exception from `accept`, such as a node failing a record while a DLQ is configured, or a node failing to initialize, with or without a DLQ | No DLQ entry, as before. Logged at error. The file ends early (3.1) and is counted as `downstream_error`. Without a DLQ, the runner drops and counts failed records without throwing, as before |
| A parquet file that does not fit in `localSpoolMaxBytes` (3.2) | One entry, first line `FILE_TOO_LARGE <format> file`, value empty, metadata `__md_fs_source_urn`. The file is checkpointed as done |

The shared run takes a policy. The S3 realtime source keeps its own: read failures, downstream
failures and outcomes that end the object become summary entries. FsSource ends the file early on
read and downstream failures. When a file ends early, batches already emitted and record entries
already written stand; the next run emits and writes them again.

A tool that replays DLQ values as records has to skip empty values, so that summary entries are
never replayed as records.

**`Action:` texts** are those of the S3 realtime spec, with these differences:

- A file that was read is not read again, so every summary entry ends with how to get a corrected
  file read:
  - with a `fileNameRegex`: "then upload the corrected file under a new name that matches
    fileNameRegex (with a ts no older than the files already read, if the regex has one); a name
    already read is not read again";
  - without one: "then upload the corrected file under a new name; a name already read is not read
    again";
  - with an `exactObjectKey`: "then upload the corrected file under a new key and set exactObjectKey
    to it; a key already read is not read again". With a `fileNameRegex` as well, "a new key" reads
    "a new key whose file name matches fileNameRegex".
- When `maxFileBytes` is set, the `maxRecordBytes` and `maxDocumentBytes` actions say to raise
  `maxFileBytes` (and the task's heap to match), instead of only the heap. For parquet, the
  `maxDocumentBytes` action says to write smaller row groups (or fewer columns, if the footer is too
  large), or to raise `maxFileBytes` and the heap.
- `FILE_TOO_LARGE`: "raise localSpoolMaxBytes above the file's size, with that much free disk where
  the task runs", followed by the text above about a new name. The details give the listed size and
  say whether the file was listed over the cap or its copy grew past it.
- A `CORRUPT_COMPRESSION` that does not end the file (an avro block's own codec): "a block's
  compressed data is damaged; rewrite the avro file", followed by the text above about a new name.
- A read failure's log line says what to check: the backend's read permission, the network, and
  free disk where the task runs. For a compression failure it also says to check whether the file
  was still being written, the compression setting (`decoding.compression`, or gzip detected
  automatically for `encodingType`), and whether the compressed data is damaged or followed by extra
  bytes, in which case the file has to be rewritten. It says that the file is read again on every
  run until then.

**Info log.** Each file read is logged at info once, with the urn, the listed size, and how it was
read: in place, from a local copy (with the copy's path and the bytes copied), or streamed (with
the reason).

**Without a DLQ**, decoder failures are dropped and the file is not read again for them. Record
failures are counted into the file's summaries instead of record entries, and each summary is logged
once at error. A `FILE_TOO_LARGE` file is logged at error with its urn, listed size and the cap,
with or without a DLQ. The run's closing error, which counted files "over maxFileBytes", now counts
files that do not fit in `localSpoolMaxBytes`.

**A DLQ write that throws** is logged, and the file goes on (the S3 realtime spec 3.3).

**Counters.**
- `inputEventCount` and `deserializeFailureCount` are counted as in the S3 realtime source, except
  that read failures are counted only in `skippedFileCount`, as before, not in
  `deserializeFailureCount`.
- `inputEventSizeCount` is counted from the records' byte spans, as in the S3 realtime source.
- `skippedFileCount` keeps its `reason` tag, with the values `read_error`, `downstream_error` and
  `file_too_large`.
- As before, a run fails when it checkpointed no file as emitted and skipped at least one file for a
  `read_error` or `downstream_error`.

### 3.4 Limits and memory

- **`maxFileBytes` set:** it is both `maxRecordBytes` and `maxDocumentBytes`, for every format. For
  parquet, the decoder then bounds the footer and row groups by it.
- **`maxFileBytes` not set:** the limits are proportions of the heap, as in the S3 realtime spec 3.4,
  from the same nominal heap size.
- The codec memory cap (xz), the batch cap and the in-memory accounting are those of the S3 realtime
  spec. A batch holds at most 1,000 records and at most min(4 MiB, heap/512) of estimated size.
- `chunkSizeBytes` is accepted and ignored.
- Disk: at most one copy of up to `localSpoolMaxBytes` per task process.

### 3.5 Shared code

The parts of `s3realtimesource` that do not depend on S3 move to the package
`io.fleak.zephflow.lib.commands.filedecode`, and both sources use them:

- the decoding inputs and the `encodingType` mapping (`S3Decoding` without `addS3Metadata`);
- the heap-derived limits (`S3DecodeLimits`), with the `maxFileBytes` override;
- the download loop of `S3ObjectDownloader`, over an opener of `InputStream`s. It covers the
  attempts, the partial copy, the rules for what is not retried, the length check (an exact length
  for S3, at least the listed size for FsSource), the size cap, and aborting a stream left early when
  the stream supports it (the AWS SDK's `Abortable`, which the FsSource S3 backend's stream is). The
  S3 `GetObject`, `NoSuchKey` and too-large handling stay in `s3realtimesource`. The streamed input
  of 3.2 aborts the same way when the file ends early;
- the per-unit run: batching, outcome handling, summaries, record ranges, DLQ texts and writes
  (`S3RealtimeRawDataConverter.ObjectRun` and its helpers). It is parameterized by the unit's name
  and noun, the values and metadata of entries, the text about a new name, the limit setting named
  in actions, the emitter, the stop signal, and the policies of 3.3;
- `SpoolDirectory`, with a prefix parameter.

`S3ObjectDecoder`, `S3Decoding`, `S3ObjectRef` and `S3RegionalClientProvider` stay in
`s3realtimesource`, with their public signatures, and delegate to the shared code. So do the
package-private members the S3 realtime tests use (`S3DecodeLimits`, `SpoolDirectory`,
`S3ObjectDownloader`, and the converter's `MAX_RECORD_ENTRY_BYTES`, `estimate`, `causeName` and
`IndexRanges`). They become thin delegates with their current signatures and the S3 prefix, so every
S3 realtime test stays unchanged. The shared code has its own tests.

## 4. Configuration

| Field | Change |
|---|---|
| `encodingType` | No longer required. Mapped as in the S3 realtime spec 4.1; gzip is still detected automatically |
| `decoding` | New: a `DecodingConfig` (the generic decoder spec, 4). Exactly one of `encodingType` and `decoding` must be set |
| `maxFileBytes` | Now the record and document limit (3.4), not a cap on what is buffered |
| `chunkSizeBytes` | Accepted and ignored (3.4) |
| `localSpoolMaxBytes` | New, optional, positive; default 1 GiB (3.2) |

The validator compiles the decoding configuration, so a bad one fails validation, not the first
file. `PARQUET` as an `encodingType` is still rejected; parquet is available through `decoding`.

### 4.1 Behavior changes for existing configurations

Existing configurations still validate and run. These behaviors change, with no switch to keep the
old ones:

1. **An oversized line or CSV record no longer drops the file.** It becomes an `OVERSIZED` summary,
   and the rest of the file is read; the records before it are emitted.
   - Line formats and CSV check each record against the limit, with no read-ahead slack. The old
     checks were looser. JSON_OBJECT_LINE and STRING_LINE checked a line against `maxFileBytes` only
     once `chunkSizeBytes` (default 16 MiB) was buffered without a newline, so a longer line in a
     smaller file was emitted. A CSV record could run 64 KiB past `maxFileBytes`. Such lines and
     records are now `OVERSIZED` summaries.
   - In JSON_ARRAY, and in the library-parsed modes of `decoding` (xml `elements`, yaml
     `multi_document`), an oversized element still ends the file after the elements before it, as
     before, with the decoder's 64 KiB slack; the file is done. A CSV quote left open to the end of
     the file ends it too (item 6).
2. **JSON_OBJECT, TEXT and XML are no longer read into memory whole.** A document over
   `maxDocumentBytes` is `OVERSIZED`.
3. **Without `maxFileBytes`, the limits come from the heap**, not from a 256 MiB default. At a 2 GiB
   heap a record may be 16 MiB and a document 32 MiB. A pipeline that reads larger documents sets
   `maxFileBytes`, and a heap to match.
4. **Record indexes count from the start of the file** (`__md_fs_source_record_index`), not from the
   start of a chunk.
5. **A file with only CR line endings, larger than `maxFileBytes`, is no longer dropped as one
   line.** The old code split chunks only at `\n`, so such a file was one oversized chunk. Now
   JSON_OBJECT_LINE and STRING_LINE split its lines at CR whatever its size.
6. **The decoding differences of the S3 realtime spec 4.2 apply:**
   - JSON_OBJECT_LINE: a line holding an array is a failed record.
   - JSON_ARRAY: a malformed element ends the file after the elements before it, with a `MALFORMED`
     summary. Before, it wrote an empty-valued entry with the element's index.
   - CSV, a quote left open to the end of the file: the file ends after the rows before it, with a
     `MALFORMED` summary (no bytes), or an `OVERSIZED maxRecordBytes` summary when the rest of the
     file is over that limit. The file is done; the old code ended the file there too.
   - CSV, other quoting errors (text after a closing quote): only the row fails. It becomes a record
     entry, and the rows after it are read. Before, the file ended at the first such row with an
     empty-valued entry.
   - CSV, a row of more than 16,384 columns fails. A header of more than 16,384 columns ends the file
     with no records, as a `MALFORMED` summary. Before, both were accepted.
   - CSV, a row shorter than the header: its record entry holds the row's bytes. Before, it held the
     row's values, formatted again.
   - CSV, a row with more columns than the header fails. Duplicate header names end the file with no
     records, as a `MALFORMED` summary, and the file is done; before, the last column with the name
     was kept. An empty header name becomes `_c<i>`; before, the file failed.
   - A UTF-8 BOM is removed. UTF-16 and UTF-32 input decodes.
   - An empty file gives no records and no DLQ entry.
   - XML: a `DOCTYPE` or content after the root element fails the file.
   - JSON_OBJECT_LINE: a line with invalid UTF-8, or a line of whitespace other than spaces and tabs,
     fails.
   - gzip followed by extra bytes after its last member is a `CORRUPT_COMPRESSION` that ends the
     file, so it is a read failure. The file is read again on every run until it is rewritten, and a
     run in which no other file is emitted fails (section 8). Before, the extra bytes were ignored
     and the file was read once.

   Apart from CSV quoting, bad lines and records were already dead-lettered one by one, so "bad data
   loses only itself" is not a change. A gzip file that is cut short or damaged is a read failure
   and is read again on the next run, as before. The difference is what is emitted before the
   failure: now the batches decoded before it, on every run. Before, nothing was emitted from a file
   smaller than `chunkSizeBytes`, and the chunks before the failure from a larger one.
7. **DLQ texts** have a fixed first line and an `Action:` line before the details, instead of only a
   stack trace. A bad record over 128 KiB is listed in its file's summary instead of getting an entry
   with its bytes. Files now get summary entries with empty values (3.3).
8. **Without a DLQ, a file whose records all fail is done.** Before, it was read again on every run
   (`nothing_deserialized` is gone). It counts as emitted, so a run in which every file fails to
   decode succeeds, where it failed before.
9. **Remote files are copied to local disk** before they are decoded (3.2), with up to 3 attempts.
10. **`chunkSizeBytes` has no effect.** Batches are bounded as in 3.4.
11. **`skippedFileCount`** no longer has the reason `nothing_deserialized`, and `file_too_large`
    now means only a parquet file that does not fit in `localSpoolMaxBytes`.
    **`deserializeFailureCount`** counts every outcome other than `Decoded` and a
    `CORRUPT_COMPRESSION` that ends the file (a read failure). So an oversized line or a CSV header
    failure adds to it; before, an oversized line was a `file_too_large` skip.
12. **`inputEventSizeCount`** is the end of the last record's byte span (the stored size for formats
    whose records have no spans, 0 for a file without records). Before, it was every decompressed
    byte read.

## 5. Compatibility

- Existing FsSource configurations stay valid: the change adds two optional fields and makes
  `encodingType` optional.
- The S3 realtime source's behavior and public classes do not change.
- DLQ values follow the S3 realtime spec 6: record entries hold bytes, summary entries are empty.

## 6. Delivery

Small PRs into the integration branch `feature/fs-decoder`, which is then merged into main.

| PR | Content |
|---|---|
| 0 | This spec |
| 1 | The shared code of 3.5, with the S3 realtime source moved onto it; its tests pass unchanged |
| 2 | FsSource's byte access of 3.2 (in place, a capped copy with retries, and the streamed fallback with the length check), with tests, not yet used. The snapshot test of 7, run against the old read path |
| 3 | FsSource on the shared run (3.1, 3.3, 3.4), and `FsPayloadReader` removed. The snapshot test's expected results change only for the cases of 4.1 |
| 4 | `decoding`, `localSpoolMaxBytes` and the validator (4) |

### 6.1 Deviations from the original plan

- **PR split.** The plan had one PR for the read path and one for downstream and no-DLQ handling.
  The read path is split into byte access (PR 2) and the run (PR 3) to keep each PR small. There is
  no downstream PR: downstream failures are handled as before, and the no-DLQ policy is part of the
  run in PR 3.
- **No startup sweep of leftover spool directories.** The plan swept them at startup. That is not
  done, because two processes sharing a working directory would delete each other's copies. A runner
  that gives each task its own working directory and deletes it when the task exits removes them
  anyway. Elsewhere, a killed run can leave one copy behind (8).
- **Downstream failures and compression failures.** The plan first marked a file done after a
  downstream failure, with a summary entry per failing node, and treated only a cut-short compressed
  file as a read failure. During review, both changed: a downstream failure keeps the old handling
  (the file is read again), and every compression failure that ends a file is a read failure.

## 7. Testing

- **Snapshot.** PR 2 records what FsSource emits, checkpoints and dead-letters with the old read path
  for a fixed set of files: records, DLQ values and metadata (texts are checked by the source tests),
  and checkpoints. The set covers each legacy encoding; valid files, files with bad records, gzip,
  cut-short gzip, gzip followed by extra bytes, and empty files; CSV with duplicate and with empty
  header names, with a quote left open and with text after a closing quote; a JSON_OBJECT_LINE line
  holding an array; CR-only lines; an oversized line; and a JSON_OBJECT document over the document
  limit. PR 3 checks the new path against these results and changes only the expected results of the
  cases 4.1 names, so its diff shows each listed change. The other decoding differences of 4.1 item 6
  (BOM, UTF-16/32, XML `DOCTYPE`, invalid UTF-8 and whitespace lines, extra CSV columns) come from
  the `encodingType` mapping, and the S3 realtime source's `LegacyDifferentialTest` covers them on
  the same deserializers and mapping.
- **Byte access**, against fake `FileReader`s:
  - a body that breaks at any offset is copied again, with the partial copy deleted;
  - 3 failed attempts;
  - `open` failing is not retried, nor are a full disk and an interrupt;
  - a short copy, and a longer copy accepted;
  - a copy reaching `localSpoolMaxBytes` deleted and streamed; a file listed over it streamed; a file
    listed as 0 bytes;
  - the streamed input throwing on an early end;
  - parquet that does not fit giving `FILE_TOO_LARGE`;
  - the copy deleted on every outcome;
  - the `file` backend read in place.
- **Source:**
  - whole expected record lists per encoding, and, in PR 4, for parquet, avro and multiline through
    `decoding`;
  - batch boundaries;
  - each entry of 3.3, with value, metadata, node and text;
  - indexes counted from the start of the file, past where the old chunk boundaries were;
  - an oversized record followed by good ones;
  - actions with `maxFileBytes` set and not set, and, in PR 4, parquet's;
  - a small malformed JSON_OBJECT and XML file dead-lettered with its bytes, and one over 128 KiB
    summarized; in PR 4, a small bad yaml `document`, protobuf `single` and avro `datum`, each a
    record entry;
  - CSV duplicate header names and an unclosed quote summarized, and an unclosed quote over
    `maxRecordBytes` as `OVERSIZED`;
  - a JSON_ARRAY oversized element ending the file;
  - a read failure, a cut-short gzip, gzip followed by extra bytes, and, in PR 4, a cut-short bzip2
    and snappy file. Each holds the watermark, drops the open batch, and is read again on the next
    run, with its summaries written once and the whole counter set checked
    (`deserializeFailureCount` only for the record outcomes before the failure,
    `skippedFileCount{reason=read_error}` +1);
  - in PR 4, an avro block with a damaged codec summarized;
  - the info line per file; no DLQ; the run failing when no file was emitted.
- **Downstream**, through a real `DagExecutor`: a node failing a record with a DLQ (the file is held
  and read again), the same without a DLQ (the file is done, and the runner counts the failure), and
  a node failing to initialize, with and without a DLQ (the file is held and the run fails).
- **S3 realtime:** its tests pass unchanged after PR 1.
- **Configuration:** exactly one of `encodingType` and `decoding`; a bad `decoding` failing
  validation; `PARQUET` rejected; `localSpoolMaxBytes` positive; `chunkSizeBytes` accepted.
- **Low memory:** the shapes of the S3 realtime low-memory test, run through FsSource with the `file`
  backend at a 64 MiB heap, decode completely without running out of memory.

## 8. Known limitations

- A file that does not fit in `localSpoolMaxBytes` is decoded from a stream on the backend. If the
  connection breaks, it is a read failure, and the whole file is read again on the next run, with
  its records emitted again.
- A parquet file that does not fit in `localSpoolMaxBytes` is not read and is marked done. To read
  it, raise the cap and upload the file again under a new name.
- Files are assumed complete when listed, except for a compressed file whose decompression fails
  (3.3). These files are marked done with what was read so far:
  - an uncompressed file still being written: a partial last record becomes a `MALFORMED` record
    entry, as before;
  - a compressed file cut exactly between gzip members, zstd or lz4 frames, or snappy chunks: it
    ends cleanly;
  - an avro or parquet file still being written: it ends with a `MALFORMED` summary.

  Writers should write under a temporary name and rename the file when it is complete, or
  `fileNameRegex` should exclude files in progress.
- A compressed file that never decodes (damaged, followed by extra bytes, or with a wrong
  compression setting) is read again on every run, and holds its bucket's watermark until it is
  rewritten. The error log and `read_error` show it. When no other file of the run is emitted, the
  run fails. If the runner retries a failed run, each attempt emits the batches decoded before the
  failure again and writes its record entries again. The old code did the same for its read errors.
- A run killed while copying can leave one copy of up to `localSpoolMaxBytes` in the working
  directory or `java.io.tmpdir`, unless the runner deletes the task's working directory (6.1).
- A tool that replays a DLQ and deletes the DLQ files it has read also deletes the summary entries
  in them. Act on the summaries of files already done before such a replay.
- A read or downstream failure that repeats on every run (a damaged gzip file, or a node that always
  fails a record while a DLQ is configured) holds its bucket's watermark. The file's records and
  record entries are emitted and written again on every run, as before.
- With `maxFileBytes` set, avro, protobuf and parquet lose the bounds the heap rule gives them. An
  avro block can need 150–250 times `maxDocumentBytes`, and a protobuf message about 300 times its
  limit. Parquet's footer and row group budget becomes `maxFileBytes`, with rows up to that size. A
  large `maxFileBytes` needs a heap to match.
- A file is checkpointed while its DLQ entries may still be in the DLQ writer's buffer. A crash at
  that moment loses them, as before.
- Several replicas on one machine each use up to `localSpoolMaxBytes` of its disk.
- The memory limits of the S3 realtime spec 9 (tree documents near the limit, the DLQ writer's
  buffer) apply here too.
