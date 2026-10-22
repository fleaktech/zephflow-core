# Message Sources on the Generic Decoder

Seven sources turn each fetched message into records through `BytesRawDataConverter` and a legacy
`FleakDeserializer`: Kafka, Kinesis, Azure Event Hubs, MQTT, ActiveMQ, StdIn and FileSource. This
change makes them decode with the generic decoder (`docs/spec/generic-decoder.md`), as the S3
realtime source and FsSource already do (`docs/spec/s3-realtime-source-decoding.md`,
`docs/spec/fs-source-decoding.md`). How each source fetches, acknowledges and commits does not
change.

## 1. Why

- **Formats.** These sources support only the seven legacy encodings. They cannot read avro,
  protobuf, parquet, yaml or multiline text, and only Kafka and Kinesis can decompress (gzip only).
- **DLQ texts.** A message that fails to decode is written to the DLQ with a stack trace as its
  error text, with no hint of what to do.
- **Two decoders.** The S3 realtime source and FsSource already use the generic decoder. Moving the
  remaining byte sources onto it means one decoder, one `decoding` config and one set of DLQ texts.

## 2. Goals and non-goals

**Goals**
- All seven sources decode through the generic decoder. A legacy `encodingType` runs through the
  same mapping the S3 realtime source and FsSource use (`SourceDecoding`).
- Each source accepts a new `decoding` field; exactly one of `encodingType` and `decoding` is set.
- A DLQ entry for a message that fails to decode has a fixed first line, an `Action:` line, then the
  details. A failed write of that entry is logged and the source goes on.
- Every behavior change for an existing configuration is listed (4.1).

**Non-goals**
- Several records per message. A message is expected to hold one record; when it holds more (for
  example JSON lines), they decode all or nothing (3.1).
- Size limits. A message is in memory when it is fetched; the decoder runs without record or
  document limits (3.4).
- Fetching, acknowledging, committing, consumer groups, checkpoints and QoS.
- Downstream failures: they keep today's handling (3.3).
- Message metadata that sources do not capture today (Kafka topic, partition, offset, headers;
  MQTT topic).
- SQS, Pub/Sub, IoT Hub, Elasticsearch, syslog UDP and the S3 file reader, which have their own
  converters.
- Removing `BytesRawDataConverter` or `GzipDetectingDecompressor`: code outside this repository
  builds on them, so they stay.

## 3. Design

### 3.1 Per message

A new `MessageDecodingConverter` (package `io.fleak.zephflow.lib.commands.source`) replaces
`BytesRawDataConverter` in all seven sources. It is a plain `RawDataConverter<SerializedEvent>`, so
messages keep going through `SimpleSourceCommand`'s batch path and each source's commit strategy
stays as it is.

The source compiles its `RecordDecoder` once, at initialization, from its decoding config (3.2). For
each message, the converter:

1. Returns success with no records when the value is null (a Kafka tombstone), without decoding it.
2. Otherwise decodes the value with `RecordDecoder.decodeAll(value, DecodeLimits.UNLIMITED)`.
3. If every outcome is a decoded record, the message succeeds with those records, in order. An
   input that decodes to no outcome (an empty value, for most formats) succeeds with no records.
4. If any outcome is `Failed` or `Oversized`, or decoding throws, the whole message fails and no
   record of it is emitted. This keeps today's all-or-nothing rule per message.
5. Merges the message's metadata into each record exactly as the legacy deserializers did:
   `SerializedEvent.metadataWithKey` (which adds `__md_key`, the base64 key, only when the message
   has metadata), put over the record's fields as string values, so metadata wins on a name
   conflict. A null metadata value becomes a string-typed null, as before (Kinesis sets
   `kinesis_hash_key` to null when a record has no explicit hash key). Of the seven sources only
   Kinesis sets metadata today.

Multiline framing joins lines only within one message; a message never continues another.

FileSource reads its whole file as one message, as today. Its files are assumed small.

### 3.2 Decoding config and compression

- **`decoding` set:** compiled as given, including its `compression`.
- **`encodingType` set:** the format comes from `SourceDecoding`'s mapping. The compression keeps
  each source's behavior of today: `AUTO` (gunzip a value that starts with the gzip magic bytes,
  pass any other value through) for Kafka, and none for the other six. `SourceDecoding` itself
  only yields `AUTO` or `GZIP`, so for the six the source builds
  `new DecodingConfig(List.of(), <the mapped format>)`: an empty compression list decompresses
  nothing.

Kinesis's `compressionTypes` is not carried over. Its validator rejects a non-empty
`compressionTypes` and says to set `decoding` with the compression instead.

StdIn reads text lines, one message per line, so its validator rejects a `decoding` with multiline
framing. Binary formats and compression cannot be read from StdIn, because each line has been
re-encoded as text before it is decoded; this is a limitation (8), as it was before.

### 3.3 Outcomes and the DLQ

A failed message is written to the DLQ as today: the value, key and metadata are the message as
received (before decompression), and the node id is the source's own. Only the error text changes.
The converter returns a `MessageDecodeException` that carries the text, and `SimpleSourceCommand`
writes that text instead of the stack trace of the wrapping `NodeExecutionException`. Every other
failure keeps today's stack-trace text.

When writing a `MessageDecodeException`'s entry throws, `SimpleSourceCommand` logs the failure at
error and goes on with the next message. The entry is lost; only the log shows it. A failed message
is committed as today: with the first commit after it, or with the batch's final commit when any
message of its fetch succeeded; a fetch in which every message fails is not committed (8). A failed
DLQ write of a downstream failure still ends the source, as today.

The text has four parts:

```
MALFORMED json message: JsonParseException
Action: check that the message is json as the source's encodingType or decoding says, and that
  its compression is set when the message is compressed, or fix the producer that wrote it; the DLQ
  value is the message as received, so it can be sent again
<the error's stack trace>
recordIndex 2, bytes 10-25 (and 1 more problem)
```

| Kind | First line | Action |
|---|---|---|
| `Failed` `MALFORMED` | `MALFORMED <format> message: <exception class>` | check that the message is `<format>` as the source's `encodingType` or `decoding` says, and that its compression is set when the message is compressed, or fix the producer that wrote it |
| `Failed` `CORRUPT_COMPRESSION` | `CORRUPT_COMPRESSION <format> message: <exception class>` | check the source's compression setting against the message; otherwise the producer wrote damaged compressed data, or bytes after its end |
| `Oversized` | `OVERSIZED <format> message: <limit>` | `DlqTexts.oversizedText`, unit `message` |
| the converter throws | `READ_FAILURE <format> message: <exception class>` | the source could not process the message (the error says why); if every message fails like this, check the source's `encodingType` or `decoding` |

The decoder turns a format's own exceptions into `Failed` outcomes, and in-memory bytes cannot fail
to read, so `READ_FAILURE` is only for an unexpected exception inside the converter.

Every action ends with "the DLQ value is the message as received, so it can be sent again".

FileSource reads a whole file as one message, so its texts name the file:

| Kind | First line | Action |
|---|---|---|
| `Failed` `MALFORMED` | `MALFORMED <format> file: <exception class>` | check that the file is `<format>` as the source's `encodingType` or `decoding` says, and that its compression is set when the file is compressed; fix the file and run the job again |
| `Failed` `CORRUPT_COMPRESSION` | `CORRUPT_COMPRESSION <format> file: <exception class>` | check the source's compression setting against the file; otherwise the file holds damaged compressed data, or bytes after its end; fix the file and run the job again |
| `Oversized` | `OVERSIZED <format> file: <limit>` | `DlqTexts.oversizedText`, unit `file` |
| the converter throws | `READ_FAILURE <format> file: <exception class>` | the source could not process the file (the error says why); check the source's `encodingType` or `decoding`, fix the file and run the job again |

The `MALFORMED`, `CORRUPT_COMPRESSION` and `READ_FAILURE` actions are the converter's own;
`DlqTexts`'s file and object wording does not fit a message, and its existing texts do not change.

`Oversized` can still occur without limits: xz codec memory (`xz memory`, `avro xz memory`), the
zstd window, native memory, parquet's heap share, a multiline event over `maxLines`, and the
formats' own maximums (avro, protobuf, parquet).

The details are the first problem's stack trace, its record index and byte span when known, and how
many other problems the message had.

Downstream failures keep today's handling: the message as received, the stack trace, and the
failing node's id (or `unknown`).

Every failed message is counted and logged at error, with or without a DLQ, as today. The log
line keeps today's content (the message's key, value and metadata) and adds the first line. Without
a DLQ, the message is then dropped.

### 3.4 Limits and memory

The decoder runs with `DecodeLimits.UNLIMITED`: no record or document limit. A message is already
whole in memory when it is fetched, and decoding it needs a few times its size. The xz codec memory
falls back to min(128 MiB, heap/2), and parquet to its heap share, as the decoder does under
unlimited limits.

### 3.5 Metrics

Counted as today, in the converter. The changes of 4.1 items 4–6 move some messages between these
counters: for example an empty JSON_OBJECT value no longer counts as a failure, and a
JSON_OBJECT_LINE message with an array line counts as one failure instead of several records.

| Case | `input_event_size` | `input_event_count` | `input_deser_err_count` |
|---|---|---|---|
| Success | the value's stored length (0 for null), tagged from the first record's `__tag__` | number of records, same tags | not counted |
| Failure | the value's stored length, no tags | not counted | +1, no tags |

### 3.6 Shared code

`MessageDecodingConverter` reuses `SourceDecoding` (the `encodingType` mapping and the
`encodingType`/`decoding` check), `DlqTexts.oversizedText`, and `UnitRun.causeName` for the
exception class in first lines (the innermost cause's class). It does not use the rest of `UnitRun`:
a message is all or nothing and needs no batching, summaries or record entries.

## 4. Configuration

Each of the seven sources gets an optional `decoding` field (the generic decoder's config). Exactly
one of `encodingType` and `decoding` must be set; the validator compiles `decoding`, so a bad one
fails at validation. `encodingType` becomes optional (Kinesis drops its `@NonNull`).

On Kafka, `encodingType` detects gzip by itself (3.2), but `decoding` decompresses only what its
`compression` says. A Kafka topic with gzipped messages moving to `decoding` sets
`compression: auto` (or `gzip`).

### 4.1 Behavior changes for existing configurations

Existing configurations still validate and run, except a Kinesis config with `compressionTypes`.
These behaviors change, with no switch to keep the old ones:

1. **DLQ error text** for a message that fails to decode: the text of 3.3 instead of a stack trace.
2. **A failed DLQ write** of a message that fails to decode is logged, and the source goes on.
   Before, it ended the source.
3. **Kinesis `compressionTypes`** is rejected at validation. A config that had it must set
   `decoding` with the same compression. Without it, Kinesis decompresses nothing, as before.
4. **A message with no value** (a Kafka tombstone, or an event with no body) gives no records.
   Before, it ended the source.
5. **Empty value:** JSON_OBJECT, JSON_ARRAY and XML gave a failure; now no records. TEXT gave one
   record `{"__raw__":""}`; now none. On Kafka the same holds for a value that is a gzip of nothing.
   CSV gave a failure for a value of only whitespace that holds a space or tab; now no records (a
   value of only line breaks gave no records before too).
6. **Decoding differences** from the S3 realtime source, per message (all make the whole message
   fail, unless noted):
   - a JSON_OBJECT_LINE line holding an array (legacy: one record per element);
   - JSON_OBJECT_LINE: a line of only whitespace other than spaces and tabs, or invalid UTF-8
     (legacy: skipped, or decoded with U+FFFD); UTF-16 and UTF-32 lines now decode;
   - CSV: a row with more columns than the header (legacy: extra columns dropped), duplicate header
     names (legacy: the last one kept); an empty header name is now named `_c<i>` instead of
     failing;
   - XML: a `DOCTYPE`, text after the root element, or a second root element;
   - Kafka: gzip followed by extra bytes after its end;
   - CSV: a header or row of more than 16,384 columns (legacy: accepted);
   - JSON: Jackson's default length limits are lifted, so a string over 20 million characters or a
     name over 50,000 characters now decodes (legacy: failed);
   - XML: an element with more than 1,021 attributes now decodes (legacy: failed);
   - a leading byte-order mark is removed in STRING_LINE, TEXT and CSV (legacy kept it in the first
     line, the text or the first header name); JSON_OBJECT_LINE with a byte-order mark now decodes.

## 5. Compatibility

- **Config:** existing configurations stay valid except Kinesis `compressionTypes` (4.1 item 3).
  The change adds one optional field per source.
- **Classes:** `BytesRawDataConverter`, `GzipDetectingDecompressor` and the other public classes of
  `lib.commands.source` keep their signatures. The seven `Config` classes gain a field, so their
  all-args constructors change; builders and JSON configs are not affected.
- **DLQ entries:** value, key, metadata and node id are unchanged; only the error text of decode
  failures changes. Tools that read DLQ values back with the source's `encodingType` see the same
  values. An entry from a source configured with `decoding` has no `encodingType` to read it with;
  such tools must learn `decoding` before they can read these entries, as for the S3 realtime source
  and FsSource.
- **Other sources** on `SimpleSourceCommand` are unchanged: only a `MessageDecodeException` gets the
  new text and the new handling of a failed DLQ write.

## 6. Delivery

On the integration branch `feature/msg-decoder`, one PR per step, each reviewed:

| PR | Content |
|---|---|
| 0 | This spec |
| 1 | `MessageDecodingConverter`, `MessageDecodeException`, the `SimpleSourceCommand` changes; unit tests. No source uses it yet. |
| 2 | Kafka: `decoding`, validator, the new converter. |
| 3 | Kinesis (with the `compressionTypes` rejection), Event Hubs, MQTT, ActiveMQ. |
| 4 | StdIn (with the multiline rejection), FileSource. |

The integration test is a separate PR in the integration-test suite, which runs released builds.
Before the branch merges to main, it runs against a build of the branch; it merges after the core
release that contains the branch, which is cut only with approval. The branch merges to main after
PR 4, with approval.

## 7. Testing

- **Converter:** one test per case of 3.1, 3.3 and 3.5: success, several records, empty value, null
  value, metadata merge, conflict and null metadata value, each `Failed` kind, read failure, and
  each reachable `Oversized` kind; the DLQ text of each kind compared whole.
- **Per-message differential:** the legacy differential cases of the S3 realtime source, with the
  legacy side run through `DeserializerFactory` and the legacy gzip step (as that test does) and
  the new side through `MessageDecodingConverter` with Kafka's compression (`AUTO`), with the
  expected difference of each 4.1 item 5–6 pinned. The six sources without compression are covered
  by the per-source gzip test.
- **`SimpleSourceCommand`:** a `MessageDecodeException` gets its own text; a failed write of its
  entry is logged and the next message is processed and committed; every other failure keeps the
  stack trace and a failed write still ends the source; node ids unchanged.
- **Per source:** validator tests for `decoding` (valid, invalid, both set, neither set; Kinesis
  `compressionTypes`; StdIn multiline). For each source, a command-level test with only `decoding`
  set checks that the source's converter decodes a message with it: StdIn and FileSource end to
  end; MQTT, ActiveMQ and Event Hubs through `createExecutionContext` with their injected client
  factories mocked; Kinesis in its LocalStack test, since it has no client seam. For each source, a
  test with `encodingType` and a value that starts with the gzip magic bytes pins the compression
  of 3.2: Kafka gunzips it, the other six decode the bytes as they are. StdIn's test feeds the bytes
  to the converter its `createExecutionContext` builds, since stdin re-encodes text.
- **Existing tests:** the Testcontainers tests (Kafka, ActiveMQ, Event Hubs, Kinesis on LocalStack)
  stay green, and Kafka's adds a `decoding` config and a bad message that reaches a mocked
  `DlqWriter` with the new text.
- **Downstream modules:** the stdin DAG tests in `clistarter` and `runner` stay green.
- **Integration:** Kafka → source → sink with a DLQ on a real task runner (6).

## 8. Known limitations

- A message that holds several records is all or nothing: one bad record sends the whole message
  to the DLQ, and its good records are not emitted.
- No size limit: a very large message with a format that expands (for example a decompression bomb)
  can use much more memory than its size. Only xz and parquet are capped. A message that runs the
  heap out, or whose decompressed bytes pass 2 GiB, ends the source; since it was not committed, it
  is delivered again after a restart and ends the source again, as a gzip message does on Kafka
  today.
- The DLQ details name the record and bytes inside the message, not where the message came from
  (topic, partition, offset), since the sources do not capture that.
- StdIn reads text lines only: no multiline, binary formats or compression. An ActiveMQ
  `TextMessage` is re-encoded as UTF-8 too, so it cannot carry binary formats, compression or another
  charset; a `BytesMessage` can.
- A fetch in which every message fails is not committed until a later message succeeds, as today:
  after a restart or rebalance in between, those messages are delivered and dead-lettered again,
  and on ActiveMQ the transaction is rolled back.
- A Kinesis record without an explicit hash key has a null metadata value, which the DLQ's Avro
  schema cannot hold, so its DLQ entry (and the others written in the same flush) is dropped with a
  log line, as today.
- `DlqTexts.oversizedText` speaks of files in some actions even for a message: the format maximum
  ("write the file in smaller pieces"), avro xz memory ("rewrite the avro file") and parquet heap
  share ("write the file with smaller row groups"). For FileSource, the native memory action ends
  with "fix the file and run the job again" although the file is fine.
