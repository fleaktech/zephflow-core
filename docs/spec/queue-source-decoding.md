# SQS, Pub/Sub and IoT Hub on the Generic Decoder

SQS, Pub/Sub and Azure IoT Hub decode each message with their own converter
(`SqsRawDataConverter`, `PubSubRawDataConverter`, `IotHubRawDataConverter`) and a legacy
`FleakDeserializer`. This change makes them decode with `MessageDecodingConverter`, as Kafka,
Kinesis, Event Hubs, MQTT, ActiveMQ, StdIn and FileSource already do
(`docs/spec/message-source-decoding.md`, called "the message-source spec" below). How each source
fetches, acknowledges and commits does not change.

## 1. Why

- **Formats.** The three sources support only the seven legacy encodings and cannot decompress.
  Pub/Sub and IoT Hub carry raw bytes, so they gain every format and compression of `decoding`;
  SQS carries text only (8).
- **DLQ texts.** A message that fails to decode is written to the DLQ with a stack trace as its
  error text, with no hint of what to do.
- **One decoder.** They are the last message sources on the legacy deserializers. Moving them
  gives every message source the same `decoding` config, DLQ texts and failure handling.

## 2. Goals and non-goals

**Goals**
- The three sources decode through `MessageDecodingConverter`. A legacy `encodingType` runs through
  the same mapping as the other message sources.
- Each source accepts a new `decoding` field; exactly one of `encodingType` and `decoding` is set.
- A message that fails to decode gets the message-source spec's DLQ text (its 3.3, unit `message`),
  and a failed write of that entry is logged and the source goes on.
- IoT Hub's records keep their shape: the merged metadata fields and the `_iothub` envelope.
- Every behavior change for an existing configuration is listed (4.1).

**Non-goals**
- Several records per message, size limits, fetching, acknowledging, committing and downstream
  failures: as in the message-source spec (2, non-goals).
- Message metadata that the sources do not capture today: SQS and Pub/Sub message ids and
  attributes, SQS receive counts.
- Elasticsearch, which decodes documents, not messages.

## 3. Design

### 3.1 Converters

`MessageDecodingConverter` decodes a `SerializedEvent`. SQS and Pub/Sub fetch their own record
types (`SqsReceivedMessage`, `PubSubReceivedMessage`), so they need an adapter; IoT Hub fetches a
`SerializedEvent` and adds an envelope after decoding.

- **`MessageDecodingConverter`** gains a package-private
  `convert(SerializedEvent, SourceExecutionContext<?>, Supplier<String> description)`. The
  description is what the error log prints for a failed message after the DLQ first line; it is
  called only when a message fails. The existing `convert(SerializedEvent, SourceExecutionContext<?>)`
  calls it with the event's `toString()`, so the seven sources already on it do not change.
- **`MappedMessageConverter<T>`** (new, package `io.fleak.zephflow.lib.commands.source`) is a
  `RawDataConverter<T>` built from a `MessageDecodingConverter`, a `Function<T, SerializedEvent>`
  and a `Function<T, String>` for the description. It converts the record to an event, decodes it,
  and returns the result with the original record as its source record: the same records on
  success, the same `MessageDecodeException` on failure.
- **SQS** uses `MappedMessageConverter` with its encoder's `serialize` (`encoder::serialize` on the
  `SqsRawDataEncoder` that `createExecutionContext` builds: the body, no key, no metadata, as
  today) and the description `"SQS messageId: <id>"`.
- **Pub/Sub** does the same with its `PubSubRawDataEncoder` and `"Pub/Sub messageId: <id>"`.
- **IoT Hub**: a new `IotHubMessageConverter` (package `azureiothubsource`) wraps a
  `MessageDecodingConverter`. It calls `convert(event, context)` (the description is the event, as
  today's log line) and, on success, merges the `_iothub` envelope into every record with
  `copyAndMerge`, as today. It builds the envelope with today's code: `IotHubRawDataConverter`'s
  private `buildEnvelope` becomes package-private and is shared. A failure is returned as it is.

Because `MessageDecodingConverter` merges `SerializedEvent.metadataWithKey` into each record as the
legacy deserializers did (message-source spec 3.1 step 5), an IoT Hub record keeps today's flat
fields too: `deviceId`, `enqueuedTime`, `prop.<name>`, and `__md_key` when the event has both a
partition key and metadata (`metadataWithKey` adds the key only when metadata is present). SQS and Pub/Sub events carry no key or metadata, so their records get no extra fields, as
today.

`SqsRawDataConverter`, `PubSubRawDataConverter` and `IotHubRawDataConverter` stay as they are
(apart from `buildEnvelope`'s visibility), with no source using them, like `BytesRawDataConverter`.
The tests use `IotHubRawDataConverter` as the legacy side of IoT Hub's differential test (7).

### 3.2 Decoding config

Each command has a static `decodingConfig(config)` that returns
`MessageDecodingConverter.decodingConfig(encodingType, decoding, false)`, which the validator
compiles and the command uses. With `encodingType`, nothing is decompressed, as today (IoT Hub's
decompressor was built with no codecs). With `decoding`, its `compression` applies.

### 3.3 Outcomes, DLQ and commit

As in the message-source spec 3.3, unit `message`: the same first lines, `Action:` lines,
details, and the same `SimpleSourceCommand` handling (the text written for a
`MessageDecodeException`, a failed write logged, node id unchanged, commit as today).

The DLQ entry is what each source's encoder writes today: the body alone for SQS and Pub/Sub, the
key, value and metadata for IoT Hub.

The error log line becomes `failed to deserialize event: <first line>: <error message>` followed
by the description: the SQS or Pub/Sub message id (today: `failed to deserialize SQS event for
messageId: <id>` and `failed to deserialize Pub/Sub message: <id>`), or the whole event for IoT Hub
(today: `failed to deserialize IoT Hub event:` and the event).

### 3.4 Metrics

As in the message-source spec 3.5. IoT Hub's tags come from the first record before the envelope
is merged; the envelope adds only `_iothub`, so the tags do not change.

## 4. Configuration

Each of the three sources gets an optional `decoding` field. Exactly one of `encodingType` and
`decoding` must be set; the validator compiles `decoding`, so a bad one fails at validation. SQS and
Pub/Sub drop `@NonNull` on `encodingType`; IoT Hub's validator no longer requires it.

### 4.1 Behavior changes for existing configurations

Existing configurations still validate and run. These behaviors change, with no switch to keep
the old ones:

1. **DLQ error text** for a message that fails to decode: the message-source spec's text instead of
   a stack trace.
2. **A failed DLQ write** of a message that fails to decode is logged, and the source goes on.
   Before, it ended the source.
3. **Empty value:** as in the message-source spec 4.1 item 5, without the Kafka gzip case.
4. **Decoding differences:** as in the message-source spec 4.1 item 6, without the Kafka gzip
   case.
5. **Error log line:** the DLQ first line is added, the wording changes as in 3.3, and the line
   and the debug `got message` lines are logged by `MessageDecodingConverter`'s logger instead of
   `SqsRawDataConverter`'s, `PubSubRawDataConverter`'s or `IotHubRawDataConverter`'s. A log alert that matches the old
   logger or the words "IoT Hub event" must be changed. IoT Hub's debug `got message` lines show
   the records before the `_iothub` envelope is merged.
6. **Validation message:** a config with neither field fails with "exactly one of encodingType and
   decoding must be set" (an `IllegalArgumentException`) instead of "no encoding type is provided"
   (a `NullPointerException`).
7. **No value:** an IoT Hub event with no body gives no records, as in the message-source spec 4.1
   item 4. Before, it ended the source. SQS and Pub/Sub bodies are never null.

## 5. Compatibility

- **Config:** existing configurations stay valid; one optional field per source.
- **Classes:** every public class keeps its signatures. `MappedMessageConverter` and
  `IotHubMessageConverter` are added. The three `Config` classes gain a field, so their all-args
  constructors change; builders and JSON configs are not affected.
- **DLQ entries:** value, key, metadata and node id unchanged; only the error text of decode
  failures changes.

## 6. Delivery

On the integration branch `feature/queue-decoder`, one reviewed PR per step:

| PR | Content |
|---|---|
| 0 | This spec |
| 1 | `MessageDecodingConverter`'s description overload, `MappedMessageConverter`; SQS and Pub/Sub moved onto them, with `decoding` and validators. |
| 2 | IoT Hub: `decoding`, validator, `IotHubMessageConverter`. |

The SQS integration test is a separate PR in the integration-test suite, which runs released
builds. Before the branch merges to main, it runs against a build of the branch; it merges after
the core release that contains the branch, which is cut only with approval. The branch merges to
main after PR 2, with approval.

## 7. Testing

- **`MappedMessageConverter`:** success returns the decoded records with the original record and
  never calls the description; failure returns the `MessageDecodeException` with the original
  record; the error log line has the first line and the description.
- **SQS and Pub/Sub:** validator tests (valid and invalid `decoding`, both set, neither set). With
  only `decoding` set, the converter that `createExecutionContext` builds (client factory mocked)
  decodes a message; with `encodingType`, a body that starts with the gzip magic bytes is decoded
  as it is, not gunzipped. A bad message reaches a mocked `DlqWriter` as the body alone, with the
  new text.
- **IoT Hub:** a differential test runs the same events through `IotHubRawDataConverter` (with
  the `encodingType`'s legacy deserializer) and `IotHubMessageConverter` (with that
  `encodingType`'s decoding config), and compares the records whole: with metadata and properties,
  with metadata and no properties, with no metadata, with a partition key and metadata, with a
  partition key and no metadata, and a message of several JSON lines. A bad message fails on both.
  Because both sides share `buildEnvelope`, the envelope is also pinned by expected records written
  out in full. As for SQS: validator tests, a decode with only `decoding` through the converter
  `createExecutionContext` builds (client factory mocked), and a gzip-magic body with `encodingType`
  decoded as it is. A bad message reaches a mocked `DlqWriter` with its key, value and metadata
  unchanged and the new text. `AzureIotHubSourceIntegrationTest` (Event Hubs emulator) stays green.
- **Integration (SQS):** `sqssource → kafkasink` on a real task runner with LocalStack SQS and an S3
  DLQ, the source with only `decoding` (json objects) and a visibility timeout longer than the test:
  a plain message and a message that is not json; once the bad one's DLQ entry is seen, another plain
  message. The good records reach Kafka (compared as a set, since a standard queue does not keep
  order), the bad one is dead-lettered as received with the new text, exactly once, and the job is
  still running afterwards. Compression is not tested here, since SQS carries text (8).

## 8. Known limitations

As in the message-source spec 8, and:

- The DLQ entry of an SQS or Pub/Sub message has no message id, as today, so an entry cannot be
  matched to its message except by content.
- On SQS and Pub/Sub one commit deletes or acknowledges every message fetched so far, so a failed
  message is committed with the first good message of its fetch, wherever it stands. A fetch in
  which every message fails stays uncommitted until a later fetch has a good message. If that comes
  after the visibility timeout (SQS) or the ack deadline (Pub/Sub), the failed messages are
  delivered and dead-lettered again, as today.
- SQS bodies are text: the fetcher re-encodes each body as UTF-8 before decoding, so compressed
  data and binary formats (avro, protobuf, parquet) cannot arrive intact, as on StdIn. A `decoding`
  with a codec other than `auto`, or with a binary format, passes validation and then fails on
  every message. `auto` is safe: a text body has no gzip magic bytes and passes through.
