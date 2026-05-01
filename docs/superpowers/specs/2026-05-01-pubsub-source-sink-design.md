# Google Pub/Sub Source and Sink Nodes

## Goal

Add `pubsubsource` and `pubsubsink` operator commands to zephflow-core, following the patterns established by the existing SQS and GCS connectors. The source consumes messages from a Pub/Sub subscription via synchronous pull; the sink publishes events to a Pub/Sub topic via synchronous batch publish.

## Architectural Decisions

| Decision                          | Choice                                                                                            |
| --------------------------------- | ------------------------------------------------------------------------------------------------- |
| Source consumption model          | Synchronous pull (`SubscriberStub.pullCallable`), `PerRecordCommitStrategy`                       |
| Sink publish model                | Synchronous batch publish (`PublisherStub.publishCallable`)                                       |
| Resource addressing               | Separate `projectId` + short `subscription` / `topic` fields; `projectId` falls back to `GcpCredential.projectId` |
| Sink Pub/Sub features             | `orderingKeyExpression` (parallel to SQS `messageGroupIdExpression`); no per-message attributes   |
| Source tunables                   | `maxMessages`, `returnImmediately`, `ackDeadlineExtensionSeconds`                                 |
| Authentication                    | Reuses existing `GcpCredential` (SERVICE_ACCOUNT_JSON_KEYFILE / ACCESS_TOKEN / APPLICATION_DEFAULT) |

## Module Layout

New packages under `lib/src/main/java/io/fleak/zephflow/lib/commands/`:

- `pubsubsource/` — synchronous-pull source.
- `pubsubsink/` — batch-publish sink.

Shared GCP infra:

- `lib/src/main/java/io/fleak/zephflow/lib/gcp/PubSubClientFactory.java` — builds `SubscriberStub` and `PublisherStub` from `GcpCredential`. Mirrors the shape of the existing `GcsClientFactory` (no changes to `GcsClientFactory` itself).

Wiring:

- `MiscUtils`: add constants `COMMAND_NAME_PUBSUB_SOURCE = "pubsubsource"` and `COMMAND_NAME_PUBSUB_SINK = "pubsubsink"`.
- `OperatorCommandRegistry.OPERATOR_COMMANDS`: register `PubSubSourceCommandFactory` and `PubSubSinkCommandFactory` against those names.
- `gradle/libs.versions.toml`: add a `pubsub` version entry and a `gcs-pubsub` library reference for `com.google.cloud:google-cloud-pubsub`. Pin a version compatible with the existing `gcs = "2.49.0"` (use whichever `google-cloud-pubsub` release lines up with that GAX/auth lineage; e.g. `1.131.x`).
- `lib/build.gradle`: `implementation libs.gcs.pubsub` (next to the existing `libs.gcs.storage`).

## Source: `pubsubsource`

### Files

- `PubSubSourceDto.java` — config interface and inner `Config` class with Lombok `@Data`/`@Builder` (parallel to `SqsSourceDto`).
- `PubSubReceivedMessage.java` — `record (byte[] body, String messageId, String ackId, Map<String,String> attributes)`.
- `PubSubRawDataConverter.java` — wraps `FleakDeserializer`, emits `SerializedEvent(null, body, attributes)`. Identical pattern to `SqsRawDataConverter` (counters, error path, DLQ-routable failure).
- `PubSubRawDataEncoder.java` — emits `SerializedEvent(null, body, attributes)` (same shape as `SqsRawDataEncoder`).
- `PubSubSourceFetcher.java` — implements `Fetcher<PubSubReceivedMessage>`; see Behavior below.
- `PubSubSourceConfigValidator.java` — validation rules, see below.
- `PubSubSourceCommand.java` — extends `SimpleSourceCommand<PubSubReceivedMessage>`, returns `SourceType.STREAMING`, `commandName() = COMMAND_NAME_PUBSUB_SOURCE`. Resolves the subscription's full path from config + credential and builds the fetcher via `PubSubClientFactory.createSubscriberStub(credential)`.
- `PubSubSourceCommandFactory.java` — extends `SourceCommandFactory`, hands in `JsonConfigParser<>`, `PubSubSourceConfigValidator`, `PubSubClientFactory`.

### Config

```java
public interface PubSubSourceDto {
  int DEFAULT_MAX_MESSAGES = 100;
  int MAX_MAX_MESSAGES = 1000;
  int MAX_ACK_DEADLINE_EXTENSION_SECONDS = 600;

  @Data @Builder @NoArgsConstructor @AllArgsConstructor
  class Config implements CommandConfig {
    private String projectId;                       // optional; falls back to GcpCredential.projectId
    @NonNull private String subscription;           // short name, e.g. "my-sub"
    @NonNull private EncodingType encodingType;
    private String credentialId;
    @Builder.Default private Integer maxMessages = DEFAULT_MAX_MESSAGES;
    @Builder.Default private Boolean returnImmediately = false;
    private Integer ackDeadlineExtensionSeconds;    // optional; null disables extension
  }
}
```

### Validation rules

- `subscription` non-blank.
- `encodingType` non-null; `DeserializerFactory.validateEncodingType(encodingType)`.
- `maxMessages` (when set) in `[1, MAX_MAX_MESSAGES]`.
- `ackDeadlineExtensionSeconds` (when set) in `[0, MAX_ACK_DEADLINE_EXTENSION_SECONDS]`.
- If `credentialId` is non-blank and `enforceCredentials(jobContext)` is true, run `lookupGcpCredential(jobContext, credentialId)` (this also forces the credential to exist).
- `projectId` resolution happens at command init (not in the validator), since the credential is resolved there. If both the config field and the credential's `projectId` are blank, `PubSubSourceCommand` throws an `IllegalArgumentException` with a clear message.

### Behavior — `PubSubSourceFetcher`

State:
- `subscriberStub` (`com.google.cloud.pubsub.v1.stub.SubscriberStub`)
- `subscriptionPath: String` — `projects/{projectId}/subscriptions/{subscription}`
- `maxMessages: int`, `returnImmediately: boolean`, `ackDeadlineExtensionSeconds: int` (`0` = disabled)
- `ConcurrentLinkedQueue<String> pendingAckIds`

`fetch()`:
1. Build `PullRequest` with subscription path, `maxMessages`, `returnImmediately`.
2. `PullResponse response = subscriberStub.pullCallable().call(request)`.
3. If `response.getReceivedMessagesCount() == 0`, return `List.of()` (debug log).
4. If `ackDeadlineExtensionSeconds > 0`, call `subscriberStub.modifyAckDeadlineCallable().call(ModifyAckDeadlineRequest)` once with all received ackIds and `ackDeadlineSeconds = ackDeadlineExtensionSeconds`. Failures here are logged at WARN; pull results are still returned.
5. For each `ReceivedMessage`:
   - Build `attributes` map starting from `message.getAttributesMap()`.
   - Add `messageId` → `message.getMessageId()`.
   - If `message.hasPublishTime()`, add `publishTime` → ISO-8601 string of `Timestamps.toMicros`.
   - If `!message.getOrderingKey().isEmpty()`, add `orderingKey` → that value.
   - Construct `PubSubReceivedMessage(message.getData().toByteArray(), message.getMessageId(), receivedMessage.getAckId(), attributes)`.
   - Enqueue `ackId` into `pendingAckIds`.
6. Return the result list.

`isExhausted()`: `false` (streaming source).

`committer()`:
- Drain `pendingAckIds` into a local list, chunk at 1000 ackIds per request, and for each chunk call `subscriberStub.acknowledgeCallable().call(AcknowledgeRequest)`. Per-chunk exceptions are logged at ERROR and not rethrown (Pub/Sub will redeliver after the ack deadline — same forgiving stance as `SqsSourceFetcher`'s delete loop).

`commitStrategy()`: `PerRecordCommitStrategy.INSTANCE`.

`close()`: `subscriberStub.close()` if non-null.

### `PubSubSourceCommand` wiring

- Resolve `GcpCredential` via `lookupGcpCredentialOpt(jobContext, credentialId)`.
- Resolve `projectId`: prefer `config.projectId`, else `credential.projectId`. If both blank, throw `IllegalArgumentException("projectId required: set Config.projectId or GcpCredential.projectId")`.
- Build `subscriberStub` via `PubSubClientFactory.createSubscriberStub(credential)` (or no-arg overload when credential is absent).
- Construct `PubSubSourceFetcher(subscriberStub, subscriptionPath, maxMessages, returnImmediately, ackDeadlineExtensionSeconds ?? 0)`.
- DLQ wiring identical to `SqsSourceCommand` (DlqWriterFactory, key prefix from `JobContext.DATA_KEY_PREFIX`).
- Counters identical: `METRIC_NAME_INPUT_EVENT_SIZE_COUNT`, `METRIC_NAME_INPUT_EVENT_COUNT`, `METRIC_NAME_INPUT_DESER_ERR_COUNT`.

## Sink: `pubsubsink`

### Files

- `PubSubSinkDto.java` — config (encodingType is a `String`, parallel to `SqsSinkDto`).
- `PubSubOutboundMessage.java` — `record (String body, String orderingKey)`.
- `PubSubSinkMessageProcessor.java` — implements `SinkMessagePreProcessor<PubSubOutboundMessage>`. Produces the body via `JsonUtils.toJsonString(event)`, matching the existing `SqsSinkMessageProcessor` exactly. The `encodingType` config field is validated up-front (so users get a clear error for unsupported values) but is not yet wired into a serializer factory in v1; this matches the current SQS sink behavior and keeps the door open for a future change that swaps in `SerializerFactory`-driven encoding without breaking the config schema.
- `PubSubSinkFlusher.java` — implements `Flusher<PubSubOutboundMessage>`; see Behavior below.
- `PubSubSinkConfigValidator.java` — validation rules.
- `PubSubSinkCommand.java` — extends `SimpleSinkCommand<PubSubOutboundMessage>`, `commandName() = COMMAND_NAME_PUBSUB_SINK`, `batchSize()` returns the config value.
- `PubSubSinkCommandFactory.java` — `commandType() = SINK`.

### Config

```java
public interface PubSubSinkDto {
  int DEFAULT_BATCH_SIZE = 100;
  int MAX_BATCH_SIZE = 1000;

  @Data @Builder @NoArgsConstructor @AllArgsConstructor
  class Config implements CommandConfig {
    private String projectId;                       // optional; falls back to GcpCredential.projectId
    @NonNull private String topic;                  // short name, e.g. "my-topic"
    @NonNull private String encodingType;
    private String credentialId;
    private String orderingKeyExpression;           // optional PathExpression
    @Builder.Default private Integer batchSize = DEFAULT_BATCH_SIZE;
  }
}
```

### Validation rules

- `topic` non-blank.
- `encodingType` non-blank; parsed via `parseEnum(EncodingType.class, encodingType)`; `SerializerFactory.validateEncodingType(encodingType)`.
- `batchSize` (when set) in `[1, MAX_BATCH_SIZE]`.
- If `orderingKeyExpression` is non-blank, `PathExpression.fromString(...)` parse-check.
- If `credentialId` is non-blank and `enforceCredentials(jobContext)`, `lookupGcpCredential(jobContext, credentialId)`.
- `projectId` resolution at command init, identical to source.

### Behavior — `PubSubSinkMessageProcessor`

```java
public PubSubOutboundMessage preprocess(RecordFleakData event, long ts) {
  String body = JsonUtils.toJsonString(event);
  String orderingKey = orderingKeyExpression != null
      ? orderingKeyExpression.getStringValueFromEventOrDefault(event, null)
      : null;
  return new PubSubOutboundMessage(body, orderingKey);
}
```

### Behavior — `PubSubSinkFlusher`

`flush(preparedInputEvents, metricTags)`:
1. If `messages` empty, return `FlushResult(0, 0, List.of())`.
2. Build `List<PubsubMessage>`:
   - `PubsubMessage.Builder` with `setData(ByteString.copyFromUtf8(message.body()))`.
   - If `message.orderingKey() != null`, `setOrderingKey(message.orderingKey())`.
3. Build `PublishRequest.newBuilder().setTopic(topicPath).addAllMessages(pubsubMessages).build()`.
4. Try: `publisherStub.publishCallable().call(request)`; on success:
   - `successCount = messages.size()`.
   - `flushedDataSize = sum of message.body().getBytes(UTF_8).length`.
   - Return `FlushResult(successCount, flushedDataSize, List.of())`.
5. Catch `Exception e`:
   - For every `(rawEvent, _)` in `preparedInputEvents.rawAndPreparedList()`, append `new ErrorOutput(rawEvent, "Pub/Sub publish failed: " + e.getMessage())`.
   - Log at ERROR.
   - Return `FlushResult(0, 0, errorOutputs)`.

`close()`: `publisherStub.close()` if non-null.

Note: unlike SQS's `sendMessageBatch` (per-entry success/failure), Pub/Sub's `Publish` RPC is atomic — a transient failure fails the entire batch. The flusher reflects that: either all events succeed or all become `ErrorOutput`s.

### `PubSubSinkCommand` wiring

- Resolve `GcpCredential` via `MiscUtils.lookupGcpCredentialOpt(jobContext, credentialId)`.
- Resolve `projectId` (config or credential) and compose `topicPath = projects/{projectId}/topics/{topic}`.
- Build `publisherStub` via `PubSubClientFactory.createPublisherStub(credential)` (or no-arg).
- Construct `PubSubSinkFlusher(publisherStub, topicPath)`.
- `SinkMessagePreProcessor` built from the parsed `orderingKeyExpression` (or null).
- Counters via `createSinkCounters(...)`.

## Shared infra: `PubSubClientFactory`

```java
public class PubSubClientFactory implements Serializable {
  public SubscriberStub createSubscriberStub() { ... default credentials ... }
  public SubscriberStub createSubscriberStub(GcpCredential credential) { ... }
  public PublisherStub createPublisherStub() { ... }
  public PublisherStub createPublisherStub(GcpCredential credential) { ... }
}
```

Internally:
- `SubscriberStubSettings.newBuilder().setCredentialsProvider(FixedCredentialsProvider.create(googleCredentials)).build()` → `GrpcSubscriberStub.create(settings)`.
- Same for `PublisherStubSettings` → `GrpcPublisherStub.create(settings)`.
- `googleCredentials` resolved from `GcpCredential.authType` exactly as `GcsClientFactory.resolveCredentials` does today (`SERVICE_ACCOUNT_JSON_KEYFILE` / `ACCESS_TOKEN` / `APPLICATION_DEFAULT`). The resolution helper can be inlined in `PubSubClientFactory` (matching `GcsClientFactory`'s style) rather than extracted into a shared utility — extraction is out of scope for this change.

## Data flow

### Source per pull cycle
1. `SimpleSourceCommand` invokes `fetcher.fetch()`.
2. `PubSubSourceFetcher.fetch()` issues `pull` RPC, optionally extends ack deadlines, returns `List<PubSubReceivedMessage>` and queues ackIds.
3. `PubSubRawDataConverter.convert()` deserializes each body; `dataSizeCounter`, `inputEventCounter`, `deserializeFailureCounter` fire as in SQS.
4. After successful processing, `committer()` drains ackIds and acknowledges them in chunks of ≤ 1000.

### Sink per flush
1. `SimpleSinkCommand` collects up to `batchSize` events and runs each through `PubSubSinkMessageProcessor` to produce `PubSubOutboundMessage`.
2. `PubSubSinkFlusher.flush()` builds `PubsubMessage`s and calls a single `publish` RPC.
3. Success → all events counted; failure → all events become `ErrorOutput`s.

## Error handling summary

| Where                         | Behavior                                                                  |
| ----------------------------- | ------------------------------------------------------------------------- |
| Validator                     | `Preconditions` throws (caught by existing config-validation flow)        |
| Source: deserializer failure  | `ConvertedResult.failure` + counter; routed to DLQ if configured          |
| Source: ack RPC failure       | Logged at ERROR; not rethrown (Pub/Sub will redeliver)                    |
| Source: modifyAckDeadline failure | Logged at WARN; pull result still returned                            |
| Sink: publish exception       | Every event in the batch becomes an `ErrorOutput`                         |
| Stub creation failure         | `RuntimeException` at command init (parallel to `GcsClientFactory`)       |
| Missing projectId             | `IllegalArgumentException` at command init                                |

## Testing

Unit tests with mocked stubs (no Pub/Sub emulator / testcontainers — matching the precedent set by `sqssource`, `sqssink`, `gcssource`, `gcssink`).

Test files under `lib/src/test/java/io/fleak/zephflow/lib/commands/`:

**`pubsubsource/`:**
- `PubSubSourceFetcherTest` — fetch returns mapped messages; attributes propagated (incl. `messageId`, `orderingKey`, `publishTime`); empty pull short-circuits; `modifyAckDeadline` RPC invoked when extension configured, not invoked when disabled; pull-request fields (subscription path, maxMessages, returnImmediately) verified via `argThat`.
- `PubSubSourceFetcherCommitTest` — committer drains queued ackIds, batches ack calls at the chunk boundary, exception in ack does not throw.
- `PubSubSourceConfigValidatorTest` — required fields, range checks for `maxMessages` and `ackDeadlineExtensionSeconds`, encoding-type validation, credential-required path under `enforceCredentials`.
- `PubSubSourceDtoTest` — JSON round-trip of config defaults.

**`pubsubsink/`:**
- `PubSubSinkFlusherTest` — successful publish (success count = messages.size, flushed size summed correctly); exception path (every event becomes ErrorOutput); orderingKey set on `PubsubMessage` when supplied (and not set when null); empty batch short-circuits with no RPC.
- `PubSubSinkMessageProcessorTest` — body produced from `JsonUtils.toJsonString(event)`; orderingKey resolved from PathExpression; null orderingKey when expression absent.
- `PubSubSinkConfigValidatorTest` — required fields, batch-size range, ordering-key parse error surfaces, credential-required path.
- `PubSubSinkDtoTest` — JSON round-trip.

## Out of scope

- High-level `Publisher` (with built-in batching, flow control, and async retries).
- Streaming pull (`SubscriberStub.streamingPullCallable`).
- Per-message attribute extraction on the sink (`attributesExpression`).
- Pub/Sub schema service / Avro schema validation.
- Pub/Sub emulator-based integration tests.
- Refactoring `GcsClientFactory` / `PubSubClientFactory` into a shared `GcpCredentials` helper.
