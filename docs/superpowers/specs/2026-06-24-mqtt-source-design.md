# MQTT Source Command Design

Date: 2026-06-24

## Goal

Add a new streaming source command, `mqttsource`, that subscribes to an MQTT
broker, consumes messages from a topic filter, deserializes them according to a
configurable `EncodingType`, and emits them into the pipeline. The
implementation follows the existing source-command standard, modeled most
closely on `activemqsource` (another broker-based streaming source).

## Standard Conformance

The implementation adheres to the project's established source conventions:

- Configurable `EncodingType` on the config DTO, validated via
  `DeserializerFactory.validateEncodingType(...)`, and used to build a
  `BytesRawDataConverter` around the matching `FleakDeserializer`.
- Full, descriptive variable names (no abbreviations like `cf`, `bm`, `tm`
  beyond what the existing code already uses; e.g. `connectionFactory`,
  `incomingMessage`).
- Readable, intention-revealing method names
  (`createMqttFetcher`, `createRawDataConverter`, `buildConnectionOptions`).
- No explanatory inline comments. Only the Apache license header (matching every
  other file) and, where genuinely unavoidable, the same kind of terse
  qualifier the existing code uses for non-obvious facts.
- Extends `SimpleSourceCommand<SerializedEvent>` and produces a
  `SourceExecutionContext<>` with the standard metric counters and optional DLQ
  writer, exactly as `ActiveMqSourceCommand` does.

## Library & Semantics Decisions

- **Client library:** Eclipse Paho MQTT v5 (`org.eclipse.paho.mqttv5.client`).
- **Acknowledgment model:** auto-ack on receive; commit strategy is fixed to
  `NONE` (`NoCommitStrategy.INSTANCE`). The fetcher exposes no committer. This
  gives at-most-once delivery and keeps the source simple, consistent with the
  approved decision.
- **Receive model:** Paho delivers messages through an async callback. The
  callback enqueues each message into a bounded `BlockingQueue`. `fetch()`
  blocks up to `receiveTimeoutMs` for the first message, then drains up to
  `maxBatchSize` additional messages without blocking — mirroring the
  receive/drain loop in `ActiveMqSourceFetcher`.

## Configuration Surface (`MqttSourceDto.Config`)

| Field | Type | Notes |
|-------|------|-------|
| `brokerUrl` | `String` | e.g. `tcp://host:1883` or `ssl://host:8883`. Required. |
| `topicFilter` | `String` | MQTT topic filter to subscribe to. Required. |
| `encodingType` | `EncodingType` | Required; validated. |
| `clientId` | `String` | MQTT client identifier. Required (stable id needed for session state). |
| `credentialId` | `String` | Optional; resolves to a `UsernamePasswordCredential`. |
| `qos` | `int` | Subscription QoS 0/1/2. Default `1`. |
| `cleanStart` | `boolean` | MQTT 5 clean-start flag. Default `true`. |
| `sessionExpiryIntervalSeconds` | `Long` | Optional; broker session retention when `cleanStart=false`. |
| `useTls` | `boolean` | Enables TLS connection options. Default `false`; the `ssl://` scheme in `brokerUrl` is the primary switch, this aligns the connect options. |
| `receiveTimeoutMs` | `Long` | First-receive block timeout. Default `DEFAULT_RECEIVE_TIMEOUT_MS = 1000L`. |
| `maxBatchSize` | `Integer` | Max messages drained per `fetch()`. Default `DEFAULT_MAX_BATCH_SIZE = 500`. |
| `receiveQueueCapacity` | `Integer` | Bounded callback queue size. Default `DEFAULT_RECEIVE_QUEUE_CAPACITY = 10000`. |

No `CommitStrategyType` enum is exposed (commit is fixed to `NONE`).

## Components

All under `io.fleak.zephflow.lib.commands.mqttsource`:

1. **`MqttSourceDto`** — interface holding the `Config` record/class (Lombok
   `@Data @Builder @NoArgsConstructor @AllArgsConstructor`) plus the default
   constants. No nested enums needed beyond what config requires (QoS is a
   plain `int`).

2. **`MqttClientProvider`** — isolates Paho client construction and connection
   options so the command/fetcher are testable without a live broker. Exposes
   `createClient(config)` returning a connected `MqttClient`, and
   `buildConnectionOptions(config, credential)`. Analogous to
   `JmsConnectionFactoryProvider`.

3. **`MqttSourceFetcher`** — `record` implementing `Fetcher<SerializedEvent>`.
   Holds the `MqttClient`, the `BlockingQueue<byte[]>`, `receiveTimeoutMs`, and
   `maxBatchSize`. `fetch()` drains the queue; `commitStrategy()` returns
   `NoCommitStrategy.INSTANCE`; `close()` unsubscribes, disconnects, and closes
   the client quietly.

4. **`MqttSourceConfigValidator`** — implements `ConfigValidator`. Checks
   `brokerUrl`, `topicFilter`, `clientId` non-blank; `encodingType` non-null and
   valid; `qos` in `{0,1,2}`; `maxBatchSize`/`receiveQueueCapacity`/
   `receiveTimeoutMs` positive when set.

5. **`MqttSourceCommand`** — extends `SimpleSourceCommand<SerializedEvent>`.
   `createExecutionContext(...)` builds the fetcher (subscribing the Paho
   callback to enqueue into the queue), a `BytesRawDataEncoder`, and the
   `BytesRawDataConverter` from `encodingType`; wires the standard metric
   counters and optional DLQ writer. `sourceType()` returns
   `SourceType.STREAMING`; `commandName()` returns `COMMAND_NAME_MQTT_SOURCE`.

6. **`MqttSourceCommandFactory`** — extends `SourceCommandFactory`; builds the
   `JsonConfigParser`, validator, and `MqttClientProvider`, and returns the
   command. Mirrors `ActiveMqSourceCommandFactory`.

## Data Flow

```
Paho callback (messageArrived) --enqueue byte[]--> BlockingQueue
        |
MqttSourceFetcher.fetch() --drain--> List<SerializedEvent>(null, body, null)
        |
BytesRawDataConverter (FleakDeserializer per EncodingType)
        |
SimpleSourceCommand pipeline --> downstream
```

The message body is taken as `MqttMessage.getPayload()` (already `byte[]`), so
no charset handling is needed (unlike JMS `TextMessage`).

## Registration

- Add `String COMMAND_NAME_MQTT_SOURCE = "mqttsource";` to `MiscUtils`.
- Register `.put(COMMAND_NAME_MQTT_SOURCE, new MqttSourceCommandFactory())` in
  `OperatorCommandRegistry`.

## Dependencies

- Add Paho v5 to `gradle/libs.versions.toml`:
  - `pahoMqttV5 = "1.2.5"` under `[versions]`
  - `paho-mqttv5-client = { module = "org.eclipse.paho:org.eclipse.paho.mqttv5.client", version.ref = "pahoMqttV5" }` under `[libraries]`
- Add `implementation libs.paho.mqttv5.client` to `lib/build.gradle`.

## Error Handling

- Client/connection construction failures in `createExecutionContext` →
  cleanup any partially opened client quietly and rethrow as `RuntimeException`,
  mirroring `ActiveMqSourceCommand.createJmsFetcher`'s catch block.
- A full receive queue drops the oldest/incoming message with a `log.warn`
  (bounded queue back-pressure), rather than blocking the Paho callback thread.
- Deserialization failures continue to flow through the existing
  `deserializeFailureCounter` / DLQ machinery in `SimpleSourceCommand`.

## Testing

Mirror the existing `activemqsource` test set under
`lib/src/test/java/.../mqttsource`:

- **`MqttSourceConfigValidatorTest`** — valid config passes; missing
  `brokerUrl`/`topicFilter`/`clientId`, null/invalid `encodingType`, bad `qos`,
  non-positive sizes each throw.
- **`MqttSourceFetcherTest`** — pre-populate the `BlockingQueue`, assert
  `fetch()` drains up to `maxBatchSize`, respects the timeout when empty,
  returns `NoCommitStrategy`, and `close()` is quiet.
- **`MqttSourceCommandTest`** — with a mocked `MqttClientProvider` (Mockito),
  assert `createExecutionContext` wires the fetcher/converter/encoder and that
  `commandName()`/`sourceType()` are correct.

No live-broker integration test is required for parity with the unit-focused
`activemqsource` tests; Paho client behavior is isolated behind
`MqttClientProvider` and mocked.

## Out of Scope (YAGNI)

- MQTT sink (publish) — separate command if ever needed.
- Per-message commit / QoS-2 manual acknowledgment — fixed `NONE` per decision.
- Keystore/truststore TLS configuration — relies on JVM defaults + URL scheme.
- Shared subscriptions, topic-alias tuning, will messages.
