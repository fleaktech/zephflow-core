# Azure IoT Hub Source Connector — Design

**Ticket:** FLE-2198 (follow-on to the Azure Event Hub connector)
**Date:** 2026-07-16
**Scope:** Source node only. The sink node is explicitly out of scope.

## Background

Azure IoT Hub exposes a built-in **Event Hub-compatible endpoint** for reading
device-to-cloud telemetry. That endpoint speaks the same AMQP protocol as Azure
Event Hub and is consumed with the identical Azure SDK
(`EventProcessorClient`, blob-backed `CheckpointStore`). Consequently the IoT Hub
source reuses the Event Hub source machinery already in this repository and adds
only the IoT-specific concerns: naming/documentation and surfacing IoT message
metadata (device identity) onto each emitted event.

Cloud-to-device messaging (a would-be "sink") uses a different SDK
(`iot-service-client`), a service connection string, and per-message device
addressing. It is **not** part of this work.

## Goals

- Add an `azureiothubsource` streaming source that consumes device telemetry from
  an IoT Hub built-in Event Hub-compatible endpoint.
- Surface IoT message metadata — originating device ID, enqueued time, and
  user-defined application properties — on each output event, under a namespaced
  envelope key so the message body is left untouched.
- Reuse the existing Event Hub auth, client-factory, checkpointing, and
  push-to-pull fetcher logic without letting the two connectors drift apart.

## Non-goals

- No IoT Hub sink / cloud-to-device / direct-method support.
- No device/twin management, no IoT Hub service SDK dependency.
- No change to Event Hub connector behavior.

## Architecture

### Reused as-is (shared `io.fleak.zephflow.lib.commands.azureeventhub` package)

- `AzureEventHubClientFactory` — builds the `EventProcessorClient` and the
  blob-backed `CheckpointStore`. The IoT Hub built-in endpoint is an Event Hub,
  so no new client wiring is required.
- `AzureEventHubConnectionConfig` — SAS connection string vs Entra ID
  (`TokenCredential`) auth resolution.
- `AzureEventHubConfigValidation.validateEventHubAuth(...)` — shared auth checks.

### New package `io.fleak.zephflow.lib.commands.azureiothubsource`

- `AzureIotHubSourceDto` — `interface` containing `class Config implements
  CommandConfig` (Lombok `@Data @Builder @NoArgsConstructor @AllArgsConstructor`)
  plus the `CommitStrategyType` and `InitialPosition` enums, mirroring
  `AzureEventHubSourceDto`. Field Javadoc points users at the IoT Hub →
  *Built-in endpoints* portal screen for the Event Hub-compatible connection
  string and name.
- `AzureIotHubSourceCommand extends SimpleSourceCommand<SerializedEvent>` —
  mirrors `AzureEventHubSourceCommand`; builds the processor client + checkpoint
  store via the shared factory, wires the IoT metadata extractor into the fetcher,
  and uses `IotHubRawDataConverter`. `sourceType()` returns `STREAMING`;
  `commandName()` returns `COMMAND_NAME_AZURE_IOTHUB_SOURCE`.
- `AzureIotHubSourceCommandFactory extends SourceCommandFactory` — constructs the
  command with a real `AzureEventHubClientFactory`.
- `AzureIotHubSourceConfigValidator implements ConfigValidator` — same checks as
  the Event Hub source validator (delegates auth to
  `AzureEventHubConfigValidation`, validates checkpoint store, encoding, buffering,
  and commit-strategy fields).
- `IotHubRawDataConverter implements RawDataConverter<SerializedEvent>` —
  deserializes the message body with the configured `EncodingType` (as
  `BytesRawDataConverter` does), then nests the IoT metadata carried on
  `SerializedEvent.metadata()` under a reserved `_iothub` key on each resulting
  `RecordFleakData`. The message body's own fields remain at the top level.

### Shared fetcher refactor (minimal, behavior-preserving)

`AzureEventHubSourceFetcher` is generalized to accept an injected
`Function<EventContext, Map<String, String>> metadataExtractor`:

- The Event Hub source command passes an extractor that returns `null`
  (equivalent to today's `new SerializedEvent(key, body, null)` — behavior
  unchanged; existing Event Hub tests continue to pass).
- The IoT Hub source command passes an extractor that reads from the
  `EventData` system/application properties and builds the metadata map.

This avoids duplicating the ~130 lines of queue-draining and per-partition
checkpoint logic and keeps the two connectors aligned. (Rejected alternative: a
standalone duplicate IoT fetcher — more isolation but a standing drift risk.)

## Data flow

1. `EventProcessorClient` (pointed at the built-in Event Hub-compatible endpoint)
   pushes events via a callback, one thread per owned partition.
2. The callback enqueues onto a bounded `LinkedBlockingQueue<EventContext>`
   (size `maxBufferedEvents`); a full queue blocks the callback, applying
   backpressure to IoT Hub.
3. `AzureEventHubSourceFetcher.fetch()` drains the queue on the single polling
   thread (up to `maxEventsPerFetch`), and for each event calls the injected
   `metadataExtractor` to populate `SerializedEvent.metadata()`.
4. `IotHubRawDataConverter` deserializes the body and nests the `_iothub`
   metadata, producing `RecordFleakData` for the pipeline.
5. Offsets are checkpointed to Azure Blob storage per the configured commit
   strategy — identical to the Event Hub source.

## Output event shape

For a telemetry message whose body deserializes to `{"temp": 72, "humidity": 40}`:

```json
{
  "temp": 72,
  "humidity": 40,
  "_iothub": {
    "deviceId": "sensor-01",
    "enqueuedTime": "2026-07-16T10:00:00Z",
    "properties": { "schema": "v2" }
  }
}
```

- `deviceId` ← `iothub-connection-device-id` system property (omitted/absent when
  the message carries none).
- `enqueuedTime` ← the event's enqueued time, ISO-8601 string.
- `properties` ← user-defined application properties (empty object when none).

The `_iothub` key is reserved; it is namespaced to avoid collisions with body
fields.

## Configuration (`AzureIotHubSourceDto.Config`)

Same surface as `AzureEventHubSourceDto.Config`:

- **Connection / auth (exactly one mode):** `connectionString` (the Event
  Hub-compatible connection string from IoT Hub) **or** `fullyQualifiedNamespace`
  with optional Entra ID service principal (`tenantId` + `clientId` +
  `clientSecret`; all-or-none, requires namespace).
- `eventHubName` — the Event Hub-compatible name from the built-in endpoint.
- `consumerGroup` — default `$Default`.
- **Checkpoint store (exactly one storage auth mode):**
  `checkpointStorageConnectionString` **or** `checkpointStorageEndpoint`, plus
  required `checkpointContainerName`.
- `encodingType` — decoding of the message body.
- `initialPosition` — `EARLIEST` (default) / `LATEST`, used only where no
  checkpoint exists.
- `maxBufferedEvents` (default 1024), `maxEventsPerFetch` (default 500).
- `commitStrategy` — `PER_RECORD` / `BATCH` (default, `commitBatchSize` 1000,
  `commitIntervalMs` 5000) / `NONE`.

## Registration

- Add `COMMAND_NAME_AZURE_IOTHUB_SOURCE = "azureiothubsource"` to
  `io.fleak.zephflow.lib.utils.MiscUtils`.
- Add `.put(COMMAND_NAME_AZURE_IOTHUB_SOURCE, new AzureIotHubSourceCommandFactory())`
  (plus the import) to `OperatorCommandRegistry.OPERATOR_COMMANDS`.

## Testing

- **Unit**
  - `AzureIotHubSourceDtoTest` — serialization/defaults of `Config`.
  - `AzureIotHubSourceConfigValidatorTest` — auth, checkpoint, encoding, buffering,
    commit-strategy validation (mirrors the Event Hub validator test).
  - `IotHubRawDataConverterTest` — given a `SerializedEvent` whose metadata carries
    `deviceId`/`enqueuedTime`/`properties`, asserts the output record nests them
    under `_iothub` and leaves body fields at the top level; and the no-metadata
    case.
  - `AzureEventHubSourceFetcherTest` — extend to cover the injected metadata
    extractor (existing null-extractor behavior plus an extractor that populates
    metadata).
- **Integration** (`@Tag("integration")`, `@Testcontainers`, requires Docker)
  - Reuse the Event Hubs emulator + Azurite setup already established for the Event
    Hub integration test. Publish an event carrying a simulated
    `iothub-connection-device-id` (and an application property), consume it through
    the IoT Hub source, and assert the emitted record contains the body plus the
    `_iothub` envelope with the expected `deviceId`. Verify a checkpoint blob is
    written to Azurite.

## Dependencies

None new. `azure-messaging-eventhubs`,
`azure-messaging-eventhubs-checkpointstore-blob`, `azure-storage-blob`,
`azure-identity`, and `azure-core-http-okhttp` are already declared in
`gradle/libs.versions.toml` and `lib/build.gradle`, and the Testcontainers Azure
support is already a test dependency.

## Risks / notes

- The Event Hub-compatible endpoint surfaces `iothub-connection-device-id` as an
  `EventData` **system** property; application properties come from
  `EventData.getProperties()`. The extractor must read from the correct maps and
  tolerate absent keys.
- The shared-fetcher refactor changes `AzureEventHubSourceFetcher`'s constructor;
  the Event Hub source command and the fetcher's existing unit test must be updated
  in the same change to keep Event Hub behavior identical.
