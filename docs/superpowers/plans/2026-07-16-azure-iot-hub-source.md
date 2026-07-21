# Azure IoT Hub Source Connector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an `azureiothubsource` streaming source that consumes device-to-cloud telemetry from an Azure IoT Hub built-in Event Hub-compatible endpoint, surfacing IoT message metadata (device ID, enqueued time, application properties) on each event.

**Architecture:** The IoT Hub built-in endpoint is an Event Hub, so the connector reuses the existing shared `azureeventhub` auth/client-factory/checkpoint machinery and the `AzureEventHubSourceFetcher`. A new `azureiothubsource` package adds the IoT-specific DTO, command, factory, config validator, an IoT metadata extractor, and a custom `RawDataConverter` that nests metadata under a reserved `_iothub` envelope key. The shared fetcher gains one backward-compatible overloaded constructor accepting a metadata extractor.

**Tech Stack:** Java 17+, Azure SDK (`azure-messaging-eventhubs`, `azure-messaging-eventhubs-checkpointstore-blob`, `azure-storage-blob`, `azure-identity`), Lombok, JUnit 5, Mockito, Testcontainers. Build: Gradle (`./gradlew`).

## Global Constraints

- License header: every new `.java` file MUST begin with the Apache 2.0 header block (copy verbatim from any existing file, e.g. `AzureEventHubSourceFetcher.java`, "Copyright 2025 Fleak Tech Inc.").
- Command name convention: lowercase, no separators, `<service><role>`. This connector's command name is exactly `azureiothubsource`.
- No new dependencies — all required Azure and test libraries are already declared in `gradle/libs.versions.toml` and `lib/build.gradle`.
- Do NOT change Azure Event Hub connector behavior. The shared fetcher change MUST be additive (new overloaded constructor) so the existing 5-arg call sites and their tests remain valid and unchanged.
- Reserved envelope key on output events: `_iothub`. Metadata sub-keys: `deviceId`, `enqueuedTime`, `properties`.
- Run a single test class with: `./gradlew :lib:test --tests 'fully.qualified.ClassName'`. Integration tests are tagged `integration` and require Docker.
- Code style is enforced by Spotless. Run `./gradlew :lib:spotlessApply` before committing.

---

### Task 1: Add metadata-extractor support to the shared fetcher

Generalize `AzureEventHubSourceFetcher` so it can attach per-event metadata, without changing existing behavior. This is the only modification to existing Event Hub code.

**Files:**
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/azureeventhubsource/AzureEventHubSourceFetcher.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/azureeventhubsource/AzureEventHubSourceFetcherTest.java`

**Interfaces:**
- Produces: a new public constructor
  `AzureEventHubSourceFetcher(EventProcessorClient, BlockingQueue<EventContext>, int, Duration, CommitStrategy, Function<EventContext, Map<String,String>> metadataExtractor)`.
  The existing 5-arg constructor is retained and delegates with an extractor that returns `null`. When the extractor returns a non-null map, it becomes `SerializedEvent.metadata()`.

- [ ] **Step 1: Write the failing test**

Add this test method to `AzureEventHubSourceFetcherTest`:

```java
  @Test
  void fetchAttachesMetadataFromExtractor() {
    BlockingQueue<EventContext> queue = new LinkedBlockingQueue<>();
    queue.add(eventContext("0", "hello"));

    AzureEventHubSourceFetcher fetcher =
        new AzureEventHubSourceFetcher(
            mock(EventProcessorClient.class),
            queue,
            500,
            SHORT_POLL,
            PerRecordCommitStrategy.INSTANCE,
            ctx -> java.util.Map.of("deviceId", "sensor-01"));

    List<SerializedEvent> events = fetcher.fetch();

    assertEquals(1, events.size());
    assertEquals("sensor-01", events.get(0).metadata().get("deviceId"));
  }

  @Test
  void fiveArgConstructorLeavesMetadataNull() {
    BlockingQueue<EventContext> queue = new LinkedBlockingQueue<>();
    queue.add(eventContext("0", "hello"));

    AzureEventHubSourceFetcher fetcher =
        new AzureEventHubSourceFetcher(
            mock(EventProcessorClient.class),
            queue,
            500,
            SHORT_POLL,
            PerRecordCommitStrategy.INSTANCE);

    List<SerializedEvent> events = fetcher.fetch();

    assertEquals(1, events.size());
    assertNull(events.get(0).metadata());
  }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.azureeventhubsource.AzureEventHubSourceFetcherTest'`
Expected: compile failure / FAIL — the 6-arg constructor does not exist yet.

- [ ] **Step 3: Implement the change**

In `AzureEventHubSourceFetcher.java`:

Add imports:
```java
import java.util.function.Function;
```

Add the field (next to the other final fields):
```java
  private final Function<EventContext, Map<String, String>> metadataExtractor;
```

Replace the existing constructor with these two:
```java
  public AzureEventHubSourceFetcher(
      EventProcessorClient processorClient,
      BlockingQueue<EventContext> queue,
      int maxEventsPerFetch,
      Duration pollTimeout,
      CommitStrategy commitStrategy) {
    this(processorClient, queue, maxEventsPerFetch, pollTimeout, commitStrategy, ctx -> null);
  }

  public AzureEventHubSourceFetcher(
      EventProcessorClient processorClient,
      BlockingQueue<EventContext> queue,
      int maxEventsPerFetch,
      Duration pollTimeout,
      CommitStrategy commitStrategy,
      Function<EventContext, Map<String, String>> metadataExtractor) {
    this.processorClient = processorClient;
    this.queue = queue;
    this.maxEventsPerFetch = maxEventsPerFetch;
    this.pollTimeout = pollTimeout;
    this.commitStrategy = commitStrategy;
    this.metadataExtractor = metadataExtractor;
  }
```

Replace the body of `accept(...)` so the extractor supplies metadata:
```java
  private void accept(EventContext eventContext, List<SerializedEvent> rawEvents) {
    latestCheckpointByPartition.put(
        eventContext.getPartitionContext().getPartitionId(), eventContext);
    EventData eventData = eventContext.getEventData();
    byte[] key =
        eventData.getPartitionKey() == null
            ? null
            : eventData.getPartitionKey().getBytes(StandardCharsets.UTF_8);
    Map<String, String> metadata = metadataExtractor.apply(eventContext);
    rawEvents.add(new SerializedEvent(key, eventData.getBody(), metadata));
  }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.azureeventhubsource.AzureEventHubSourceFetcherTest'`
Expected: PASS (all methods, including the pre-existing ones).

- [ ] **Step 5: Commit**

```bash
./gradlew :lib:spotlessApply
git add lib/src/main/java/io/fleak/zephflow/lib/commands/azureeventhubsource/AzureEventHubSourceFetcher.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/azureeventhubsource/AzureEventHubSourceFetcherTest.java
git commit -m "FLE-2198: add optional metadata extractor to Event Hub source fetcher"
```

---

### Task 2: IoT Hub source configuration DTO

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/azureiothubsource/AzureIotHubSourceDto.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/azureiothubsource/AzureIotHubSourceDtoTest.java`

**Interfaces:**
- Produces: `AzureIotHubSourceDto.Config` (Lombok `@Data @Builder @NoArgsConstructor @AllArgsConstructor`, `implements CommandConfig`) with getters `getConnectionString()`, `getFullyQualifiedNamespace()`, `getEventHubName()`, `getConsumerGroup()`, `getTenantId()`, `getClientId()`, `getClientSecret()`, `getCheckpointStorageConnectionString()`, `getCheckpointStorageEndpoint()`, `getCheckpointContainerName()`, `getEncodingType()`, `getInitialPosition()`, `getMaxBufferedEvents()`, `getMaxEventsPerFetch()`, `getCommitStrategy()`, `getCommitBatchSize()`, `getCommitIntervalMs()`. Nested enums `CommitStrategyType { PER_RECORD, BATCH, NONE }` and `InitialPosition { EARLIEST, LATEST }`.

- [ ] **Step 1: Write the failing test**

```java
/* <Apache license header> */
package io.fleak.zephflow.lib.commands.azureiothubsource;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.serdes.EncodingType;
import org.junit.jupiter.api.Test;

class AzureIotHubSourceDtoTest {

  @Test
  void buildsWithDefaults() {
    AzureIotHubSourceDto.Config config =
        AzureIotHubSourceDto.Config.builder()
            .connectionString("Endpoint=sb://ns/;SharedAccessKeyName=k;SharedAccessKey=v")
            .eventHubName("hub")
            .checkpointStorageConnectionString("UseDevelopmentStorage=true")
            .checkpointContainerName("checkpoints")
            .encodingType(EncodingType.JSON_OBJECT)
            .build();

    assertEquals("$Default", config.getConsumerGroup());
    assertEquals(AzureIotHubSourceDto.InitialPosition.EARLIEST, config.getInitialPosition());
    assertEquals(1024, config.getMaxBufferedEvents());
    assertEquals(500, config.getMaxEventsPerFetch());
    assertEquals(AzureIotHubSourceDto.CommitStrategyType.BATCH, config.getCommitStrategy());
    assertEquals(1000, config.getCommitBatchSize());
    assertEquals(5000L, config.getCommitIntervalMs());
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.azureiothubsource.AzureIotHubSourceDtoTest'`
Expected: compile failure — `AzureIotHubSourceDto` does not exist.

- [ ] **Step 3: Create the DTO**

Create `AzureIotHubSourceDto.java` (Apache header, then):

```java
package io.fleak.zephflow.lib.commands.azureiothubsource;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.lib.serdes.EncodingType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Configuration for the Azure IoT Hub source connector. Consumes device-to-cloud telemetry from an
 * IoT Hub built-in <b>Event Hub-compatible endpoint</b> (IoT Hub portal → Hub settings → Built-in
 * endpoints). The connection string and name below are the Event Hub-compatible values found there.
 */
public interface AzureIotHubSourceDto {

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {

    // --- IoT Hub built-in endpoint connection / authentication (exactly one auth mode) ---

    /** Event Hub-compatible connection string for the IoT Hub built-in endpoint. */
    private String connectionString;

    /** Event Hub-compatible fully qualified namespace (Entra ID auth path). */
    private String fullyQualifiedNamespace;

    /** Event Hub-compatible name of the built-in endpoint. */
    private String eventHubName;

    /** Consumer group to read under. Defaults to the built-in {@code $Default} group. */
    @Builder.Default private String consumerGroup = "$Default";

    // --- Optional Entra ID service principal; when omitted, DefaultAzureCredential is used ---
    private String tenantId;
    private String clientId;
    private String clientSecret;

    // --- Blob checkpoint store (durable partition offsets; exactly one storage auth mode) ---

    /** Connection string for the Azure Storage account that backs the checkpoint store. */
    private String checkpointStorageConnectionString;

    /** Blob endpoint for the checkpoint storage account (Entra ID auth path). */
    private String checkpointStorageEndpoint;

    /** Blob container that holds checkpoint and ownership data. */
    private String checkpointContainerName;

    // --- Decoding of the telemetry message body ---
    private EncodingType encodingType;

    /** Where to start when a partition has no persisted checkpoint. */
    @Builder.Default private InitialPosition initialPosition = InitialPosition.EARLIEST;

    // --- Push-to-pull buffering ---
    @Builder.Default private int maxBufferedEvents = 1024;
    @Builder.Default private int maxEventsPerFetch = 500;

    // --- Commit (checkpoint) strategy ---
    @Builder.Default private CommitStrategyType commitStrategy = CommitStrategyType.BATCH;
    @Builder.Default private Integer commitBatchSize = 1000;
    @Builder.Default private Long commitIntervalMs = 5000L;
  }

  enum CommitStrategyType {
    PER_RECORD,
    BATCH,
    NONE
  }

  enum InitialPosition {
    EARLIEST,
    LATEST
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.azureiothubsource.AzureIotHubSourceDtoTest'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
./gradlew :lib:spotlessApply
git add lib/src/main/java/io/fleak/zephflow/lib/commands/azureiothubsource/AzureIotHubSourceDto.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/azureiothubsource/AzureIotHubSourceDtoTest.java
git commit -m "FLE-2198: add Azure IoT Hub source config DTO"
```

---

### Task 3: IoT metadata converter (`_iothub` envelope)

Deserializes the message body and nests IoT metadata under `_iothub`. Defines the metadata key constants shared with the extractor (Task 4).

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/azureiothubsource/IotHubRawDataConverter.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/azureiothubsource/IotHubRawDataConverterTest.java`

**Interfaces:**
- Consumes: `SerializedEvent.metadata()` (Task 1) — a flat `Map<String,String>` whose keys are `deviceId`, `enqueuedTime`, and `prop.<name>` entries for application properties (produced by Task 4).
- Produces: `class IotHubRawDataConverter implements RawDataConverter<SerializedEvent>`, constructor `IotHubRawDataConverter(FleakDeserializer<?> fleakDeserializer)`. Public constants: `ENVELOPE_KEY="_iothub"`, `DEVICE_ID_KEY="deviceId"`, `ENQUEUED_TIME_KEY="enqueuedTime"`, `PROPERTIES_KEY="properties"`, `PROPERTY_PREFIX="prop."`.

- [ ] **Step 1: Write the failing test**

```java
/* <Apache license header> */
package io.fleak.zephflow.lib.commands.azureiothubsource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IotHubRawDataConverterTest {

  private final FleakDeserializer<?> deserializer =
      DeserializerFactory.createDeserializerFactory(EncodingType.JSON_OBJECT).createDeserializer();

  @SuppressWarnings("unchecked")
  private SourceExecutionContext<SerializedEvent> ctx() {
    SourceExecutionContext<SerializedEvent> ctx = mock(SourceExecutionContext.class);
    when(ctx.dataSizeCounter()).thenReturn(mock(FleakCounter.class));
    when(ctx.inputEventCounter()).thenReturn(mock(FleakCounter.class));
    when(ctx.deserializeFailureCounter()).thenReturn(mock(FleakCounter.class));
    return ctx;
  }

  @Test
  void nestsMetadataUnderIothubAndKeepsBodyFields() {
    Map<String, String> metadata = new LinkedHashMap<>();
    metadata.put(IotHubRawDataConverter.DEVICE_ID_KEY, "sensor-01");
    metadata.put(IotHubRawDataConverter.ENQUEUED_TIME_KEY, "2026-07-16T10:00:00Z");
    metadata.put(IotHubRawDataConverter.PROPERTY_PREFIX + "schema", "v2");

    SerializedEvent event =
        new SerializedEvent(null, "{\"temp\":72}".getBytes(StandardCharsets.UTF_8), metadata);

    ConvertedResult<SerializedEvent> result =
        new IotHubRawDataConverter(deserializer).convert(event, ctx());

    RecordFleakData record = result.transformedData().get(0);
    Map<String, Object> map = record.unwrap();

    assertEquals(72.0, ((Number) map.get("temp")).doubleValue());

    @SuppressWarnings("unchecked")
    Map<String, Object> iothub = (Map<String, Object>) map.get("_iothub");
    assertEquals("sensor-01", iothub.get("deviceId"));
    assertEquals("2026-07-16T10:00:00Z", iothub.get("enqueuedTime"));

    @SuppressWarnings("unchecked")
    Map<String, Object> properties = (Map<String, Object>) iothub.get("properties");
    assertEquals("v2", properties.get("schema"));
  }

  @Test
  void omitsEnvelopeWhenNoMetadata() {
    SerializedEvent event =
        new SerializedEvent(null, "{\"temp\":72}".getBytes(StandardCharsets.UTF_8), null);

    ConvertedResult<SerializedEvent> result =
        new IotHubRawDataConverter(deserializer).convert(event, ctx());

    Map<String, Object> map = result.transformedData().get(0).unwrap();
    assertFalse(map.containsKey("_iothub"));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.azureiothubsource.IotHubRawDataConverterTest'`
Expected: compile failure — `IotHubRawDataConverter` does not exist.

> Note: `ConvertedResult` is a record; its events list accessor is `transformedData()` and it is built with `ConvertedResult.success(events, sourceRecord)` / `ConvertedResult.failure(error, sourceRecord)`.

- [ ] **Step 3: Create the converter**

Create `IotHubRawDataConverter.java` (Apache header, then):

```java
package io.fleak.zephflow.lib.commands.azureiothubsource;

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static io.fleak.zephflow.lib.utils.MiscUtils.getCallingUserTagAndEventTags;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.RawDataConverter;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.compression.Decompressor;
import io.fleak.zephflow.lib.serdes.compression.DecompressorFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * Deserializes an IoT Hub telemetry message body and nests IoT metadata (device id, enqueued time,
 * application properties) under a reserved {@code _iothub} envelope key, leaving the body's own
 * fields at the top level. Metadata is carried on {@link SerializedEvent#metadata()} as a flat map
 * (see {@link IotHubMetadataExtractor}); application properties arrive prefixed with {@link
 * #PROPERTY_PREFIX} and are re-nested under {@code properties} here.
 */
@Slf4j
public class IotHubRawDataConverter implements RawDataConverter<SerializedEvent> {

  public static final String ENVELOPE_KEY = "_iothub";
  public static final String DEVICE_ID_KEY = "deviceId";
  public static final String ENQUEUED_TIME_KEY = "enqueuedTime";
  public static final String PROPERTIES_KEY = "properties";
  public static final String PROPERTY_PREFIX = "prop.";

  private final FleakDeserializer<?> fleakDeserializer;
  private final Decompressor decompressor;

  public IotHubRawDataConverter(FleakDeserializer<?> fleakDeserializer) {
    this(fleakDeserializer, DecompressorFactory.getDecompressor(List.of()));
  }

  public IotHubRawDataConverter(
      FleakDeserializer<?> fleakDeserializer, Decompressor decompressor) {
    this.fleakDeserializer = fleakDeserializer;
    this.decompressor = decompressor;
  }

  @Override
  public ConvertedResult<SerializedEvent> convert(
      SerializedEvent sourceRecord, SourceExecutionContext<?> sourceInitializedConfig) {
    try {
      List<RecordFleakData> deserialized =
          fleakDeserializer.deserialize(decompressor.decompress(sourceRecord));

      FleakData envelope = buildEnvelope(sourceRecord.metadata());
      List<RecordFleakData> events =
          envelope == null
              ? deserialized
              : deserialized.stream()
                  .map(r -> r.copyAndMerge(Map.of(ENVELOPE_KEY, envelope)))
                  .toList();

      Map<String, String> eventTags =
          getCallingUserTagAndEventTags(null, events.isEmpty() ? null : events.getFirst());
      sourceInitializedConfig.dataSizeCounter().increase(sourceRecord.value().length, eventTags);
      sourceInitializedConfig.inputEventCounter().increase(events.size(), eventTags);
      if (log.isDebugEnabled()) {
        events.forEach(e -> log.debug("got message: {}", toJsonString(e)));
      }
      return ConvertedResult.success(events, sourceRecord);
    } catch (Exception e) {
      sourceInitializedConfig.dataSizeCounter().increase(sourceRecord.value().length, Map.of());
      sourceInitializedConfig.deserializeFailureCounter().increase(Map.of());
      log.error("failed to deserialize IoT Hub event:\n{}", sourceRecord);
      return ConvertedResult.failure(e, sourceRecord);
    }
  }

  private static FleakData buildEnvelope(Map<String, String> metadata) {
    if (metadata == null || metadata.isEmpty()) {
      return null;
    }
    Map<String, Object> envelope = new LinkedHashMap<>();
    Map<String, Object> properties = new LinkedHashMap<>();
    for (Map.Entry<String, String> e : metadata.entrySet()) {
      if (e.getKey().startsWith(PROPERTY_PREFIX)) {
        properties.put(e.getKey().substring(PROPERTY_PREFIX.length()), e.getValue());
      } else {
        envelope.put(e.getKey(), e.getValue());
      }
    }
    if (!properties.isEmpty()) {
      envelope.put(PROPERTIES_KEY, properties);
    }
    return FleakData.wrap(envelope);
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.azureiothubsource.IotHubRawDataConverterTest'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
./gradlew :lib:spotlessApply
git add lib/src/main/java/io/fleak/zephflow/lib/commands/azureiothubsource/IotHubRawDataConverter.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/azureiothubsource/IotHubRawDataConverterTest.java
git commit -m "FLE-2198: add IoT Hub raw-data converter with _iothub envelope"
```

---

### Task 4: IoT metadata extractor

Reads IoT-specific properties off each `EventContext` into the flat metadata map the converter consumes.

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/azureiothubsource/IotHubMetadataExtractor.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/azureiothubsource/IotHubMetadataExtractorTest.java`

**Interfaces:**
- Consumes: `IotHubRawDataConverter.DEVICE_ID_KEY`, `ENQUEUED_TIME_KEY`, `PROPERTY_PREFIX` (Task 3).
- Produces: `class IotHubMetadataExtractor implements Function<EventContext, Map<String,String>>`, no-arg constructor. Returns `null` when no metadata is present (so the converter omits `_iothub`). Reads device id from the `iothub-connection-device-id` system property, enqueued time from `EventData.getEnqueuedTime()`, and each application property from `EventData.getProperties()` prefixed with `prop.`.

- [ ] **Step 1: Write the failing test**

```java
/* <Apache license header> */
package io.fleak.zephflow.lib.commands.azureiothubsource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.models.EventContext;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IotHubMetadataExtractorTest {

  private static EventContext eventContext(
      Map<String, Object> systemProperties, Instant enqueued, Map<String, Object> appProperties) {
    EventData eventData = mock(EventData.class);
    when(eventData.getSystemProperties()).thenReturn(systemProperties);
    when(eventData.getEnqueuedTime()).thenReturn(enqueued);
    when(eventData.getProperties()).thenReturn(appProperties);
    EventContext ctx = mock(EventContext.class);
    when(ctx.getEventData()).thenReturn(eventData);
    return ctx;
  }

  @Test
  void extractsDeviceIdEnqueuedTimeAndProperties() {
    EventContext ctx =
        eventContext(
            Map.of("iothub-connection-device-id", "sensor-01"),
            Instant.parse("2026-07-16T10:00:00Z"),
            Map.of("schema", "v2"));

    Map<String, String> metadata = new IotHubMetadataExtractor().apply(ctx);

    assertEquals("sensor-01", metadata.get("deviceId"));
    assertEquals("2026-07-16T10:00:00Z", metadata.get("enqueuedTime"));
    assertEquals("v2", metadata.get("prop.schema"));
  }

  @Test
  void returnsNullWhenNothingPresent() {
    EventContext ctx = eventContext(Map.of(), null, Map.of());
    assertNull(new IotHubMetadataExtractor().apply(ctx));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.azureiothubsource.IotHubMetadataExtractorTest'`
Expected: compile failure — `IotHubMetadataExtractor` does not exist.

- [ ] **Step 3: Create the extractor**

Create `IotHubMetadataExtractor.java` (Apache header, then):

```java
package io.fleak.zephflow.lib.commands.azureiothubsource;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.models.EventContext;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Extracts IoT Hub message metadata off each {@link EventContext} into the flat {@code
 * Map<String,String>} carried on {@code SerializedEvent.metadata()}. The IoT Hub built-in endpoint
 * exposes the originating device id as the {@code iothub-connection-device-id} system property;
 * enqueued time comes from {@link EventData#getEnqueuedTime()} and user-defined application
 * properties from {@link EventData#getProperties()}. Returns {@code null} when no metadata is
 * present so the converter emits no {@code _iothub} envelope.
 */
public class IotHubMetadataExtractor implements Function<EventContext, Map<String, String>> {

  static final String DEVICE_ID_SYSTEM_PROPERTY = "iothub-connection-device-id";

  @Override
  public Map<String, String> apply(EventContext eventContext) {
    EventData eventData = eventContext.getEventData();
    Map<String, String> metadata = new HashMap<>();

    Map<String, Object> systemProperties = eventData.getSystemProperties();
    if (systemProperties != null) {
      Object deviceId = systemProperties.get(DEVICE_ID_SYSTEM_PROPERTY);
      if (deviceId != null) {
        metadata.put(IotHubRawDataConverter.DEVICE_ID_KEY, String.valueOf(deviceId));
      }
    }

    if (eventData.getEnqueuedTime() != null) {
      metadata.put(
          IotHubRawDataConverter.ENQUEUED_TIME_KEY, eventData.getEnqueuedTime().toString());
    }

    Map<String, Object> properties = eventData.getProperties();
    if (properties != null) {
      for (Map.Entry<String, Object> e : properties.entrySet()) {
        if (e.getValue() != null) {
          metadata.put(
              IotHubRawDataConverter.PROPERTY_PREFIX + e.getKey(), String.valueOf(e.getValue()));
        }
      }
    }

    return metadata.isEmpty() ? null : metadata;
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.azureiothubsource.IotHubMetadataExtractorTest'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
./gradlew :lib:spotlessApply
git add lib/src/main/java/io/fleak/zephflow/lib/commands/azureiothubsource/IotHubMetadataExtractor.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/azureiothubsource/IotHubMetadataExtractorTest.java
git commit -m "FLE-2198: add IoT Hub metadata extractor"
```

---

### Task 5: IoT Hub source config validator

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/azureiothubsource/AzureIotHubSourceConfigValidator.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/azureiothubsource/AzureIotHubSourceConfigValidatorTest.java`

**Interfaces:**
- Consumes: `AzureIotHubSourceDto.Config` (Task 2); `AzureEventHubConfigValidation.validateEventHubAuth(...)` (existing shared).
- Produces: `class AzureIotHubSourceConfigValidator implements ConfigValidator` with `void validateConfig(CommandConfig, String nodeId, JobContext)`.

- [ ] **Step 1: Write the failing test**

```java
/* <Apache license header> */
package io.fleak.zephflow.lib.commands.azureiothubsource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.serdes.EncodingType;
import org.junit.jupiter.api.Test;

class AzureIotHubSourceConfigValidatorTest {

  private final AzureIotHubSourceConfigValidator validator = new AzureIotHubSourceConfigValidator();
  private final JobContext jobContext = mock(JobContext.class);

  private static AzureIotHubSourceDto.Config.ConfigBuilder validConnectionStringConfig() {
    return AzureIotHubSourceDto.Config.builder()
        .connectionString("Endpoint=sb://ns/;SharedAccessKeyName=k;SharedAccessKey=v")
        .eventHubName("hub")
        .consumerGroup("$Default")
        .checkpointStorageConnectionString("UseDevelopmentStorage=true")
        .checkpointContainerName("checkpoints")
        .encodingType(EncodingType.JSON_OBJECT);
  }

  private void validate(AzureIotHubSourceDto.Config config) {
    validator.validateConfig(config, "node", jobContext);
  }

  @Test
  void acceptsValidConnectionStringConfig() {
    assertDoesNotThrow(() -> validate(validConnectionStringConfig().build()));
  }

  @Test
  void acceptsValidEntraIdConfig() {
    AzureIotHubSourceDto.Config config =
        validConnectionStringConfig()
            .connectionString(null)
            .fullyQualifiedNamespace("ns.servicebus.windows.net")
            .tenantId("t")
            .clientId("c")
            .clientSecret("s")
            .build();
    assertDoesNotThrow(() -> validate(config));
  }

  @Test
  void rejectsMissingEventHubName() {
    var config = validConnectionStringConfig().eventHubName(" ").build();
    assertThrows(IllegalArgumentException.class, () -> validate(config));
  }

  @Test
  void rejectsWhenNoAuthProvided() {
    var config = validConnectionStringConfig().connectionString(null).build();
    assertThrows(IllegalArgumentException.class, () -> validate(config));
  }

  @Test
  void rejectsWhenBothAuthModesProvided() {
    var config =
        validConnectionStringConfig().fullyQualifiedNamespace("ns.servicebus.windows.net").build();
    assertThrows(IllegalArgumentException.class, () -> validate(config));
  }

  @Test
  void rejectsMissingCheckpointContainer() {
    var config = validConnectionStringConfig().checkpointContainerName(null).build();
    assertThrows(IllegalArgumentException.class, () -> validate(config));
  }

  @Test
  void rejectsBothCheckpointStorageAuthModes() {
    var config =
        validConnectionStringConfig().checkpointStorageEndpoint("https://acct.blob.core").build();
    assertThrows(IllegalArgumentException.class, () -> validate(config));
  }

  @Test
  void rejectsMissingEncodingType() {
    var config = validConnectionStringConfig().encodingType(null).build();
    assertThrows(Exception.class, () -> validate(config));
  }

  @Test
  void rejectsNonPositiveBufferSizes() {
    assertThrows(
        IllegalArgumentException.class,
        () -> validate(validConnectionStringConfig().maxBufferedEvents(0).build()));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.azureiothubsource.AzureIotHubSourceConfigValidatorTest'`
Expected: compile failure — validator does not exist.

- [ ] **Step 3: Create the validator**

Create `AzureIotHubSourceConfigValidator.java` (Apache header, then):

```java
package io.fleak.zephflow.lib.commands.azureiothubsource;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.azureeventhub.AzureEventHubConfigValidation;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import org.apache.commons.lang3.StringUtils;

/** Validates {@link AzureIotHubSourceDto.Config}. */
public class AzureIotHubSourceConfigValidator implements ConfigValidator {

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    AzureIotHubSourceDto.Config config = (AzureIotHubSourceDto.Config) commandConfig;

    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getEventHubName()), "no eventHubName is provided");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getConsumerGroup()), "no consumerGroup is provided");

    // Exactly one authentication mode (shared with the Event Hub connector).
    AzureEventHubConfigValidation.validateEventHubAuth(
        config.getConnectionString(),
        config.getFullyQualifiedNamespace(),
        config.getTenantId(),
        config.getClientId(),
        config.getClientSecret());

    // Checkpoint store: container required, plus exactly one storage auth mode.
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getCheckpointContainerName()),
        "no checkpointContainerName is provided");
    boolean hasStorageConnectionString =
        StringUtils.isNotBlank(config.getCheckpointStorageConnectionString());
    boolean hasStorageEndpoint = StringUtils.isNotBlank(config.getCheckpointStorageEndpoint());
    Preconditions.checkArgument(
        hasStorageConnectionString || hasStorageEndpoint,
        "checkpoint store requires either checkpointStorageConnectionString or checkpointStorageEndpoint");
    Preconditions.checkArgument(
        !(hasStorageConnectionString && hasStorageEndpoint),
        "provide only one of checkpointStorageConnectionString or checkpointStorageEndpoint, not both");

    Preconditions.checkNotNull(config.getEncodingType(), "no encoding type is provided");
    DeserializerFactory.validateEncodingType(config.getEncodingType());

    Preconditions.checkArgument(
        config.getMaxBufferedEvents() > 0,
        "maxBufferedEvents must be positive, got: %s",
        config.getMaxBufferedEvents());
    Preconditions.checkArgument(
        config.getMaxEventsPerFetch() > 0,
        "maxEventsPerFetch must be positive, got: %s",
        config.getMaxEventsPerFetch());

    if (config.getCommitStrategy() == AzureIotHubSourceDto.CommitStrategyType.BATCH) {
      if (config.getCommitBatchSize() != null) {
        Preconditions.checkArgument(
            config.getCommitBatchSize() > 0,
            "commitBatchSize must be positive, got: %s",
            config.getCommitBatchSize());
      }
      if (config.getCommitIntervalMs() != null) {
        Preconditions.checkArgument(
            config.getCommitIntervalMs() > 0,
            "commitIntervalMs must be positive, got: %s",
            config.getCommitIntervalMs());
      }
    }
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.azureiothubsource.AzureIotHubSourceConfigValidatorTest'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
./gradlew :lib:spotlessApply
git add lib/src/main/java/io/fleak/zephflow/lib/commands/azureiothubsource/AzureIotHubSourceConfigValidator.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/azureiothubsource/AzureIotHubSourceConfigValidatorTest.java
git commit -m "FLE-2198: add Azure IoT Hub source config validator"
```

---

### Task 6: IoT Hub source command and command factory

Wires the fetcher (with the IoT metadata extractor) and the IoT converter into a `SimpleSourceCommand`, and adds the factory. No compile-time registration yet (Task 7).

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/azureiothubsource/AzureIotHubSourceCommand.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/azureiothubsource/AzureIotHubSourceCommandFactory.java`

**Interfaces:**
- Consumes: `AzureEventHubClientFactory`, `AzureEventHubConnectionConfig` (shared); `AzureEventHubSourceFetcher` 6-arg constructor (Task 1); `IotHubMetadataExtractor` (Task 4); `IotHubRawDataConverter` (Task 3); `AzureIotHubSourceDto.Config` (Task 2); `AzureIotHubSourceConfigValidator` (Task 5); `MiscUtils.COMMAND_NAME_AZURE_IOTHUB_SOURCE` (added in Task 7).
- Produces: `AzureIotHubSourceCommand extends SimpleSourceCommand<SerializedEvent>` and `AzureIotHubSourceCommandFactory extends SourceCommandFactory` (no-arg constructor plus a `AzureEventHubClientFactory`-injecting constructor; `createCommand(String nodeId, JobContext) -> AzureIotHubSourceCommand`).

> **Ordering note:** this task references `COMMAND_NAME_AZURE_IOTHUB_SOURCE`, which Task 7 adds to `MiscUtils`. If implementing in strict order, add that one constant now (it is shown in Task 7 Step 3) and the code compiles; Task 7 then wires the registry and SDK. Either order is fine as long as both land before Task 8.

- [ ] **Step 1: Create the command**

Create `AzureIotHubSourceCommand.java` (Apache header, then):

```java
package io.fleak.zephflow.lib.commands.azureiothubsource;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import com.azure.messaging.eventhubs.CheckpointStore;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.models.ErrorContext;
import com.azure.messaging.eventhubs.models.EventContext;
import com.azure.messaging.eventhubs.models.EventPosition;
import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.azureeventhub.AzureEventHubClientFactory;
import io.fleak.zephflow.lib.commands.azureeventhub.AzureEventHubConnectionConfig;
import io.fleak.zephflow.lib.commands.azureeventhubsource.AzureEventHubSourceFetcher;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import lombok.extern.slf4j.Slf4j;

/**
 * Streaming source that consumes device-to-cloud telemetry from an Azure IoT Hub built-in Event
 * Hub-compatible endpoint. Reuses the Event Hub processor/checkpoint machinery and attaches IoT
 * metadata (device id, enqueued time, application properties) under a {@code _iothub} envelope.
 */
@Slf4j
public class AzureIotHubSourceCommand extends SimpleSourceCommand<SerializedEvent> {

  private static final Duration FETCH_POLL_TIMEOUT = Duration.ofMillis(1000);

  private final AzureEventHubClientFactory clientFactory;

  public AzureIotHubSourceCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      AzureEventHubClientFactory clientFactory) {
    super(nodeId, jobContext, configParser, configValidator);
    this.clientFactory = clientFactory;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    AzureIotHubSourceDto.Config config = (AzureIotHubSourceDto.Config) commandConfig;

    Fetcher<SerializedEvent> fetcher = createFetcher(config);
    RawDataEncoder<SerializedEvent> encoder = new BytesRawDataEncoder();
    RawDataConverter<SerializedEvent> converter = createRawDataConverter(config);

    var metricTags = basicCommandMetricTags(jobContext.getMetricTags(), commandName(), nodeId);
    FleakCounter dataSizeCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_EVENT_SIZE_COUNT, metricTags);
    FleakCounter inputEventCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_EVENT_COUNT, metricTags);
    FleakCounter deserializeFailureCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_DESER_ERR_COUNT, metricTags);

    String keyPrefix = (String) jobContext.getOtherProperties().get(JobContext.DATA_KEY_PREFIX);
    DlqWriter dlqWriter =
        Optional.of(jobContext)
            .map(JobContext::getDlqConfig)
            .map(c -> DlqWriterFactory.createDlqWriter(c, keyPrefix))
            .orElse(null);
    if (dlqWriter != null) {
      dlqWriter.open();
    }

    return new SourceExecutionContext<>(
        fetcher,
        converter,
        encoder,
        dataSizeCounter,
        inputEventCounter,
        deserializeFailureCounter,
        dlqWriter);
  }

  private Fetcher<SerializedEvent> createFetcher(AzureIotHubSourceDto.Config config) {
    AzureEventHubConnectionConfig connection = connectionConfig(config);

    CheckpointStore checkpointStore =
        clientFactory.createBlobCheckpointStore(
            connection,
            config.getCheckpointStorageConnectionString(),
            config.getCheckpointStorageEndpoint(),
            config.getCheckpointContainerName());

    BlockingQueue<EventContext> queue = new LinkedBlockingQueue<>(config.getMaxBufferedEvents());
    Consumer<EventContext> onEvent =
        eventContext -> {
          try {
            queue.put(eventContext);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        };
    Consumer<ErrorContext> onError =
        errorContext ->
            log.error(
                "IoT Hub processor error on partition {}",
                errorContext.getPartitionContext().getPartitionId(),
                errorContext.getThrowable());

    EventPosition initialPosition =
        config.getInitialPosition() == AzureIotHubSourceDto.InitialPosition.LATEST
            ? EventPosition.latest()
            : EventPosition.earliest();

    EventProcessorClient processorClient =
        clientFactory.createProcessorClient(
            connection,
            config.getConsumerGroup(),
            checkpointStore,
            initialPosition,
            onEvent,
            onError);

    AzureEventHubSourceFetcher fetcher =
        new AzureEventHubSourceFetcher(
            processorClient,
            queue,
            config.getMaxEventsPerFetch(),
            FETCH_POLL_TIMEOUT,
            createCommitStrategy(config),
            new IotHubMetadataExtractor());

    processorClient.start();
    return fetcher;
  }

  private static AzureEventHubConnectionConfig connectionConfig(AzureIotHubSourceDto.Config config) {
    return new AzureEventHubConnectionConfig(
        config.getConnectionString(),
        config.getFullyQualifiedNamespace(),
        config.getEventHubName(),
        config.getTenantId(),
        config.getClientId(),
        config.getClientSecret());
  }

  private RawDataConverter<SerializedEvent> createRawDataConverter(
      AzureIotHubSourceDto.Config config) {
    FleakDeserializer<?> deserializer =
        DeserializerFactory.createDeserializerFactory(config.getEncodingType())
            .createDeserializer();
    return new IotHubRawDataConverter(deserializer);
  }

  private CommitStrategy createCommitStrategy(AzureIotHubSourceDto.Config config) {
    return switch (config.getCommitStrategy()) {
      case PER_RECORD -> PerRecordCommitStrategy.INSTANCE;
      case BATCH ->
          new BatchCommitStrategy(
              config.getCommitBatchSize() != null ? config.getCommitBatchSize() : 1000,
              config.getCommitIntervalMs() != null ? config.getCommitIntervalMs() : 5000L);
      case NONE -> NoCommitStrategy.INSTANCE;
    };
  }

  @Override
  public SourceType sourceType() {
    return SourceType.STREAMING;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_AZURE_IOTHUB_SOURCE;
  }
}
```

- [ ] **Step 2: Create the command factory**

Create `AzureIotHubSourceCommandFactory.java` (Apache header, then):

```java
package io.fleak.zephflow.lib.commands.azureiothubsource;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.JsonConfigParser;
import io.fleak.zephflow.lib.commands.azureeventhub.AzureEventHubClientFactory;
import io.fleak.zephflow.lib.commands.source.SourceCommandFactory;

/** Creates {@link AzureIotHubSourceCommand} instances. */
public class AzureIotHubSourceCommandFactory extends SourceCommandFactory {

  private final AzureEventHubClientFactory clientFactory;

  public AzureIotHubSourceCommandFactory() {
    this(new AzureEventHubClientFactory());
  }

  public AzureIotHubSourceCommandFactory(AzureEventHubClientFactory clientFactory) {
    this.clientFactory = clientFactory;
  }

  @Override
  public AzureIotHubSourceCommand createCommand(String nodeId, JobContext jobContext) {
    JsonConfigParser<AzureIotHubSourceDto.Config> configParser =
        new JsonConfigParser<>(AzureIotHubSourceDto.Config.class);
    AzureIotHubSourceConfigValidator validator = new AzureIotHubSourceConfigValidator();
    return new AzureIotHubSourceCommand(
        nodeId, jobContext, configParser, validator, clientFactory);
  }
}
```

- [ ] **Step 3: Compile**

Run: `./gradlew :lib:compileJava`
Expected: BUILD SUCCESSFUL. (Requires `COMMAND_NAME_AZURE_IOTHUB_SOURCE` to exist — add it now per Task 7 Step 3 if it is not yet present.)

- [ ] **Step 4: Commit**

```bash
./gradlew :lib:spotlessApply
git add lib/src/main/java/io/fleak/zephflow/lib/commands/azureiothubsource/AzureIotHubSourceCommand.java \
        lib/src/main/java/io/fleak/zephflow/lib/commands/azureiothubsource/AzureIotHubSourceCommandFactory.java
git commit -m "FLE-2198: add Azure IoT Hub source command and factory"
```

---

### Task 7: Register the command and add SDK convenience method

**Files:**
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java` (constants block, near line 114)
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java` (imports + `OPERATOR_COMMANDS` builder)
- Modify: `sdk/src/main/java/io/fleak/zephflow/sdk/ZephFlow.java` (imports + new `iotHubSource(...)` method, mirroring `eventHubSource(...)` around line 528)
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistryTest.java` (create only if no registry test exists; otherwise extend the existing one — search first with `find lib/src/test -name 'OperatorCommandRegistry*'`)

**Interfaces:**
- Consumes: `AzureIotHubSourceCommandFactory` (Task 6), `AzureIotHubSourceDto.Config` (Task 2).
- Produces: constant `COMMAND_NAME_AZURE_IOTHUB_SOURCE = "azureiothubsource"`; a registry entry keyed by it; SDK method `ZephFlow.iotHubSource(...)`.

- [ ] **Step 1: Write the failing test**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistryTest.java` (Apache header, then):

```java
package io.fleak.zephflow.lib.commands;

import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_AZURE_IOTHUB_SOURCE;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.commands.azureiothubsource.AzureIotHubSourceCommandFactory;
import org.junit.jupiter.api.Test;

class OperatorCommandRegistryTest {

  @Test
  void registersIotHubSource() {
    assertEquals("azureiothubsource", COMMAND_NAME_AZURE_IOTHUB_SOURCE);
    assertInstanceOf(
        AzureIotHubSourceCommandFactory.class,
        OperatorCommandRegistry.OPERATOR_COMMANDS.get(COMMAND_NAME_AZURE_IOTHUB_SOURCE));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.OperatorCommandRegistryTest'`
Expected: compile failure — constant / factory not referenced from the registry yet.

- [ ] **Step 3: Add the command-name constant**

In `MiscUtils.java`, immediately after the line
`String COMMAND_NAME_AZURE_EVENTHUB_SINK = "azureeventhubsink";`
add:

```java
  String COMMAND_NAME_AZURE_IOTHUB_SOURCE = "azureiothubsource";
```

- [ ] **Step 4: Register the factory**

In `OperatorCommandRegistry.java`, add the import (next to the existing Azure Event Hub imports):

```java
import io.fleak.zephflow.lib.commands.azureiothubsource.AzureIotHubSourceCommandFactory;
```

And add one builder line in the `OPERATOR_COMMANDS` map (next to the Azure Event Hub entries):

```java
          .put(COMMAND_NAME_AZURE_IOTHUB_SOURCE, new AzureIotHubSourceCommandFactory())
```

Confirm `COMMAND_NAME_AZURE_IOTHUB_SOURCE` is statically imported — `OperatorCommandRegistry` uses `import static io.fleak.zephflow.lib.utils.MiscUtils.*;` (verify the wildcard static import is present; the other command names rely on it).

- [ ] **Step 5: Add the SDK convenience method**

In `ZephFlow.java`, add the import near the existing `AzureEventHubSourceDto` import:

```java
import io.fleak.zephflow.lib.commands.azureiothubsource.AzureIotHubSourceDto;
```

Add this method directly after `eventHubSource(...)` (it ends around line 545, right before the `eventHubSink` Javadoc):

```java
  /**
   * Appends an Azure IoT Hub source node to the flow. Consumes device-to-cloud telemetry from the
   * IoT Hub built-in Event Hub-compatible endpoint, authenticated with the Event Hub-compatible SAS
   * connection string, checkpointing partition offsets to Azure Blob Storage. Device id, enqueued
   * time and application properties are attached to each event under an {@code _iothub} key. For
   * Entra ID authentication or advanced tuning, build an {@link AzureIotHubSourceDto.Config} and use
   * {@link #appendNode(String, CommandConfig)} directly.
   *
   * @param connectionString The IoT Hub Event Hub-compatible SAS connection string.
   * @param eventHubName The Event Hub-compatible name of the built-in endpoint.
   * @param consumerGroup The consumer group to read under (e.g. {@code $Default}).
   * @param checkpointStorageConnectionString Connection string for the checkpoint storage account.
   * @param checkpointContainerName Blob container holding checkpoint and ownership data.
   * @param encodingType The encoding of the telemetry messages.
   * @return A new ZephFlow instance with the IoT Hub source appended.
   */
  public ZephFlow iotHubSource(
      @NonNull String connectionString,
      @NonNull String eventHubName,
      @NonNull String consumerGroup,
      @NonNull String checkpointStorageConnectionString,
      @NonNull String checkpointContainerName,
      @NonNull EncodingType encodingType) {
    AzureIotHubSourceDto.Config config =
        AzureIotHubSourceDto.Config.builder()
            .connectionString(connectionString)
            .eventHubName(eventHubName)
            .consumerGroup(consumerGroup)
            .checkpointStorageConnectionString(checkpointStorageConnectionString)
            .checkpointContainerName(checkpointContainerName)
            .encodingType(encodingType)
            .build();
    return appendNode(COMMAND_NAME_AZURE_IOTHUB_SOURCE, config);
  }
```

Confirm `COMMAND_NAME_AZURE_IOTHUB_SOURCE` resolves in `ZephFlow.java` the same way `COMMAND_NAME_AZURE_EVENTHUB_SOURCE` does (follow the existing import style for that constant — static import or fully-qualified).

- [ ] **Step 6: Run tests to verify they pass**

Run:
```bash
./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.OperatorCommandRegistryTest'
./gradlew :sdk:compileJava
```
Expected: PASS and BUILD SUCCESSFUL.

- [ ] **Step 7: Commit**

```bash
./gradlew spotlessApply
git add lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java \
        lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistryTest.java \
        sdk/src/main/java/io/fleak/zephflow/sdk/ZephFlow.java
git commit -m "FLE-2198: register Azure IoT Hub source command and SDK builder"
```

---

### Task 8: End-to-end integration test

Consumes telemetry through the full command against the Event Hubs emulator + Azurite (the IoT Hub built-in endpoint is Event Hub-compatible, so the emulator is the correct stand-in). Verifies the `_iothub` envelope surfaces application properties and enqueued time, and that checkpoints are written.

**Files:**
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/azureiothubsource/AzureIotHubSourceIntegrationTest.java`

**Interfaces:**
- Consumes: `AzureIotHubSourceCommandFactory` (Task 6), `AzureIotHubSourceDto` (Task 2). Publishes with a raw `EventHubProducerClient` so application properties can be set (the Event Hub sink does not set them). The emulator cannot inject the `iothub-connection-device-id` system property, so `deviceId` is asserted only in the Task 4 unit test; here we assert application properties and enqueued time.

> Before writing, confirm the emulator image tags and the Azurite version pin from the recalled setup note ("Event Hub emulator + Testcontainers"). Reuse the exact `EMULATOR_CONFIG_JSON`, image tags, and container wiring from `lib/src/test/java/io/fleak/zephflow/lib/commands/azureeventhub/AzureEventHubIntegrationTest.java` (entity `eh1`, consumer group `cg1`, container `checkpoints`).

- [ ] **Step 1: Write the integration test**

Create `AzureIotHubSourceIntegrationTest.java` (Apache header, then):

```java
package io.fleak.zephflow.lib.commands.azureiothubsource;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventHubClientBuilder;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.azure.AzuriteContainer;
import org.testcontainers.azure.EventHubsEmulatorContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * End-to-end test of the IoT Hub source against the Event Hubs emulator (the IoT Hub built-in
 * endpoint is Event Hub-compatible) plus Azurite for checkpoint storage. Requires Docker.
 */
@Slf4j
@Tag("integration")
@Testcontainers
public class AzureIotHubSourceIntegrationTest {

  private static final String EVENT_HUB_NAME = "eh1";
  private static final String CONSUMER_GROUP = "cg1";
  private static final String CHECKPOINT_CONTAINER = "checkpoints";

  private static final String EMULATOR_CONFIG_JSON =
      """
      {
        "UserConfig": {
          "NamespaceConfig": [
            {
              "Type": "EventHub",
              "Name": "emulatorNs1",
              "Entities": [
                {
                  "Name": "eh1",
                  "PartitionCount": "1",
                  "ConsumerGroups": [ { "Name": "cg1" } ]
                }
              ]
            }
          ],
          "LoggingConfig": { "Type": "File" }
        }
      }
      """;

  private static final Network NETWORK = Network.newNetwork();
  private static AzuriteContainer azurite;
  private static EventHubsEmulatorContainer eventHubs;

  @BeforeAll
  static void startContainers() {
    azurite =
        new AzuriteContainer("mcr.microsoft.com/azure-storage/azurite:3.35.0").withNetwork(NETWORK);
    azurite.start();

    eventHubs =
        new EventHubsEmulatorContainer("mcr.microsoft.com/azure-messaging/eventhubs-emulator:2.1.0")
            .acceptLicense()
            .withNetwork(NETWORK)
            .withAzuriteContainer(azurite)
            .withConfig(Transferable.of(EMULATOR_CONFIG_JSON));
    eventHubs.start();

    new BlobContainerClientBuilder()
        .connectionString(azurite.getConnectionString())
        .containerName(CHECKPOINT_CONTAINER)
        .buildClient()
        .createIfNotExists();
  }

  @AfterAll
  static void stopContainers() {
    if (eventHubs != null) {
      eventHubs.stop();
    }
    if (azurite != null) {
      azurite.stop();
    }
    NETWORK.close();
  }

  @Test
  void surfacesApplicationPropertiesUnderIothubEnvelope() throws Exception {
    publishTelemetryWithProperties();

    CollectingAcceptor acceptor = new CollectingAcceptor(1);
    AzureIotHubSourceCommand sourceCommand = buildSourceCommand();

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      executor.submit(
          () -> {
            sourceCommand.initialize(new MetricClientProvider.NoopMetricClientProvider());
            sourceCommand.execute("test_user", acceptor);
          });

      assertTrue(
          acceptor.latch.await(90, TimeUnit.SECONDS), "did not receive the event within timeout");

      Map<String, Object> record = acceptor.received.get(0).unwrap();
      assertEquals(72.0, ((Number) record.get("temp")).doubleValue());

      @SuppressWarnings("unchecked")
      Map<String, Object> iothub = (Map<String, Object>) record.get("_iothub");
      assertNotNull(iothub, "expected _iothub envelope");
      assertNotNull(iothub.get("enqueuedTime"), "expected enqueuedTime in _iothub");

      @SuppressWarnings("unchecked")
      Map<String, Object> properties = (Map<String, Object>) iothub.get("properties");
      assertNotNull(properties, "expected application properties in _iothub");
      assertEquals("v2", properties.get("schema"));
    } finally {
      sourceCommand.terminate();
      executor.shutdownNow();
      //noinspection ResultOfMethodCallIgnored
      executor.awaitTermination(10, TimeUnit.SECONDS);
    }
  }

  private void publishTelemetryWithProperties() {
    try (EventHubProducerClient producer =
        new EventHubClientBuilder()
            .connectionString(eventHubs.getConnectionString(), EVENT_HUB_NAME)
            .buildProducerClient()) {
      EventData event = new EventData("{\"temp\":72}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
      event.getProperties().put("schema", "v2");
      producer.send(List.of(event));
    }
  }

  private AzureIotHubSourceCommand buildSourceCommand() {
    AzureIotHubSourceDto.Config config =
        AzureIotHubSourceDto.Config.builder()
            .connectionString(eventHubs.getConnectionString())
            .eventHubName(EVENT_HUB_NAME)
            .consumerGroup(CONSUMER_GROUP)
            .checkpointStorageConnectionString(azurite.getConnectionString())
            .checkpointContainerName(CHECKPOINT_CONTAINER)
            .encodingType(EncodingType.JSON_OBJECT)
            .initialPosition(AzureIotHubSourceDto.InitialPosition.EARLIEST)
            .commitStrategy(AzureIotHubSourceDto.CommitStrategyType.PER_RECORD)
            .build();

    AzureIotHubSourceCommand command =
        (AzureIotHubSourceCommand)
            new AzureIotHubSourceCommandFactory().createCommand("source", TestUtils.JOB_CONTEXT);
    command.parseAndValidateArg(OBJECT_MAPPER.convertValue(config, new TypeReference<>() {}));
    return command;
  }

  private static class CollectingAcceptor implements SourceEventAcceptor {
    private final List<RecordFleakData> received = Collections.synchronizedList(new ArrayList<>());
    private final CountDownLatch latch;

    CollectingAcceptor(int expected) {
      this.latch = new CountDownLatch(expected);
    }

    @Override
    public void accept(List<RecordFleakData> records) {
      received.addAll(records);
      records.forEach(r -> latch.countDown());
    }

    @Override
    public void terminate() {}
  }
}
```

- [ ] **Step 2: Run the integration test (requires Docker)**

Run: `./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.azureiothubsource.AzureIotHubSourceIntegrationTest'`
Expected: PASS. The event is consumed, `_iothub.properties.schema == "v2"`, and `_iothub.enqueuedTime` is present.

If the integration task in this repo runs only under a specific Gradle task/profile for `@Tag("integration")` tests, use that (check `lib/build.gradle` for a `test`/`integrationTest` task filter); otherwise the command above runs it directly.

- [ ] **Step 3: Commit**

```bash
./gradlew :lib:spotlessApply
git add lib/src/test/java/io/fleak/zephflow/lib/commands/azureiothubsource/AzureIotHubSourceIntegrationTest.java
git commit -m "FLE-2198: add Azure IoT Hub source integration test"
```

---

### Task 9: Full verification

- [ ] **Step 1: Run the whole lib unit suite**

Run: `./gradlew :lib:test`
Expected: BUILD SUCCESSFUL. Confirm the existing Event Hub tests still pass (proves the fetcher change was behavior-preserving).

- [ ] **Step 2: Compile all modules**

Run: `./gradlew compileJava compileTestJava`
Expected: BUILD SUCCESSFUL (lib, sdk, and dependents).

- [ ] **Step 3: Verify Spotless is clean**

Run: `./gradlew spotlessCheck`
Expected: BUILD SUCCESSFUL. If it fails, run `./gradlew spotlessApply` and amend the last commit.

---

## Notes for the implementer

- The IoT Hub built-in endpoint is a real Event Hub. That is why every "Event Hub" class is reused verbatim for connection, checkpointing, and the processor client — do not reimplement them.
- The `_iothub` envelope is added via `RecordFleakData.copyAndMerge(...)`, which copies the payload; the original deserialized record is not mutated.
- `SerializedEvent.metadata()` is a flat `Map<String,String>`. Application properties are carried as `prop.<name>` entries and re-nested under `properties` by the converter — keep the `PROPERTY_PREFIX` constant the single source of truth shared between `IotHubMetadataExtractor` and `IotHubRawDataConverter`.
- The Event Hubs emulator cannot set the `iothub-connection-device-id` system property, so device-id mapping is proven by the `IotHubMetadataExtractor` unit test (mocked system properties), not the integration test.
```
