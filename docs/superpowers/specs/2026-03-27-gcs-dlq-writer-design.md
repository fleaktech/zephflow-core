# GCS Dead Letter Queue Writer Design

## Overview

Add GCP Cloud Storage (GCS) support for the Dead Letter Queue (DLQ) writer, allowing zephflow pipelines to send failed/dead-lettered messages to a GCS bucket. This mirrors the existing S3 DLQ writer functionality. Additionally, extract a `DlqWriterFactory` to centralize the duplicated DLQ dispatch logic across ~10 command classes.

## Configuration

New `GcsDlqConfig` added to `JobContext` alongside `S3DlqConfig`, registered via Jackson `@JsonSubTypes` with discriminator `"gcs"`.

### Config Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `type` | String | Yes | - | Must be `"gcs"` |
| `bucket` | String | Yes | - | GCS bucket name |
| `serviceAccountJson` | String | Yes | - | Full GCP service account JSON key |
| `batchSize` | int | Yes | - | Number of dead letters per batch before flushing |
| `flushIntervalMillis` | int | Yes | - | Max time between flushes in milliseconds |
| `rawDataSampleIntervalMs` | long | No | 60000 | Interval for raw data sampling |

### Example YAML

```yaml
dlqConfig:
  type: "gcs"
  bucket: "my-dlq-bucket"
  serviceAccountJson: "${GCP_SERVICE_ACCOUNT_JSON}"
  batchSize: 100
  flushIntervalMillis: 5000
  rawDataSampleIntervalMs: 60000
```

### Authentication

Service Account JSON key only. The full JSON string is provided via the `serviceAccountJson` config field. The `projectId` is inferred from the service account JSON automatically. No region field is needed since the GCS client resolves bucket location automatically.

## Architecture

### GcsClientFactory

New class at `lib/src/main/java/io/fleak/zephflow/lib/gcp/GcsClientFactory.java`, parallel to `AwsClientFactory`.

Responsibility: create a `com.google.cloud.storage.Storage` client from a service account JSON string.

```java
public class GcsClientFactory implements Serializable {
    public Storage createStorageClient(String serviceAccountJson) {
        GoogleCredentials credentials = ServiceAccountCredentials
            .fromStream(new ByteArrayInputStream(
                serviceAccountJson.getBytes(StandardCharsets.UTF_8)));
        return StorageOptions.newBuilder()
            .setCredentials(credentials)
            .build()
            .getService();
    }
}
```

### GcsDlqWriter

New class at `lib/src/main/java/io/fleak/zephflow/lib/dlq/GcsDlqWriter.java`, mirroring `S3DlqWriter`.

Key characteristics:
- Extends `DlqWriter` (abstract base class with `doWrite()`, `open()`, `close()`)
- Uses `BufferedWriter<DeadLetter>` for batching (same as S3)
- Reuses `DeadLetterCommiterSerializer` for Avro serialization (cloud-agnostic)
- Same object key pattern: `{keyPrefix}/{pathSegment}/dt={yyyy-MM-dd}/hr={HH}/{filePrefix}-{timestamp}_{uuid}.avro`
- Same retry logic: 3 attempts with linear backoff
- Two factory methods: `createGcsDlqWriter(GcsDlqConfig, keyPrefix)` and `createGcsSampleWriter(GcsDlqConfig, keyPrefix)`

Upload method uses `storage.create(BlobInfo, byte[])` for the actual GCS write.

### DlqWriterFactory

New class at `lib/src/main/java/io/fleak/zephflow/lib/dlq/DlqWriterFactory.java` that centralizes the `instanceof` dispatch logic currently duplicated across ~10 command classes.

Two static methods:
- `createDlqWriter(DlqConfig, keyPrefix)` - dispatches to S3 or GCS DLQ writer
- `createSampleWriter(DlqConfig, keyPrefix)` - dispatches to S3 or GCS sample writer

Both throw `UnsupportedOperationException` for unknown config types.

### Serializer Rename

`DeadLetterS3CommiterSerializer` is renamed to `DeadLetterCommiterSerializer` since it performs cloud-agnostic Avro serialization and is now shared between S3 and GCS writers.

## Object Key Pattern

Identical to the existing S3 pattern:

- DLQ: `{keyPrefix}/dead-letters/dt={yyyy-MM-dd}/hr={HH}/deadletter-{timestamp}_{uuid}.avro`
- Samples: `{keyPrefix}/raw-data-samples/dt={yyyy-MM-dd}/hr={HH}/sample-{timestamp}_{uuid}.avro`

Key prefix sanitization follows the same rules as S3: strip whitespace and leading/trailing slashes.

## Dependencies

Add to `lib/build.gradle`:
```groovy
implementation 'com.google.cloud:google-cloud-storage:<latest>'
testImplementation 'org.testcontainers:gcloud:<testcontainers-version>'
```

## Files Changed

### New Files
1. `lib/src/main/java/io/fleak/zephflow/lib/gcp/GcsClientFactory.java`
2. `lib/src/main/java/io/fleak/zephflow/lib/dlq/GcsDlqWriter.java`
3. `lib/src/main/java/io/fleak/zephflow/lib/dlq/DlqWriterFactory.java`
4. `lib/src/test/java/io/fleak/zephflow/lib/dlq/GcsDlqWriterTest.java`
5. `lib/src/test/java/io/fleak/zephflow/lib/dlq/GcsDlqWriterKeyPrefixTest.java`
6. `lib/src/test/java/io/fleak/zephflow/lib/dlq/DlqWriterFactoryTest.java`

### Modified Files
7. `api/src/main/java/io/fleak/zephflow/api/JobContext.java` - add `GcsDlqConfig` + `@JsonSubTypes` entry
8. `lib/build.gradle` - add `google-cloud-storage` and `testcontainers:gcloud` dependencies
9. `lib/src/main/java/io/fleak/zephflow/lib/dlq/DeadLetterS3CommiterSerializer.java` - rename to `DeadLetterCommiterSerializer`
10. `lib/src/main/java/io/fleak/zephflow/lib/dlq/S3DlqWriter.java` - update serializer reference
11. `lib/src/main/java/io/fleak/zephflow/lib/commands/kafkasource/KafkaSourceCommand.java` - replace `createDlqWriter()` with `DlqWriterFactory`
12. `lib/src/main/java/io/fleak/zephflow/lib/commands/kinesissource/KinesisSourceCommand.java` - same
13. `lib/src/main/java/io/fleak/zephflow/lib/commands/splunksource/SplunkSourceCommand.java` - same
14. `lib/src/main/java/io/fleak/zephflow/lib/commands/filesource/FileSourceCommand.java` - same
15. `lib/src/main/java/io/fleak/zephflow/lib/commands/stdin/StdInSourceCommand.java` - same
16. `lib/src/main/java/io/fleak/zephflow/lib/commands/reader/ReaderCommand.java` - same
17. `lib/src/main/java/io/fleak/zephflow/lib/commands/syslogudp/SyslogUdpCommand.java` - same
18. `lib/src/main/java/io/fleak/zephflow/lib/commands/s3/S3SinkCommand.java` - same
19. `lib/src/main/java/io/fleak/zephflow/lib/commands/databrickssink/DatabricksSinkCommand.java` - same
20. `lib/src/main/java/io/fleak/zephflow/lib/commands/deltalakesink/DeltaLakeSinkCommand.java` - same
21. `lib/src/main/java/io/fleak/zephflow/lib/commands/source/SimpleSourceCommand.java` - use `DlqWriterFactory.createSampleWriter()`

## Testing

### Integration Tests (GcsDlqWriterTest)
Uses Testcontainers with `fsouza/fake-gcs-server` Docker image, mirroring `S3DlqWriterTest` with MinIO.

Test cases:
- `testDeadLettersFlushedOnBatchSize` - write N records, verify one object appears in GCS
- `testDeadLettersFlushedOnFlushInterval` - write fewer than batch size, wait for timer, verify flush
- `testGcsObjectKeyFormat` - verify path matches expected pattern
- `testGcsObjectKeyFormatWithNullPrefix` - verify path works without prefix
- `testSampleWriterGcsObjectKeyFormat` - verify sample writer uses `raw-data-samples/` path
- `testSampleWriterGcsObjectKeyFormatWithNullPrefix` - same with null prefix
- `testUploadedDeadLettersContent` - read back Avro data, verify key/value/metadata match
- `testUploadedDeadLettersContainNodeId` - verify nodeId round-trips
- `testCreateGcsDlqWriterFromConfig` - end-to-end test using `GcsDlqConfig`
- `testCreateGcsSampleWriterFromConfig` - same for sample writer

### Unit Tests (GcsDlqWriterKeyPrefixTest)
Pure unit tests (no Docker), mirroring `S3DlqWriterKeyPrefixTest`:
- Clean prefix, leading/trailing slashes, whitespace, null prefix, all-slashes prefix

### Factory Tests (DlqWriterFactoryTest)
- Dispatches `S3DlqConfig` correctly
- Dispatches `GcsDlqConfig` correctly
- Same for `createSampleWriter`
- Throws `UnsupportedOperationException` for unknown config type

## Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Authentication | Service Account JSON key only | Simplest model, standard GCP artifact |
| Credential format | Single `serviceAccountJson` field | Less error-prone than separate fields |
| Raw data sampling | Full parity with S3 | Trivial once core writer exists |
| Object path pattern | Identical to S3 | Consistency across cloud providers |
| Region field | Not included | GCS client doesn't require it |
| Type discriminator | `"gcs"` | Specific to the storage service |
| Design approach | Direct mirror + DlqWriterFactory | Low risk, follows existing patterns, reduces duplication |
