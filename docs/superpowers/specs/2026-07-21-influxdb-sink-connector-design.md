# InfluxDB Sink Connector (FLE-2204)

## Summary

A SINK connector `influxdbsink` that writes zephflow records to InfluxDB using the
**v2 write API** (`com.influxdb:influxdb-client-java`). The v2 write API also targets
InfluxDB 3.x (OSS / Cloud), so a single client covers 2.x and 3.x. InfluxDB 1.x is out
of scope.

## Motivation & the core mapping problem

InfluxDB is time-series oriented, but zephflow records are arbitrary structured data
(`RecordFleakData` — a map of typed fields). This is workable because an InfluxDB
**field** value may be a string, boolean, or number, and the timestamp is optional.
The design's substance is therefore the **mapping policy**: for each record, decide the
`measurement`, which keys become `tags` (indexed metadata) vs `fields` (data), and where
the `timestamp` comes from.

Decision: **explicit, config-declared mapping** (predictable, InfluxDB-idiomatic, avoids
tag-cardinality footguns). Selectors are **top-level record field names**; nested
extraction is done upstream with transforms.

## Data model reminder

Line protocol: `measurement,<tag_set> <field_set> <timestamp>`
- **measurement** — required, like a table name.
- **tags** — indexed string key/values (low-cardinality metadata).
- **fields** — the data; scalar values (string / number / bool). At least one required.
- **timestamp** — optional; defaults to write time.

## Components

Package `lib/src/main/java/io/fleak/zephflow/lib/commands/influxdbsink/`. Mirrors the
Azure Event Hub sink, with a typed outbound point like SQS. `T = com.influxdb.client.write.Point`.

| File | Role |
|------|------|
| `InfluxDbSinkDto` | interface + `Config implements CommandConfig` |
| `InfluxDbSinkConfigValidator` | imperative validation (Guava `Preconditions`) |
| `InfluxDbSinkMessageProcessor` | mapping: `RecordFleakData` → `Point` |
| `InfluxDbSinkFlusher` | `Flusher<Point>` using `WriteApiBlocking` (synchronous) |
| `InfluxDbSinkCommand` | `SimpleSinkCommand<Point>` |
| `InfluxDbSinkCommandFactory` | `CommandFactory`, `commandType() = SINK` |
| `InfluxDbClientProvider` | injectable, `Serializable` client factory (named to avoid clash with the client lib's own `InfluxDBClientFactory`); lets tests inject a mock |

The processor builds the `Point` (unit-testable via `point.toLineProtocol()`); the flusher
writes the batch and maps failures.

## Config schema

```java
class Config implements CommandConfig {
  // connection
  @NonNull String url;              // e.g. https://influx:8086
  @NonNull String org;
  @NonNull String bucket;
  String credentialId;              // -> ApiKeyCredential.key (token). Unset = no auth.

  // mapping (all selectors are top-level record field names)
  String measurement;               // literal measurement name
  String measurementField;          // OR source measurement from this record field
  @Builder.Default List<String> tagFields = [];   // -> string tags
  List<String> fieldFields;         // -> typed fields; null/empty = all remaining keys
  String timestampField;            // optional; null = now()
  @Builder.Default WritePrecision precision = MS; // NS | US | MS | S

  // batching
  @Builder.Default Integer batchSize = 1000;
}
```

Rules:
- Exactly one of `measurement` / `measurementField` must be set (validator enforced).
- Token comes only from `credentialId`; there is no inline token field. Unset `credentialId`
  means connect without auth (local/unsecured InfluxDB).

## Mapping semantics

For each record:
- **Measurement** — `measurement` literal, or the string value of the `measurementField` key.
- **Tags** — for each name in `tagFields`, add `name = String.valueOf(value)` (tags are always
  strings). Missing/null value → tag skipped.
- **Fields** — for each name in `fieldFields` (or *all remaining* keys when unset, excluding
  those used as tags / timestamp / measurementField):
  - `Number` LONG → integer field; DOUBLE → float field
  - `Boolean` → boolean field
  - `String` → string field
  - nested object / array → **JSON-stringified string field** (no data silently dropped)
- **Timestamp** — from `timestampField`: numeric → epoch at configured `precision`; ISO-8601
  string → parsed; missing/unparseable when a `timestampField` is configured → record error.
  When `timestampField` is unset → `Instant.now()`.
- **Precision** — applied per point.

## Error handling

Grounded in `SimpleSinkCommand`:
- **Mapping problems throw in `preprocess`** → that single record becomes an `ErrorOutput`,
  the rest of the batch proceeds. Cases: no measurement resolved; **zero valid fields**
  (InfluxDB requires ≥1 field); configured timestamp missing/unparseable.
- **Write/connectivity failure** → the flusher lets `InfluxException` propagate → the whole
  batch is treated as failed (store-and-forward compatible). `flushedDataSize` = Σ
  line-protocol bytes of the batch.

## Registration & SDK

- Add `COMMAND_NAME_INFLUXDB_SINK = "influxdbsink"` in `MiscUtils`.
- Register `new InfluxDbSinkCommandFactory()` in `OperatorCommandRegistry.OPERATOR_COMMANDS`.
- Update `OperatorCommandRegistryTest`.
- Add an `influxDbSink(...)` fluent builder to `sdk/.../ZephFlow.java`.

## Testing

- **Unit (Mockito / JUnit 5):**
  - `InfluxDbSinkMessageProcessorTest` — all mapping cases: value types, nested→JSON, tags,
    measurement literal vs field, timestamp numeric/ISO/missing, zero-field error, all-remaining
    default field selection.
  - `InfluxDbSinkConfigValidatorTest` — required fields, exactly-one-of measurement rule,
    precision, credential lookup.
  - `InfluxDbSinkDtoTest` — JSON config parse round-trip.
  - `InfluxDbSinkFlusherTest` — mock `WriteApiBlocking`: success, write-throws-whole-batch,
    flushedDataSize accounting.
- **Integration (TestContainers):** add `testcontainers-influxdb` to the version catalog and
  `lib` `testImplementation`; start an `influxdb:2` container, write real points, query them
  back to assert (real test, not a documented caveat).

## Out of scope (YAGNI)

v1 API; async/fire-and-forget `WriteApi`; inline token; path-expression selectors; per-field
key renaming; retention-policy / bucket auto-creation.
