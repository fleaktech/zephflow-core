# SplunkHecSink — Design

**Date:** 2026-04-29
**Status:** Approved (pending implementation)
**Scope:** A new sink node that delivers events to Splunk via the HTTP Event Collector (HEC) `/services/collector/event` endpoint.

## Goal

Add a `splunkhecsink` operator to zephflow-core so pipelines can deliver events to Splunk in the same shape that the Python reference sender (`splunk_hec_sender.py`) uses today: JSON-wrapped events, batched as NDJSON, authenticated with a HEC token.

## Scope

In scope:
- HEC `/services/collector/event` endpoint (JSON-wrapped events).
- Static, pipeline-level metadata (`index`, `sourcetype`, `source`).
- Required `credentialId` resolving to an `ApiKeyCredential` (the HEC token).
- Optional SSL verification toggle for self-signed Splunk deployments.
- Synchronous batched delivery using Java's built-in `HttpClient`.

Out of scope (intentionally — add later only if asked for):
- HEC `/raw` endpoint.
- Per-event metadata override from record fields (timestamp extraction, dynamic index, etc.).
- Sink-level retry/backoff (framework defaults apply).
- Integration tests against a real Splunk container.

## Architecture

The sink follows the established `SimpleSinkCommand<T>` pattern used by `ElasticsearchSink` and `AzureMonitorSink`. `SimpleSinkCommand` handles batching, metric tagging, and per-batch error merging; the new code provides a `Flusher`, a `MessageProcessor`, a `Config` DTO, a `Validator`, and a `CommandFactory`.

### Package layout

`lib/src/main/java/io/fleak/zephflow/lib/commands/splunkhecsink/`

| File | Role |
|---|---|
| `SplunkHecSinkCommand.java` | Extends `SimpleSinkCommand<SplunkHecOutboundEvent>`. Wires batch size, flusher, and message processor. |
| `SplunkHecSinkDto.java` | Interface holding the `Config` class. |
| `SplunkHecSinkCommandFactory.java` | Extends `CommandFactory`. Resolves credential, builds `HttpClient` (with optional SSL bypass), constructs flusher and command. |
| `SplunkHecSinkConfigValidator.java` | Validates the config. |
| `SplunkHecSinkFlusher.java` | Implements `SimpleSinkCommand.Flusher<SplunkHecOutboundEvent>`. Concatenates pre-encoded NDJSON lines, posts to HEC, parses response. |
| `SplunkHecSinkMessageProcessor.java` | Implements `SinkMessagePreProcessor<SplunkHecOutboundEvent>`. Wraps each `RecordFleakData` into the HEC envelope and pre-serializes. |
| `SplunkHecOutboundEvent.java` | Record holding `byte[] preEncodedNdjsonLine`. |

Naming choice: `splunkhecsink` (not `splunksink`) leaves room to add other Splunk ingest paths later without renaming.

## Configuration DTO

```java
@Data @Builder @NoArgsConstructor @AllArgsConstructor
class Config implements CommandConfig {
  @NonNull private String hecUrl;            // full URL, e.g. https://splunk:8088/services/collector/event
  @NonNull private String credentialId;      // resolves to an ApiKeyCredential (the HEC token)
  private String index;                      // optional — HEC token default applies if omitted
  private String sourcetype;                 // optional — HEC token default applies if omitted
  private String source;                     // optional pipeline-level tag
  @Builder.Default private Boolean verifySsl = true;
  @Builder.Default private Integer batchSize = 500;
}
```

Notes:
- `hecUrl` and `credentialId` are required. Everything else is optional.
- `index`, `sourcetype`, and `source` are static per pipeline. They are included in the per-event JSON envelope only when set, so HEC token defaults are honored when omitted.
- `batchSize = 500` matches `ElasticsearchSink` and `AzureMonitorSink` defaults.
- `verifySsl = true` is the safe default; users opt out for self-signed certs.

### Validation rules (`SplunkHecSinkConfigValidator`)

1. `hecUrl` parses as a URI with `http` or `https` scheme.
2. `credentialId` is non-blank and resolves to a non-null `ApiKeyCredential` (when credentials are enforced — uses `lookupApiKeyCredential` from `MiscUtils`).
3. `batchSize >= 1` if explicitly set.

## Data flow & wire format

### Per-record (in `SplunkHecSinkMessageProcessor.preprocess`)

For each `RecordFleakData`, build the HEC envelope:

```json
{"event": <record-as-json>, "sourcetype": "...", "index": "...", "source": "..."}
```

`sourcetype`, `index`, `source` keys are only included when their corresponding config values are non-null. Serialize with the project's `OBJECT_MAPPER`, append `\n`, return the bytes wrapped in `SplunkHecOutboundEvent`.

Pre-serializing per-event (rather than building the full NDJSON body in the flusher) matches the `ElasticsearchOutboundDoc` pattern and ensures any per-record JSON serialization failures are attributed to the right record before reaching the network.

### Per-batch (in `SplunkHecSinkFlusher.flush`)

1. Concatenate all `preEncodedNdjsonLine` byte arrays into a single body.
2. POST to `hecUrl` with headers:
   - `Authorization: Splunk <token>`
   - `Content-Type: application/json`
3. Use `HttpClient.send()` synchronously, request timeout 60s.

### HTTP client construction (in factory)

- Connect timeout: 30s.
- If `verifySsl == false`, build an `SSLContext` initialized with a trust-all `X509TrustManager` and pass it via `HttpClient.Builder#sslContext`. Wrapped in a clearly-named helper (e.g., `buildHttpClient(boolean verifySsl)`) so the unsafe path is easy to grep for.

## Failure semantics

Splunk HEC, unlike Elasticsearch's `_bulk` API, does not return per-event status. The whole batch either succeeds or fails based on a single HTTP response. Failure handling follows from this:

**Success (HTTP 200, body `{"text":"Success","code":0}`):**
Return `FlushResult(successCount = batch.size(), flushedDataSize = bodyBytes.length, errors = [])`.

**HTTP error (4xx / 5xx):**
- All records in the batch are reported as failed.
- Parse the response body as HEC error JSON (e.g., `{"text":"Invalid token","code":4}`) and use that text as the error reason. Fall back to `HTTP <status>: <truncated body>` if the body is not HEC JSON.
- Return `FlushResult(0, 0, errorOutputs)` where each `ErrorOutput` pairs a raw record with the same error reason.

**Network / IO exception:**
Throw from `flush()`. `SimpleSinkCommand` catches and marks all records failed with the exception message. Same as Elasticsearch.

**Auth:**
HEC tokens don't expire or rotate, so there is no 401-retry loop (unlike Azure Monitor). A 401 is reported as a normal HTTP error.

**Retries:**
None at the sink level. This matches `ElasticsearchSink` and `AzureMonitorSink` (both implement `Flusher` directly, not `AbstractBufferedFlusher`). Higher-level retry is the caller's responsibility.

### Known tradeoffs

1. **Partial-batch ambiguity:** If HEC returns 5xx after partially indexing some events, all are reported failed even though some may have been written. This is unavoidable without per-event ACKs from HEC; operators retrying will produce duplicates. This is the standard Splunk HEC caveat.
2. **No Splunk-specific DLQ logic:** Failed records flow to whatever DLQ `SimpleSinkCommand` already routes them to.

## Registration

Three small touches outside the new package:

1. **`lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java`** — add constant:
   ```java
   String COMMAND_NAME_SPLUNK_HEC_SINK = "splunkhecsink";
   ```
2. **`lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java`** — add map entry:
   ```java
   .put(COMMAND_NAME_SPLUNK_HEC_SINK, new SplunkHecSinkCommandFactory())
   ```
3. **No new dependencies** in `lib/build.gradle` — Java's built-in `HttpClient` and existing Jackson cover everything.

## Testing

Test directory: `lib/src/test/java/io/fleak/zephflow/lib/commands/splunkhecsink/`

### `SplunkHecSinkFlusherTest` (unit, mocked `HttpClient` — same shape as `AzureMonitorSinkFlusherTest`)

- `flush_emptyBatch_noHttpCall` — guard against empty batch.
- `flush_200_returnsSuccessForAllRecords` — happy path.
- `flush_4xxWithHecErrorJson_returnsErrorPerRecordWithHecReason` — verifies we surface HEC's `{"text":..., "code":...}` in the error reason.
- `flush_5xxWithNonJsonBody_returnsErrorPerRecordWithStatusFallback` — fallback path.
- `flush_401_returnsAuthErrorPerRecord` — bad token, no retry.
- `flush_networkException_propagates` — IOException bubbles out (framework handles).
- `flush_buildsValidNdjsonBody` — captures the request body, asserts each line is valid JSON with the expected envelope keys.

### `SplunkHecSinkMessageProcessorTest` (unit)

- Renders envelope with `sourcetype` / `index` / `source` when set.
- Omits those keys when null in config (so HEC token defaults apply).
- Output is valid JSON terminated with `\n`.

### `SplunkHecSinkConfigValidatorTest` (unit)

- Rejects blank `hecUrl`, malformed URL, non-`http(s)` scheme.
- Rejects blank `credentialId`.
- Rejects missing `ApiKeyCredential` when credentials are enforced.
- Rejects `batchSize < 1`.
- Accepts minimal valid config (`hecUrl` + `credentialId`).

No integration test against a real Splunk container — neither Elasticsearch nor Azure Monitor sinks have one, so we follow the existing precedent.

## Reference

The Python sender at `/Users/dan/PycharmProjects/pawn_log_generator/splunk_hec_sender.py` is the behavioral reference for the wire format: JSON-wrapped events with `event` / `sourcetype` / `index` keys, NDJSON-concatenated for batches, `Authorization: Splunk <token>`, optional SSL verification.
