# Azure Monitor Source Connector Design

**Date:** 2026-04-21
**Branch:** FLE-1519
**Status:** Approved

## Background

ZephFlow has a `sentinelsink` connector that writes events to Azure Monitor via the Logs Ingestion API. This spec covers the complementary read side: a `azuremonitorsource` connector that queries a Log Analytics workspace using KQL and emits results as ZephFlow events.

## Scope

Implement `azuremonitorsource` as a batch source connector. Also refactor `EntraIdTokenProvider` (currently in `sentinelsink`) to accept a `scope` parameter and move it to a shared `azure` package so both the sink and source can reuse it.

## Config Shape (`AzureMonitorSourceDto.Config`)

| Field | Type | Required | Notes |
|---|---|---|---|
| `workspaceId` | String | yes | Log Analytics workspace GUID |
| `tenantId` | String | yes | Entra ID tenant GUID |
| `kqlQuery` | String | yes | Full KQL query including any time filtering, e.g. `ZephflowTest2_CL \| where TimeGenerated > ago(1h)` |
| `credentialId` | String | yes | Key into `jobContext.otherProperties`; references a `UsernamePasswordCredential` where `username`=clientId and `password`=clientSecret |
| `batchSize` | Integer | no | Default 1000. Must be ≥ 1 if specified. Controls rows emitted per fetch cycle. |

## Refactor: `EntraIdTokenProvider`

**Current location:** `io.fleak.zephflow.lib.commands.sentinelsink`

**New location:** `io.fleak.zephflow.lib.commands.azure`

**Change:** Add `scope` as a constructor parameter (currently hardcoded to `https://monitor.azure.com/.default`).

**Updated constructor:** `EntraIdTokenProvider(String tenantId, String clientId, String clientSecret, String scope, HttpClient httpClient)`

**Sink update:** `SentinelSinkCommand` passes `"https://monitor.azure.com/.default"` explicitly.

**Source usage:** `AzureMonitorSourceCommand` passes `"https://api.loganalytics.io/.default"`.

No behavior change to the sink. Tests that construct `EntraIdTokenProvider` directly update the constructor call.

## New Class: `AzureMonitorQueryClient`

**Package:** `io.fleak.zephflow.lib.commands.azuremonitorsource`

**Constructor:** `AzureMonitorQueryClient(String workspaceId, EntraIdTokenProvider tokenProvider, HttpClient httpClient)`

**Public method:** `List<Map<String, String>> executeQuery(String kqlQuery) throws Exception`

**Endpoint:** `POST https://api.loganalytics.io/v1/workspaces/{workspaceId}/query`

**Request body:**
```json
{"query": "{kqlQuery}"}
```

**Auth header:** `Authorization: Bearer {token}` from `tokenProvider.getToken()`.

**Response parsing:** The API returns:
```json
{
  "tables": [{
    "columns": [{"name": "col1", "type": "string"}, ...],
    "rows": [["val1", ...], ...]
  }]
}
```
Zip `columns[i].name` with `rows[j][i]` to produce `Map<String, String>` per row (all values converted via `String.valueOf()`). Throws `RuntimeException` if `tables` array is missing or empty.

**401 retry:** Same pattern as `SentinelSinkFlusher` — invalidate token, re-fetch, retry once. If still 401, throw `RuntimeException("Azure Monitor auth error 401: token rejected after refresh")`.

**Other non-200:** Throw `RuntimeException` with status code + response body.

## New Class: `AzureMonitorSourceFetcher`

**Package:** `io.fleak.zephflow.lib.commands.azuremonitorsource`

**Implements:** `Fetcher<Map<String, String>>`

**Constructor:** `AzureMonitorSourceFetcher(String kqlQuery, int batchSize, AzureMonitorQueryClient queryClient)`

**State:** `List<Map<String, String>> allRows` (null until first fetch), `int offset = 0`

**`fetch()`:** On first call, invokes `queryClient.executeQuery(kqlQuery)` and stores result in `allRows`. Returns the next `batchSize` rows starting at `offset`, advances `offset`. Returns empty list when exhausted.

**`isExhausted()`:** Returns `allRows != null && offset >= allRows.size()`

**`commitStrategy()`:** Returns `NoCommitStrategy.INSTANCE`

**`close()`:** No-op.

## New Class: `AzureMonitorSourceCommand`

**Package:** `io.fleak.zephflow.lib.commands.azuremonitorsource`

**Extends:** `SimpleSourceCommand<Map<String, String>>`

**`sourceType()`:** `SourceType.BATCH`

**`commandName()`:** `"azuremonitorsource"`

**`createExecutionContext()`:** Reads `workspaceId`, `tenantId`, `credentialId`, `batchSize` from config. Constructs `EntraIdTokenProvider` with scope `"https://api.loganalytics.io/.default"`, then `AzureMonitorQueryClient`, then `AzureMonitorSourceFetcher`. Uses existing `MapRawDataEncoder` and `MapRawDataConverter`.

## New Class: `AzureMonitorSourceConfigValidator`

**Package:** `io.fleak.zephflow.lib.commands.azuremonitorsource`

**Validates:**
- `workspaceId` not blank
- `tenantId` not blank
- `kqlQuery` not blank
- `credentialId` not blank (+ credential lookup if `enforceCredentials` is true)
- `batchSize` ≥ 1 if specified

## New Class: `AzureMonitorSourceCommandFactory`

**Package:** `io.fleak.zephflow.lib.commands.azuremonitorsource`

Standard factory — constructs `JsonConfigParser<AzureMonitorSourceDto.Config>`, `AzureMonitorSourceConfigValidator`, and `AzureMonitorSourceCommand`. Returns `CommandType.SOURCE`.

## Modifications to Existing Files

| File | Change |
|---|---|
| `MiscUtils.java` | Add `COMMAND_NAME_AZURE_MONITOR_SOURCE = "azuremonitorsource"` |
| `OperatorCommandRegistry.java` | Add `.put(COMMAND_NAME_AZURE_MONITOR_SOURCE, new AzureMonitorSourceCommandFactory())` |
| `SentinelSinkCommand.java` | Update `EntraIdTokenProvider` constructor call to pass `"https://monitor.azure.com/.default"` |
| `EntraIdTokenProviderTest.java` | Update constructor calls to include scope argument |
| `SentinelSinkFlusherTest.java` | Update any `EntraIdTokenProvider` mock constructor calls |

## Data Flow

```
azuremonitorsource
       │
AzureMonitorSourceFetcher.fetch()
       │
AzureMonitorQueryClient.executeQuery(kqlQuery)
       │
POST https://api.loganalytics.io/v1/workspaces/{workspaceId}/query
Authorization: Bearer {token}
Content-Type: application/json
Body: {"query": "..."}
       │
Parse tables[0].columns + rows → List<Map<String,String>>
       │
Page by batchSize → RecordFleakData → downstream nodes
```

## Error Handling

| Condition | Behaviour |
|---|---|
| Token fetch fails | `RuntimeException` propagates — job fails at startup |
| HTTP 401 | Invalidate token, retry once; if still 401, `RuntimeException` |
| Other non-200 | `RuntimeException` with status + body |
| Empty result set | `isExhausted()` true immediately, zero events emitted |
| `tables` missing/empty in response | `RuntimeException` describing malformed response |

## Test Plan

**`AzureMonitorQueryClientTest`**
- Successful query: columns + rows parsed into correct `Map<String, String>` list
- 401 retry succeeds on second attempt
- 401 retry fails → `RuntimeException`
- Non-200 response → `RuntimeException` with status + body
- Empty `tables` array → `RuntimeException`

**`AzureMonitorSourceFetcherTest`**
- Single batch: all rows returned in one fetch, `isExhausted()` true after
- Multi-batch: rows split across multiple fetch calls correctly
- Empty result: `isExhausted()` true on first call, fetch returns empty list
- `fetch()` called after exhaustion returns empty list

**`AzureMonitorSourceConfigValidatorTest`**
- Valid config passes
- Each required field missing fails with descriptive message
- `batchSize=0` fails
- `batchSize=1` passes

## Azure Prerequisites for Testing

The App Registration (`5c45b9fd...`) needs **Log Analytics Reader** role on the `SentinelLogsSink` workspace (in addition to existing Monitoring Metrics Publisher on the DCR).

Example YAML:
```yaml
jobContext:
  otherProperties:
    sentinel_cred:
      username: "5c45b9fd-38c2-4431-917a-a880cdf7d34a"
      password: "<your-client-secret-here>"

dag:
  - id: "source"
    commandName: "azuremonitorsource"
    config:
      workspaceId: "c8bbf4ce-5902-4281-9241-91be9fb42ea4"
      tenantId: "71373c6f-eb1b-40bf-bcf4-19abdb2b9ca3"
      kqlQuery: "ZephflowTest2_CL | where TimeGenerated > ago(1h) | limit 10"
      credentialId: "sentinel_cred"
      batchSize: 100
    outputs:
      - "out"

  - id: "out"
    commandName: "stdout"
    config:
      encodingType: "JSON_OBJECT"
```
