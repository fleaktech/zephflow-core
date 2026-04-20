# Microsoft Sentinel Sink — Entra ID Migration Design

**Date:** 2026-04-20
**Branch:** FLE-1519
**Status:** Approved

## Background

The original implementation used the Azure HTTP Data Collector API with HMAC-SHA256 SharedKey authentication (workspace primary/secondary key). Azure has deprecated this in favour of the Logs Ingestion API, which authenticates via short-lived Entra ID OAuth tokens scoped to a service principal. Authorization is controlled by Azure RBAC (Monitoring Metrics Publisher role on a DCR), not by a shared secret.

## Scope

Migrate `sentinelsink` from the HTTP Data Collector API + SharedKey auth to the Logs Ingestion API + Entra ID client credentials flow. No other connectors are in scope.

## Config Shape (`SentinelSinkDto.Config`)

| Field | Type | Required | Notes |
|---|---|---|---|
| `tenantId` | String | yes | Entra ID tenant GUID |
| `dceEndpoint` | String | yes | Data Collection Endpoint URI (e.g. `https://my-dce.eastus-1.ingest.monitor.azure.com`) |
| `dcrImmutableId` | String | yes | DCR immutable ID, must start with `dcr-` |
| `streamName` | String | yes | Stream name declared in DCR (e.g. `Custom-ZephflowTest_CL`) |
| `credentialId` | String | yes | Key into `jobContext.otherProperties`; references a `UsernamePasswordCredential` where `username`=clientId and `password`=clientSecret |
| `timeGeneratedField` | String | no | Default `"TimeGenerated"`. Auto-injected into event payload if absent. |
| `batchSize` | Integer | no | Default 500. Must be ≥ 1 if specified. |

**Removed fields:** `workspaceId`, `logType` (table mapping is now owned by the DCR).

## New Class: `EntraIdTokenProvider`

Package: `io.fleak.zephflow.lib.commands.sentinelsink`

Owns token acquisition and caching. Keeps `SentinelSinkFlusher` focused on HTTP mechanics.

**Constructor:** `EntraIdTokenProvider(String tenantId, String clientId, String clientSecret)`

**State:** `volatile String cachedToken` (null = no cached token)

**Public methods:**
- `getToken()` — returns `cachedToken` if non-null; otherwise calls `fetchToken()`, stores result, returns it
- `invalidate()` — sets `cachedToken = null`

**Private method:**
- `fetchToken()` — POST to `https://login.microsoftonline.com/{tenantId}/oauth2/v2.0/token` with `application/x-www-form-urlencoded` body: `grant_type=client_credentials&client_id={clientId}&client_secret={clientSecret}&scope=https://monitor.azure.com//.default`. Parses JSON response for `access_token`. Throws `RuntimeException` on non-200.

## Updated Class: `SentinelSinkFlusher`

**Constructor:** `SentinelSinkFlusher(String dceEndpoint, String dcrImmutableId, String streamName, String timeGeneratedField, EntraIdTokenProvider tokenProvider)`

**Endpoint:** `{dceEndpoint}/dataCollectionRules/{dcrImmutableId}/streams/{streamName}?api-version=2023-01-01`

**Auth header:** `Authorization: Bearer {token}` — replaces all SharedKey/HMAC-SHA256 logic.

**401 retry flow:**
1. Call `tokenProvider.getToken()`, send request
2. If response is 401: call `tokenProvider.invalidate()`, call `tokenProvider.getToken()` to force re-fetch, retry once
3. If retry is still 401: return all events as `ErrorOutput` with message `"Sentinel auth error 401: token rejected after refresh"`
4. Any other non-200: return all events as `ErrorOutput` with status + body

**Removed:** `buildAuthorization()` and all HMAC-SHA256 logic.

## Updated Class: `SentinelSinkCommand`

Credential extraction changes: `credential.getUsername()` → `clientId`, `credential.getPassword()` → `clientSecret`. Constructs `EntraIdTokenProvider(tenantId, clientId, clientSecret)` and passes it to `SentinelSinkFlusher`.

## Updated Class: `SentinelSinkConfigValidator`

Replaces `workspaceId`/`logType` checks with:
- `tenantId` not blank
- `dceEndpoint` not blank
- `dcrImmutableId` not blank and starts with `dcr-`
- `streamName` not blank
- `credentialId` not blank (+ credential lookup if `enforceCredentials` is true, same as before)
- `batchSize` ≥ 1 if specified

## Data Flow

```
stdin → (eval transform) → sentinelsink
                               │
                    EntraIdTokenProvider
                    (getToken / invalidate)
                               │
                    POST {dceEndpoint}/dataCollectionRules/{dcrImmutableId}/streams/{streamName}
                    Authorization: Bearer {token}
                    Content-Type: application/json
                    Body: JSON array of events
```

## Error Handling

| Condition | Behaviour |
|---|---|
| Token fetch fails | `RuntimeException` propagates — job fails fast at startup or flush |
| HTTP 401 | Invalidate token, retry once; if still 401, all events → `ErrorOutput` |
| Other non-200 | All events → `ErrorOutput` with status + body |
| Network error | All events → `ErrorOutput` with exception message |

## Azure Setup Prerequisites (for testing)

1. Create an **App Registration** in Entra ID → note `tenantId`, `clientId`, generate a `clientSecret`
2. Create a **Data Collection Endpoint (DCE)** → note the Logs Ingestion URI
3. Create a **Data Collection Rule (DCR)** targeting a Log Analytics workspace table; note the immutable ID and stream name
4. Grant the app registration **Monitoring Metrics Publisher** role on the DCR
