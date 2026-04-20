# Sentinel Sink — Entra ID Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace SharedKey authentication in `sentinelsink` with Entra ID client-credentials OAuth, targeting the new Logs Ingestion API.

**Architecture:** A new `EntraIdTokenProvider` class owns token acquisition and caching. `SentinelSinkFlusher` is rewritten to call the new DCE/DCR endpoint with a Bearer token, with reactive 401 retry. Config fields `workspaceId`/`logType` are replaced by `tenantId`/`dceEndpoint`/`dcrImmutableId`/`streamName`.

**Tech Stack:** Java 11 `java.net.http.HttpClient`, Jackson for JSON, JUnit 5, Mockito 5.

---

## File Map

| Action | Path | Responsibility |
|---|---|---|
| Modify | `lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkDto.java` | Config shape — swap old fields for new |
| Create | `lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/EntraIdTokenProvider.java` | OAuth2 token fetch + cache + invalidate |
| Modify | `lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkFlusher.java` | Logs Ingestion API HTTP call, 401 retry |
| Modify | `lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkConfigValidator.java` | Validate new required fields |
| Modify | `lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkCommand.java` | Wire up EntraIdTokenProvider from credential |
| Modify | `verify-microsoft-sentinel.yml` | Update YAML to new field names |
| Create | `lib/src/test/java/io/fleak/zephflow/lib/commands/sentinelsink/EntraIdTokenProviderTest.java` | Unit tests for token provider |
| Create | `lib/src/test/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkConfigValidatorTest.java` | Unit tests for validator |
| Create | `lib/src/test/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkFlusherTest.java` | Unit tests for flusher incl. 401 retry |

---

## Task 1: Update Config DTO

**Files:**
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkDto.java`

- [ ] **Step 1: Replace the Config class body**

```java
/**
 * Copyright 2025 Fleak Tech Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fleak.zephflow.lib.commands.sentinelsink;

import io.fleak.zephflow.api.CommandConfig;
import lombok.*;

public interface SentinelSinkDto {

  int DEFAULT_BATCH_SIZE = 500;

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {
    /** Entra ID tenant GUID. */
    @NonNull private String tenantId;
    /** Data Collection Endpoint Logs Ingestion URI. */
    @NonNull private String dceEndpoint;
    /** DCR immutable ID — must start with "dcr-". */
    @NonNull private String dcrImmutableId;
    /** Stream name declared in the DCR (e.g. Custom-ZephflowTest_CL). */
    @NonNull private String streamName;
    /**
     * ID of a stored UsernamePasswordCredential: username = clientId, password = clientSecret.
     */
    @NonNull private String credentialId;
    /** Field name in the event to use as the timestamp. Defaults to "TimeGenerated". */
    @Builder.Default private String timeGeneratedField = "TimeGenerated";

    @Builder.Default private Integer batchSize = DEFAULT_BATCH_SIZE;
  }
}
```

- [ ] **Step 2: Verify compilation**

```bash
./gradlew :lib:compileJava
```

Expected: `BUILD SUCCESSFUL` (other files that reference old fields will fail — that is expected and will be fixed in subsequent tasks).

---

## Task 2: Create `EntraIdTokenProvider`

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/EntraIdTokenProvider.java`
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/sentinelsink/EntraIdTokenProviderTest.java`

- [ ] **Step 1: Write the failing tests first**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/sentinelsink/EntraIdTokenProviderTest.java`:

```java
/**
 * Copyright 2025 Fleak Tech Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fleak.zephflow.lib.commands.sentinelsink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class EntraIdTokenProviderTest {

  private HttpClient mockHttpClient;
  private HttpResponse<String> mockResponse;
  private EntraIdTokenProvider provider;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    mockHttpClient = mock(HttpClient.class);
    mockResponse = mock(HttpResponse.class);
    provider =
        new EntraIdTokenProvider("tenant-id", "client-id", "client-secret", mockHttpClient);
  }

  @Test
  @SuppressWarnings("unchecked")
  void getToken_fetchesAndCachesToken() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body())
        .thenReturn("{\"access_token\":\"my-token\",\"expires_in\":3600}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    String token = provider.getToken();

    assertEquals("my-token", token);
    verify(mockHttpClient, times(1)).send(any(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  void getToken_returnsCachedTokenOnSecondCall() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body())
        .thenReturn("{\"access_token\":\"my-token\",\"expires_in\":3600}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.getToken();
    provider.getToken();

    verify(mockHttpClient, times(1)).send(any(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  void invalidate_clearsCache_forcesNewFetch() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body())
        .thenReturn("{\"access_token\":\"my-token\",\"expires_in\":3600}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.getToken();
    provider.invalidate();
    provider.getToken();

    verify(mockHttpClient, times(2)).send(any(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  void getToken_throwsOnNon200() throws Exception {
    when(mockResponse.statusCode()).thenReturn(401);
    when(mockResponse.body()).thenReturn("{\"error\":\"invalid_client\"}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    RuntimeException ex = assertThrows(RuntimeException.class, () -> provider.getToken());
    assertTrue(ex.getMessage().contains("401"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void getToken_throwsWhenAccessTokenMissingFromResponse() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("{\"token_type\":\"Bearer\"}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    RuntimeException ex = assertThrows(RuntimeException.class, () -> provider.getToken());
    assertTrue(ex.getMessage().contains("access_token"));
  }
}
```

- [ ] **Step 2: Run tests to confirm they fail (class not found)**

```bash
./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.sentinelsink.EntraIdTokenProviderTest"
```

Expected: compilation error — `EntraIdTokenProvider` does not exist yet.

- [ ] **Step 3: Create `EntraIdTokenProvider`**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/EntraIdTokenProvider.java`:

```java
/**
 * Copyright 2025 Fleak Tech Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fleak.zephflow.lib.commands.sentinelsink;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;

/** Acquires and caches an Entra ID access token via the client-credentials OAuth2 flow. */
@Slf4j
public class EntraIdTokenProvider {

  private static final String TOKEN_ENDPOINT_TEMPLATE =
      "https://login.microsoftonline.com/%s/oauth2/v2.0/token";
  private static final String SCOPE = "https://monitor.azure.com//.default";

  private final String tenantId;
  private final String clientId;
  private final String clientSecret;
  private final HttpClient httpClient;

  private volatile String cachedToken;

  public EntraIdTokenProvider(
      String tenantId, String clientId, String clientSecret, HttpClient httpClient) {
    this.tenantId = tenantId;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.httpClient = httpClient;
  }

  public String getToken() throws Exception {
    if (cachedToken != null) {
      return cachedToken;
    }
    cachedToken = fetchToken();
    return cachedToken;
  }

  public void invalidate() {
    cachedToken = null;
  }

  private String fetchToken() throws Exception {
    String body =
        "grant_type=client_credentials"
            + "&client_id="
            + URLEncoder.encode(clientId, StandardCharsets.UTF_8)
            + "&client_secret="
            + URLEncoder.encode(clientSecret, StandardCharsets.UTF_8)
            + "&scope="
            + URLEncoder.encode(SCOPE, StandardCharsets.UTF_8);

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(String.format(TOKEN_ENDPOINT_TEMPLATE, tenantId)))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .timeout(Duration.ofSeconds(30))
            .build();

    HttpResponse<String> response =
        httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new RuntimeException(
          "Failed to obtain Entra ID token: HTTP " + response.statusCode() + " " + response.body());
    }

    JsonNode json = OBJECT_MAPPER.readTree(response.body());
    String token = json.path("access_token").asText(null);
    if (token == null || token.isEmpty()) {
      throw new RuntimeException(
          "Entra ID token response missing access_token: " + response.body());
    }
    return token;
  }
}
```

- [ ] **Step 4: Run the tests and confirm they pass**

```bash
./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.sentinelsink.EntraIdTokenProviderTest"
```

Expected: `BUILD SUCCESSFUL`, 5 tests passed.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/EntraIdTokenProvider.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/sentinelsink/EntraIdTokenProviderTest.java \
        lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkDto.java
git commit -m "FLE-1519: Add EntraIdTokenProvider and update SentinelSinkDto for Entra ID migration"
```

---

## Task 3: Rewrite `SentinelSinkFlusher`

**Files:**
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkFlusher.java`
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkFlusherTest.java`

- [ ] **Step 1: Write the failing tests**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkFlusherTest.java`:

```java
/**
 * Copyright 2025 Fleak Tech Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fleak.zephflow.lib.commands.sentinelsink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SentinelSinkFlusherTest {

  private HttpClient mockHttpClient;
  private HttpResponse<String> mockResponse;
  private EntraIdTokenProvider mockTokenProvider;
  private SentinelSinkFlusher flusher;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    mockHttpClient = mock(HttpClient.class);
    mockResponse = mock(HttpResponse.class);
    mockTokenProvider = mock(EntraIdTokenProvider.class);
    flusher =
        new SentinelSinkFlusher(
            "https://my-dce.eastus.ingest.monitor.azure.com",
            "dcr-abc123",
            "Custom-ZephflowTest_CL",
            "TimeGenerated",
            mockTokenProvider,
            mockHttpClient);
  }

  @Test
  void flush_emptyBatch_returnsZeroWithNoHttpCall() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<SentinelOutboundEvent> events =
        new SimpleSinkCommand.PreparedInputEvents<>();

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(0, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    verifyNoInteractions(mockHttpClient);
  }

  @Test
  @SuppressWarnings("unchecked")
  void flush_204Response_countsSuccess() throws Exception {
    when(mockTokenProvider.getToken()).thenReturn("test-token");
    when(mockResponse.statusCode()).thenReturn(204);
    when(mockResponse.body()).thenReturn("");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    SimpleSinkCommand.PreparedInputEvents<SentinelOutboundEvent> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    RecordFleakData record =
        new RecordFleakData(Map.of("msg", new StringPrimitiveFleakData("hello")));
    events.add(record, new SentinelOutboundEvent("{\"msg\":\"hello\"}"));

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(1, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    verify(mockHttpClient, times(1)).send(any(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  void flush_401_invalidatesTokenAndRetries_thenSucceeds() throws Exception {
    HttpResponse<String> unauthorizedResponse = mock(HttpResponse.class);
    when(unauthorizedResponse.statusCode()).thenReturn(401);
    when(unauthorizedResponse.body()).thenReturn("Unauthorized");

    when(mockTokenProvider.getToken()).thenReturn("stale-token").thenReturn("fresh-token");
    when(mockResponse.statusCode()).thenReturn(204);
    when(mockResponse.body()).thenReturn("");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(unauthorizedResponse)
        .thenReturn(mockResponse);

    SimpleSinkCommand.PreparedInputEvents<SentinelOutboundEvent> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    RecordFleakData record =
        new RecordFleakData(Map.of("msg", new StringPrimitiveFleakData("hello")));
    events.add(record, new SentinelOutboundEvent("{\"msg\":\"hello\"}"));

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(1, result.successCount());
    verify(mockTokenProvider).invalidate();
    verify(mockHttpClient, times(2)).send(any(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  void flush_401AfterRetry_returnsErrorPerEvent() throws Exception {
    when(mockTokenProvider.getToken()).thenReturn("stale-token").thenReturn("fresh-token");
    when(mockResponse.statusCode()).thenReturn(401);
    when(mockResponse.body()).thenReturn("Unauthorized");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    SimpleSinkCommand.PreparedInputEvents<SentinelOutboundEvent> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    RecordFleakData record =
        new RecordFleakData(Map.of("msg", new StringPrimitiveFleakData("hello")));
    events.add(record, new SentinelOutboundEvent("{\"msg\":\"hello\"}"));

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(0, result.successCount());
    assertEquals(1, result.errorOutputList().size());
    assertTrue(result.errorOutputList().get(0).errorMessage().contains("401"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void flush_503Response_returnsErrorPerEvent() throws Exception {
    when(mockTokenProvider.getToken()).thenReturn("test-token");
    when(mockResponse.statusCode()).thenReturn(503);
    when(mockResponse.body()).thenReturn("Service Unavailable");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    SimpleSinkCommand.PreparedInputEvents<SentinelOutboundEvent> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    RecordFleakData record =
        new RecordFleakData(Map.of("msg", new StringPrimitiveFleakData("hello")));
    events.add(record, new SentinelOutboundEvent("{\"msg\":\"hello\"}"));

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(0, result.successCount());
    assertEquals(1, result.errorOutputList().size());
    assertTrue(result.errorOutputList().get(0).errorMessage().contains("503"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void flush_networkException_returnsErrorPerEvent() throws Exception {
    when(mockTokenProvider.getToken()).thenReturn("test-token");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new RuntimeException("Connection refused"));

    SimpleSinkCommand.PreparedInputEvents<SentinelOutboundEvent> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    RecordFleakData record =
        new RecordFleakData(Map.of("msg", new StringPrimitiveFleakData("hello")));
    events.add(record, new SentinelOutboundEvent("{\"msg\":\"hello\"}"));

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(0, result.successCount());
    assertEquals(1, result.errorOutputList().size());
    assertTrue(result.errorOutputList().get(0).errorMessage().contains("Connection refused"));
  }
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.sentinelsink.SentinelSinkFlusherTest"
```

Expected: compilation errors — flusher constructor signature doesn't match yet.

- [ ] **Step 3: Rewrite `SentinelSinkFlusher`**

Replace the entire content of `lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkFlusher.java`:

```java
/**
 * Copyright 2025 Fleak Tech Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fleak.zephflow.lib.commands.sentinelsink;

import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

/**
 * Flushes events to Microsoft Sentinel via the Azure Monitor Logs Ingestion API.
 *
 * <p>Reference: https://learn.microsoft.com/en-us/azure/azure-monitor/logs/logs-ingestion-api-overview
 */
@Slf4j
public class SentinelSinkFlusher implements SimpleSinkCommand.Flusher<SentinelOutboundEvent> {

  private static final String API_VERSION = "2023-01-01";

  private final String dceEndpoint;
  private final String dcrImmutableId;
  private final String streamName;
  private final String timeGeneratedField;
  private final EntraIdTokenProvider tokenProvider;
  private final HttpClient httpClient;

  public SentinelSinkFlusher(
      String dceEndpoint,
      String dcrImmutableId,
      String streamName,
      String timeGeneratedField,
      EntraIdTokenProvider tokenProvider,
      HttpClient httpClient) {
    this.dceEndpoint = dceEndpoint;
    this.dcrImmutableId = dcrImmutableId;
    this.streamName = streamName;
    this.timeGeneratedField = timeGeneratedField;
    this.tokenProvider = tokenProvider;
    this.httpClient = httpClient;
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<SentinelOutboundEvent> preparedInputEvents,
      Map<String, String> metricTags)
      throws Exception {
    List<SentinelOutboundEvent> events = preparedInputEvents.preparedList();
    if (events.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    String jsonBody =
        "["
            + events.stream()
                .map(SentinelOutboundEvent::jsonPayload)
                .collect(Collectors.joining(","))
            + "]";
    byte[] bodyBytes = jsonBody.getBytes(StandardCharsets.UTF_8);

    try {
      String token = tokenProvider.getToken();
      HttpResponse<String> response = sendRequest(token, bodyBytes);

      if (response.statusCode() == 401) {
        tokenProvider.invalidate();
        token = tokenProvider.getToken();
        response = sendRequest(token, bodyBytes);
      }

      if (response.statusCode() == 204) {
        log.debug("Successfully sent {} events to Microsoft Sentinel", events.size());
        return new SimpleSinkCommand.FlushResult(events.size(), bodyBytes.length, List.of());
      } else {
        final int statusCode = response.statusCode();
        final String responseBody = response.body();
        log.error("Sentinel API returned status {}: {}", statusCode, responseBody);
        List<ErrorOutput> errors =
            preparedInputEvents.rawAndPreparedList().stream()
                .map(
                    p ->
                        new ErrorOutput(
                            p.getLeft(), "Sentinel API error " + statusCode + ": " + responseBody))
                .collect(Collectors.toList());
        return new SimpleSinkCommand.FlushResult(0, 0, errors);
      }
    } catch (Exception e) {
      log.error("Failed to send events to Microsoft Sentinel", e);
      List<ErrorOutput> errors =
          preparedInputEvents.rawAndPreparedList().stream()
              .map(
                  p ->
                      new ErrorOutput(
                          p.getLeft(), "Sentinel connection error: " + e.getMessage()))
              .collect(Collectors.toList());
      return new SimpleSinkCommand.FlushResult(0, 0, errors);
    }
  }

  private HttpResponse<String> sendRequest(String token, byte[] bodyBytes) throws Exception {
    String endpoint =
        dceEndpoint
            + "/dataCollectionRules/"
            + dcrImmutableId
            + "/streams/"
            + streamName
            + "?api-version="
            + API_VERSION;

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(endpoint))
            .header("Authorization", "Bearer " + token)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes))
            .timeout(java.time.Duration.ofSeconds(60))
            .build();

    return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
  }

  @Override
  public void close() {
    // HttpClient does not require explicit closing
  }
}
```

- [ ] **Step 4: Run the tests and confirm they pass**

```bash
./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.sentinelsink.SentinelSinkFlusherTest"
```

Expected: `BUILD SUCCESSFUL`, 6 tests passed.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkFlusher.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkFlusherTest.java
git commit -m "FLE-1519: Rewrite SentinelSinkFlusher for Logs Ingestion API with Entra ID Bearer auth"
```

---

## Task 4: Update `SentinelSinkConfigValidator`

**Files:**
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkConfigValidator.java`
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkConfigValidatorTest.java`

- [ ] **Step 1: Write the failing tests**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkConfigValidatorTest.java`:

```java
/**
 * Copyright 2025 Fleak Tech Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fleak.zephflow.lib.commands.sentinelsink;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.TestUtils;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SentinelSinkConfigValidatorTest {

  static final JobContext TEST_JOB_CONTEXT =
      JobContext.builder()
          .metricTags(TestUtils.JOB_CONTEXT.getMetricTags())
          .otherProperties(
              new HashMap<>(
                  Map.of(
                      "sentinel_cred",
                      new HashMap<>(
                          Map.of("username", "client-id", "password", "client-secret")))))
          .build();

  private final SentinelSinkConfigValidator validator = new SentinelSinkConfigValidator();

  private SentinelSinkDto.Config validConfig() {
    return SentinelSinkDto.Config.builder()
        .tenantId("tenant-id")
        .dceEndpoint("https://my-dce.eastus.ingest.monitor.azure.com")
        .dcrImmutableId("dcr-abc123")
        .streamName("Custom-ZephflowTest_CL")
        .credentialId("sentinel_cred")
        .build();
  }

  @Test
  void validateConfig_valid() {
    assertDoesNotThrow(() -> validator.validateConfig(validConfig(), "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_missingTenantId() {
    SentinelSinkDto.Config c = validConfig();
    c.setTenantId("");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_missingDceEndpoint() {
    SentinelSinkDto.Config c = validConfig();
    c.setDceEndpoint("");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_missingDcrImmutableId() {
    SentinelSinkDto.Config c = validConfig();
    c.setDcrImmutableId("");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_dcrImmutableIdWrongPrefix() {
    SentinelSinkDto.Config c = validConfig();
    c.setDcrImmutableId("abc-123456");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_missingStreamName() {
    SentinelSinkDto.Config c = validConfig();
    c.setStreamName("");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_missingCredentialId() {
    SentinelSinkDto.Config c = validConfig();
    c.setCredentialId("");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_batchSizeZero() {
    SentinelSinkDto.Config c = validConfig();
    c.setBatchSize(0);
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_batchSizeOne_isValid() {
    SentinelSinkDto.Config c = validConfig();
    c.setBatchSize(1);
    assertDoesNotThrow(() -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }
}
```

- [ ] **Step 2: Run tests to confirm they fail**

```bash
./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.sentinelsink.SentinelSinkConfigValidatorTest"
```

Expected: failures — validator still checks old fields.

- [ ] **Step 3: Rewrite `SentinelSinkConfigValidator`**

Replace the entire content of `lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkConfigValidator.java`:

```java
/**
 * Copyright 2025 Fleak Tech Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fleak.zephflow.lib.commands.sentinelsink;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import org.apache.commons.lang3.StringUtils;

public class SentinelSinkConfigValidator implements ConfigValidator {

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    SentinelSinkDto.Config config = (SentinelSinkDto.Config) commandConfig;

    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getTenantId()), "tenantId is required");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getDceEndpoint()), "dceEndpoint is required");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getDcrImmutableId()), "dcrImmutableId is required");
    Preconditions.checkArgument(
        config.getDcrImmutableId().startsWith("dcr-"),
        "dcrImmutableId must start with 'dcr-'");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getStreamName()), "streamName is required");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getCredentialId()), "credentialId is required");

    if (config.getBatchSize() != null) {
      Preconditions.checkArgument(config.getBatchSize() >= 1, "batchSize must be at least 1");
    }

    if (enforceCredentials(jobContext)) {
      lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
    }
  }
}
```

- [ ] **Step 4: Run the tests and confirm they pass**

```bash
./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.sentinelsink.SentinelSinkConfigValidatorTest"
```

Expected: `BUILD SUCCESSFUL`, 9 tests passed.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkConfigValidator.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkConfigValidatorTest.java
git commit -m "FLE-1519: Update SentinelSinkConfigValidator for new Entra ID config fields"
```

---

## Task 5: Update `SentinelSinkCommand`

**Files:**
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkCommand.java`

- [ ] **Step 1: Rewrite `SentinelSinkCommand`**

Replace the entire content of `lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkCommand.java`:

```java
/**
 * Copyright 2025 Fleak Tech Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fleak.zephflow.lib.commands.sentinelsink;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import java.net.http.HttpClient;
import java.time.Duration;

public class SentinelSinkCommand extends SimpleSinkCommand<SentinelOutboundEvent> {

  protected SentinelSinkCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator) {
    super(nodeId, jobContext, configParser, configValidator);
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_SENTINEL_SINK;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    SinkCounters counters =
        createSinkCounters(metricClientProvider, jobContext, commandName(), nodeId);

    SentinelSinkDto.Config config = (SentinelSinkDto.Config) commandConfig;

    String clientId =
        lookupUsernamePasswordCredentialOpt(jobContext, config.getCredentialId())
            .map(UsernamePasswordCredential::getUsername)
            .orElse("");
    String clientSecret =
        lookupUsernamePasswordCredentialOpt(jobContext, config.getCredentialId())
            .map(UsernamePasswordCredential::getPassword)
            .orElse("");

    HttpClient httpClient =
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
    EntraIdTokenProvider tokenProvider =
        new EntraIdTokenProvider(config.getTenantId(), clientId, clientSecret, httpClient);

    SimpleSinkCommand.Flusher<SentinelOutboundEvent> flusher =
        new SentinelSinkFlusher(
            config.getDceEndpoint(),
            config.getDcrImmutableId(),
            config.getStreamName(),
            config.getTimeGeneratedField(),
            tokenProvider,
            httpClient);

    SimpleSinkCommand.SinkMessagePreProcessor<SentinelOutboundEvent> messagePreProcessor =
        new SentinelSinkMessageProcessor(config.getTimeGeneratedField());

    return new SinkExecutionContext<>(
        flusher,
        messagePreProcessor,
        counters.inputMessageCounter(),
        counters.errorCounter(),
        counters.sinkOutputCounter(),
        counters.outputSizeCounter(),
        counters.sinkErrorCounter());
  }

  @Override
  protected int batchSize() {
    SentinelSinkDto.Config config = (SentinelSinkDto.Config) commandConfig;
    return config.getBatchSize() != null
        ? config.getBatchSize()
        : SentinelSinkDto.DEFAULT_BATCH_SIZE;
  }
}
```

- [ ] **Step 2: Run the full module test suite to confirm nothing is broken**

```bash
./gradlew :lib:test
```

Expected: `BUILD SUCCESSFUL`, all tests pass (the three new test classes plus all pre-existing tests).

- [ ] **Step 3: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkCommand.java
git commit -m "FLE-1519: Wire EntraIdTokenProvider into SentinelSinkCommand"
```

---

## Task 6: Update `verify-microsoft-sentinel.yml`

**Files:**
- Modify: `verify-microsoft-sentinel.yml`

- [ ] **Step 1: Replace the YAML with the updated field names**

Replace the full content of `verify-microsoft-sentinel.yml`:

```yaml
jobContext:
  otherProperties:
    # Credential entry for the Sentinel service principal.
    # username = Application (client) ID from your App Registration
    # password = Client secret VALUE (shown once at creation time)
    sentinel_cred:
      username: "YOUR_CLIENT_ID"
      password: "YOUR_CLIENT_SECRET"

  metricTags:
  dlqConfig:

dag:
  # Source: read JSON events from stdin, one JSON object per line.
  - id: "source"
    commandName: "stdin"
    config:
      encodingType: "JSON_OBJECT"
    outputs:
      - "sentinel"

  # Sink: send events to Microsoft Sentinel via the Logs Ingestion API.
  # The DCR maps the stream to a table — e.g. streamName "Custom-ZephflowTest_CL"
  # lands in table "ZephflowTest_CL" in your Log Analytics workspace.
  - id: "sentinel"
    commandName: "sentinelsink"
    config:
      tenantId: "YOUR_TENANT_ID"
      dceEndpoint: "https://YOUR-DCE-NAME.eastus-1.ingest.monitor.azure.com"
      dcrImmutableId: "dcr-YOUR_DCR_IMMUTABLE_ID"
      streamName: "Custom-ZephflowTest_CL"
      credentialId: "sentinel_cred"
      timeGeneratedField: "TimeGenerated"
      batchSize: 10
```

- [ ] **Step 2: Commit**

```bash
git add verify-microsoft-sentinel.yml
git commit -m "FLE-1519: Update verify-microsoft-sentinel.yml for Entra ID Logs Ingestion API"
```

---

## Task 7: Final Build Verification

- [ ] **Step 1: Build the CLI starter fat jar**

```bash
./gradlew :clistarter:shadowJar
```

Expected: `BUILD SUCCESSFUL`.

- [ ] **Step 2: Confirm the jar is present**

```bash
ls clistarter/build/libs/clistarter-*-all.jar
```

Expected: one file listed.

- [ ] **Step 3: Commit**

No new files — the jar is in `build/` which is gitignored.
