# Azure Monitor Source Connector Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `azuremonitorsource`, a batch source connector that queries a Log Analytics workspace using KQL and emits results as ZephFlow events, while refactoring `EntraIdTokenProvider` to a shared `azure` package so both the sink and source can reuse it.

**Architecture:** `EntraIdTokenProvider` moves to an `azure` package and gains a `scope` constructor parameter. A new `AzureMonitorQueryClient` calls the Log Analytics REST API, `AzureMonitorSourceFetcher` pages results in batches, and `AzureMonitorSourceCommand` wires everything together following the same `SimpleSourceCommand` / `Fetcher` pattern already used by `SplunkSourceCommand`.

**Tech Stack:** Java 17, Jackson, `java.net.http.HttpClient`, Mockito/JUnit 5, Lombok, Guava Preconditions.

---

## File Structure

**New files:**
- `lib/src/main/java/io/fleak/zephflow/lib/commands/azure/EntraIdTokenProvider.java` — moved + `scope` param added
- `lib/src/main/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceDto.java` — config DTO
- `lib/src/main/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceConfigValidator.java` — config validation
- `lib/src/main/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorQueryClient.java` — HTTP client for Log Analytics
- `lib/src/main/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceFetcher.java` — paging fetcher
- `lib/src/main/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceCommand.java` — command wiring
- `lib/src/main/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceCommandFactory.java` — factory
- `lib/src/test/java/io/fleak/zephflow/lib/commands/azure/EntraIdTokenProviderTest.java` — moved test, updated constructor
- `lib/src/test/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorQueryClientTest.java`
- `lib/src/test/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceFetcherTest.java`
- `lib/src/test/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceConfigValidatorTest.java`

**Modified files:**
- `lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkCommand.java` — update import + add scope arg
- `lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkFlusher.java` — update import only
- `lib/src/test/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkFlusherTest.java` — update import only
- `lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java` — add `COMMAND_NAME_AZURE_MONITOR_SOURCE`
- `lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java` — register factory

**Deleted files:**
- `lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/EntraIdTokenProvider.java`
- `lib/src/test/java/io/fleak/zephflow/lib/commands/sentinelsink/EntraIdTokenProviderTest.java`

---

### Task 1: Refactor EntraIdTokenProvider to shared azure package

Move `EntraIdTokenProvider` from `sentinelsink` to `azure` package and add `scope` as a constructor parameter so both the sink and the upcoming source can reuse it with different OAuth scopes.

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/azure/EntraIdTokenProvider.java`
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/azure/EntraIdTokenProviderTest.java`
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkCommand.java`
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkFlusher.java`
- Modify: `lib/src/test/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkFlusherTest.java`
- Delete: `lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/EntraIdTokenProvider.java`
- Delete: `lib/src/test/java/io/fleak/zephflow/lib/commands/sentinelsink/EntraIdTokenProviderTest.java`

- [ ] **Step 1: Create new EntraIdTokenProvider in azure package**

```java
// lib/src/main/java/io/fleak/zephflow/lib/commands/azure/EntraIdTokenProvider.java
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
package io.fleak.zephflow.lib.commands.azure;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class EntraIdTokenProvider {

  private static final String TOKEN_ENDPOINT_TEMPLATE =
      "https://login.microsoftonline.com/%s/oauth2/v2.0/token";

  private final String tenantId;
  private final String clientId;
  private final String clientSecret;
  private final String scope;
  private final HttpClient httpClient;

  private String cachedToken;

  public EntraIdTokenProvider(
      String tenantId, String clientId, String clientSecret, String scope, HttpClient httpClient) {
    this.tenantId = tenantId;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.scope = scope;
    this.httpClient = httpClient;
  }

  public synchronized String getToken() throws Exception {
    if (cachedToken != null) {
      return cachedToken;
    }
    cachedToken = fetchToken();
    return cachedToken;
  }

  public synchronized void invalidate() {
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
            + URLEncoder.encode(scope, StandardCharsets.UTF_8);

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(
                URI.create(
                    String.format(
                        TOKEN_ENDPOINT_TEMPLATE,
                        URLEncoder.encode(tenantId, StandardCharsets.UTF_8))))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .timeout(Duration.ofSeconds(30))
            .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

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

- [ ] **Step 2: Create moved test in azure package**

```java
// lib/src/test/java/io/fleak/zephflow/lib/commands/azure/EntraIdTokenProviderTest.java
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
package io.fleak.zephflow.lib.commands.azure;

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
        new EntraIdTokenProvider(
            "tenant-id",
            "client-id",
            "client-secret",
            "https://monitor.azure.com/.default",
            mockHttpClient);
  }

  @Test
  void getToken_fetchesAndCachesToken() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("{\"access_token\":\"my-token\",\"expires_in\":3600}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    String token = provider.getToken();

    assertEquals("my-token", token);
    verify(mockHttpClient, times(1)).send(any(), any());
  }

  @Test
  void getToken_returnsCachedTokenOnSecondCall() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("{\"access_token\":\"my-token\",\"expires_in\":3600}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.getToken();
    provider.getToken();

    verify(mockHttpClient, times(1)).send(any(), any());
  }

  @Test
  void invalidate_clearsCache_forcesNewFetch() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("{\"access_token\":\"my-token\",\"expires_in\":3600}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    provider.getToken();
    provider.invalidate();
    provider.getToken();

    verify(mockHttpClient, times(2)).send(any(), any());
  }

  @Test
  void getToken_throwsOnNon200() throws Exception {
    when(mockResponse.statusCode()).thenReturn(401);
    when(mockResponse.body()).thenReturn("{\"error\":\"invalid_client\"}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    RuntimeException ex = assertThrows(RuntimeException.class, () -> provider.getToken());
    assertTrue(ex.getMessage().contains("401"));
  }

  @Test
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

- [ ] **Step 3: Run the new test to verify it passes**

```
./mvnw test -pl lib -Dtest=io.fleak.zephflow.lib.commands.azure.EntraIdTokenProviderTest -am
```

Expected: BUILD SUCCESS, 5 tests passing.

- [ ] **Step 4: Update SentinelSinkFlusher — change import only**

In `lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkFlusher.java`, change the field declaration from the `sentinelsink` class to the `azure` class.

The field `private final EntraIdTokenProvider tokenProvider;` and constructor parameter type are unqualified — only the import needs to change. Replace:

```java
// (no explicit import — EntraIdTokenProvider was in same package)
```

Add at the top of the import block:
```java
import io.fleak.zephflow.lib.commands.azure.EntraIdTokenProvider;
```

The file has no existing import for `EntraIdTokenProvider` because it was in the same package. After moving, add the explicit import. The full updated imports section (add this import alongside the existing ones):

```java
import io.fleak.zephflow.lib.commands.azure.EntraIdTokenProvider;
import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
```

- [ ] **Step 5: Update SentinelSinkCommand — change import and add scope argument**

In `lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkCommand.java`:

Add import (same-package reference → now needs explicit import):
```java
import io.fleak.zephflow.lib.commands.azure.EntraIdTokenProvider;
```

Update the constructor call on line ~63 from:
```java
    EntraIdTokenProvider tokenProvider =
        new EntraIdTokenProvider(config.getTenantId(), clientId, clientSecret, httpClient);
```
to:
```java
    EntraIdTokenProvider tokenProvider =
        new EntraIdTokenProvider(
            config.getTenantId(),
            clientId,
            clientSecret,
            "https://monitor.azure.com/.default",
            httpClient);
```

- [ ] **Step 6: Update SentinelSinkFlusherTest — change import only**

In `lib/src/test/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkFlusherTest.java`, `EntraIdTokenProvider` was in same package so no import existed. Add:
```java
import io.fleak.zephflow.lib.commands.azure.EntraIdTokenProvider;
```

- [ ] **Step 7: Run all sentinel sink tests to verify nothing broke**

```
./mvnw test -pl lib -Dtest="io.fleak.zephflow.lib.commands.sentinelsink.*" -am
```

Expected: BUILD SUCCESS.

- [ ] **Step 8: Delete old EntraIdTokenProvider files**

```bash
git rm lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/EntraIdTokenProvider.java
git rm lib/src/test/java/io/fleak/zephflow/lib/commands/sentinelsink/EntraIdTokenProviderTest.java
```

- [ ] **Step 9: Run full lib test suite to confirm clean**

```
./mvnw test -pl lib -am
```

Expected: BUILD SUCCESS.

- [ ] **Step 10: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/azure/
git add lib/src/test/java/io/fleak/zephflow/lib/commands/azure/
git add lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkCommand.java
git add lib/src/main/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkFlusher.java
git add lib/src/test/java/io/fleak/zephflow/lib/commands/sentinelsink/SentinelSinkFlusherTest.java
git commit -m "refactor: move EntraIdTokenProvider to azure package with scope parameter"
```

---

### Task 2: AzureMonitorSourceDto and AzureMonitorSourceConfigValidator

Define the config DTO and the validator that checks required fields and batchSize bounds.

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceDto.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceConfigValidator.java`
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceConfigValidatorTest.java`

- [ ] **Step 1: Write the failing validator tests**

```java
// lib/src/test/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceConfigValidatorTest.java
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
package io.fleak.zephflow.lib.commands.azuremonitorsource;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.TestUtils;
import org.junit.jupiter.api.Test;

class AzureMonitorSourceConfigValidatorTest {

  private final AzureMonitorSourceConfigValidator validator =
      new AzureMonitorSourceConfigValidator();

  @Test
  void validConfig_passes() {
    var config =
        AzureMonitorSourceDto.Config.builder()
            .workspaceId("ws-guid")
            .tenantId("tenant-guid")
            .kqlQuery("TableName_CL | limit 10")
            .credentialId("cred-id")
            .build();

    assertDoesNotThrow(
        () -> validator.validateConfig(config, "node", TestUtils.JOB_CONTEXT));
  }

  @Test
  void validConfig_withExplicitBatchSize_passes() {
    var config =
        AzureMonitorSourceDto.Config.builder()
            .workspaceId("ws-guid")
            .tenantId("tenant-guid")
            .kqlQuery("TableName_CL | limit 10")
            .credentialId("cred-id")
            .batchSize(1)
            .build();

    assertDoesNotThrow(
        () -> validator.validateConfig(config, "node", TestUtils.JOB_CONTEXT));
  }

  @Test
  void missingWorkspaceId_fails() {
    var config =
        AzureMonitorSourceDto.Config.builder()
            .tenantId("tenant-guid")
            .kqlQuery("TableName_CL | limit 10")
            .credentialId("cred-id")
            .build();

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "node", TestUtils.JOB_CONTEXT));
    assertTrue(ex.getMessage().contains("workspaceId"));
  }

  @Test
  void missingTenantId_fails() {
    var config =
        AzureMonitorSourceDto.Config.builder()
            .workspaceId("ws-guid")
            .kqlQuery("TableName_CL | limit 10")
            .credentialId("cred-id")
            .build();

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "node", TestUtils.JOB_CONTEXT));
    assertTrue(ex.getMessage().contains("tenantId"));
  }

  @Test
  void missingKqlQuery_fails() {
    var config =
        AzureMonitorSourceDto.Config.builder()
            .workspaceId("ws-guid")
            .tenantId("tenant-guid")
            .credentialId("cred-id")
            .build();

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "node", TestUtils.JOB_CONTEXT));
    assertTrue(ex.getMessage().contains("kqlQuery"));
  }

  @Test
  void missingCredentialId_fails() {
    var config =
        AzureMonitorSourceDto.Config.builder()
            .workspaceId("ws-guid")
            .tenantId("tenant-guid")
            .kqlQuery("TableName_CL | limit 10")
            .build();

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "node", TestUtils.JOB_CONTEXT));
    assertTrue(ex.getMessage().contains("credentialId"));
  }

  @Test
  void batchSizeZero_fails() {
    var config =
        AzureMonitorSourceDto.Config.builder()
            .workspaceId("ws-guid")
            .tenantId("tenant-guid")
            .kqlQuery("TableName_CL | limit 10")
            .credentialId("cred-id")
            .batchSize(0)
            .build();

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> validator.validateConfig(config, "node", TestUtils.JOB_CONTEXT));
    assertTrue(ex.getMessage().contains("batchSize"));
  }

  @Test
  void batchSizeOne_passes() {
    var config =
        AzureMonitorSourceDto.Config.builder()
            .workspaceId("ws-guid")
            .tenantId("tenant-guid")
            .kqlQuery("TableName_CL | limit 10")
            .credentialId("cred-id")
            .batchSize(1)
            .build();

    assertDoesNotThrow(
        () -> validator.validateConfig(config, "node", TestUtils.JOB_CONTEXT));
  }
}
```

- [ ] **Step 2: Run test — expect compile failure because classes don't exist yet**

```
./mvnw test -pl lib -Dtest=AzureMonitorSourceConfigValidatorTest -am 2>&1 | head -30
```

Expected: compilation error — `AzureMonitorSourceDto` and `AzureMonitorSourceConfigValidator` not found.

- [ ] **Step 3: Create AzureMonitorSourceDto**

```java
// lib/src/main/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceDto.java
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
package io.fleak.zephflow.lib.commands.azuremonitorsource;

import io.fleak.zephflow.api.CommandConfig;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public interface AzureMonitorSourceDto {

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {
    private String workspaceId;
    private String tenantId;
    private String kqlQuery;
    private String credentialId;
    @Builder.Default private int batchSize = 1000;
  }
}
```

- [ ] **Step 4: Create AzureMonitorSourceConfigValidator**

```java
// lib/src/main/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceConfigValidator.java
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
package io.fleak.zephflow.lib.commands.azuremonitorsource;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import org.apache.commons.lang3.StringUtils;

public class AzureMonitorSourceConfigValidator implements ConfigValidator {

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    AzureMonitorSourceDto.Config config = (AzureMonitorSourceDto.Config) commandConfig;

    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getWorkspaceId()), "workspaceId is required");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getTenantId()), "tenantId is required");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getKqlQuery()), "kqlQuery is required");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getCredentialId()), "credentialId is required");
    Preconditions.checkArgument(
        config.getBatchSize() >= 1, "batchSize must be at least 1");

    if (enforceCredentials(jobContext)) {
      lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
    }
  }
}
```

- [ ] **Step 5: Run validator tests**

```
./mvnw test -pl lib -Dtest=AzureMonitorSourceConfigValidatorTest -am
```

Expected: BUILD SUCCESS, 8 tests passing.

- [ ] **Step 6: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceDto.java
git add lib/src/main/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceConfigValidator.java
git add lib/src/test/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceConfigValidatorTest.java
git commit -m "feat: add AzureMonitorSourceDto and AzureMonitorSourceConfigValidator"
```

---

### Task 3: AzureMonitorQueryClient

HTTP client for the Log Analytics REST API. Posts KQL, parses the tables/columns/rows response into `List<Map<String, String>>`, and implements a single 401 retry.

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorQueryClient.java`
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorQueryClientTest.java`

- [ ] **Step 1: Write the failing tests**

```java
// lib/src/test/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorQueryClientTest.java
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
package io.fleak.zephflow.lib.commands.azuremonitorsource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.lib.commands.azure.EntraIdTokenProvider;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AzureMonitorQueryClientTest {

  private HttpClient mockHttpClient;
  private HttpResponse<String> mockResponse;
  private EntraIdTokenProvider mockTokenProvider;
  private AzureMonitorQueryClient client;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() throws Exception {
    mockHttpClient = mock(HttpClient.class);
    mockResponse = mock(HttpResponse.class);
    mockTokenProvider = mock(EntraIdTokenProvider.class);
    when(mockTokenProvider.getToken()).thenReturn("test-token");
    client = new AzureMonitorQueryClient("workspace-id", mockTokenProvider, mockHttpClient);
  }

  @Test
  @SuppressWarnings("unchecked")
  void executeQuery_parsesColumnsAndRows() throws Exception {
    String responseBody =
        """
        {
          "tables": [{
            "columns": [
              {"name": "TimeGenerated", "type": "datetime"},
              {"name": "message", "type": "string"},
              {"name": "severity", "type": "string"}
            ],
            "rows": [
              ["2024-01-01T00:00:00Z", "hello", "info"],
              ["2024-01-01T01:00:00Z", "world", "warn"]
            ]
          }]
        }
        """;
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseBody);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    List<Map<String, String>> result =
        client.executeQuery("TableName_CL | limit 10");

    assertEquals(2, result.size());
    assertEquals("2024-01-01T00:00:00Z", result.get(0).get("TimeGenerated"));
    assertEquals("hello", result.get(0).get("message"));
    assertEquals("info", result.get(0).get("severity"));
    assertEquals("world", result.get(1).get("message"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void executeQuery_nullValueBecomesEmptyString() throws Exception {
    String responseBody =
        """
        {
          "tables": [{
            "columns": [{"name": "col1", "type": "string"}],
            "rows": [[null]]
          }]
        }
        """;
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn(responseBody);
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    List<Map<String, String>> result = client.executeQuery("T | limit 1");

    assertEquals(1, result.size());
    assertEquals("", result.get(0).get("col1"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void executeQuery_401_retriesOnce_succeeds() throws Exception {
    HttpResponse<String> unauthorizedResponse = mock(HttpResponse.class);
    when(unauthorizedResponse.statusCode()).thenReturn(401);
    when(unauthorizedResponse.body()).thenReturn("Unauthorized");

    when(mockTokenProvider.getToken()).thenReturn("stale-token").thenReturn("fresh-token");
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body())
        .thenReturn(
            "{\"tables\":[{\"columns\":[{\"name\":\"c\",\"type\":\"string\"}],\"rows\":[[\"v\"]]}]}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(unauthorizedResponse)
        .thenReturn(mockResponse);

    List<Map<String, String>> result = client.executeQuery("T");

    assertEquals(1, result.size());
    assertEquals("v", result.get(0).get("c"));
    verify(mockTokenProvider).invalidate();
    verify(mockHttpClient, times(2)).send(any(), any());
  }

  @Test
  @SuppressWarnings("unchecked")
  void executeQuery_401_afterRetry_throwsRuntimeException() throws Exception {
    when(mockTokenProvider.getToken()).thenReturn("stale-token").thenReturn("fresh-token");
    when(mockResponse.statusCode()).thenReturn(401);
    when(mockResponse.body()).thenReturn("Unauthorized");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> client.executeQuery("T"));
    assertTrue(ex.getMessage().contains("token rejected after refresh"));
    verify(mockTokenProvider).invalidate();
  }

  @Test
  @SuppressWarnings("unchecked")
  void executeQuery_non200_throwsWithStatusAndBody() throws Exception {
    when(mockResponse.statusCode()).thenReturn(500);
    when(mockResponse.body()).thenReturn("Internal Server Error");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> client.executeQuery("T"));
    assertTrue(ex.getMessage().contains("500"));
    assertTrue(ex.getMessage().contains("Internal Server Error"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void executeQuery_emptyTablesArray_throwsRuntimeException() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("{\"tables\":[]}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> client.executeQuery("T"));
    assertTrue(ex.getMessage().toLowerCase().contains("tables"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void executeQuery_missingTablesField_throwsRuntimeException() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("{\"someOtherField\":42}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    RuntimeException ex =
        assertThrows(RuntimeException.class, () -> client.executeQuery("T"));
    assertTrue(ex.getMessage().toLowerCase().contains("tables"));
  }
}
```

- [ ] **Step 2: Run test — expect compile failure**

```
./mvnw test -pl lib -Dtest=AzureMonitorQueryClientTest -am 2>&1 | head -30
```

Expected: compilation error — `AzureMonitorQueryClient` not found.

- [ ] **Step 3: Implement AzureMonitorQueryClient**

```java
// lib/src/main/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorQueryClient.java
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
package io.fleak.zephflow.lib.commands.azuremonitorsource;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;

import com.fasterxml.jackson.databind.JsonNode;
import io.fleak.zephflow.lib.commands.azure.EntraIdTokenProvider;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AzureMonitorQueryClient {

  private static final String QUERY_ENDPOINT_TEMPLATE =
      "https://api.loganalytics.io/v1/workspaces/%s/query";

  private final String workspaceId;
  private final EntraIdTokenProvider tokenProvider;
  private final HttpClient httpClient;

  public AzureMonitorQueryClient(
      String workspaceId, EntraIdTokenProvider tokenProvider, HttpClient httpClient) {
    this.workspaceId = workspaceId;
    this.tokenProvider = tokenProvider;
    this.httpClient = httpClient;
  }

  public List<Map<String, String>> executeQuery(String kqlQuery) throws Exception {
    String token = tokenProvider.getToken();
    HttpResponse<String> response = sendRequest(kqlQuery, token);

    if (response.statusCode() == 401) {
      tokenProvider.invalidate();
      token = tokenProvider.getToken();
      response = sendRequest(kqlQuery, token);
      if (response.statusCode() == 401) {
        throw new RuntimeException(
            "Azure Monitor auth error 401: token rejected after refresh");
      }
    }

    if (response.statusCode() != 200) {
      throw new RuntimeException(
          "Azure Monitor API returned status "
              + response.statusCode()
              + ": "
              + response.body());
    }

    return parseResponse(response.body());
  }

  private HttpResponse<String> sendRequest(String kqlQuery, String token) throws Exception {
    String url = String.format(QUERY_ENDPOINT_TEMPLATE, workspaceId);
    String body = OBJECT_MAPPER.writeValueAsString(Map.of("query", kqlQuery));

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Authorization", "Bearer " + token)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .timeout(Duration.ofSeconds(60))
            .build();

    return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private List<Map<String, String>> parseResponse(String body) throws Exception {
    JsonNode root = OBJECT_MAPPER.readTree(body);
    JsonNode tables = root.path("tables");
    if (tables.isMissingNode() || !tables.isArray() || tables.isEmpty()) {
      throw new RuntimeException(
          "Azure Monitor response missing or empty tables array: " + body);
    }

    JsonNode table = tables.get(0);
    JsonNode columns = table.path("columns");
    JsonNode rows = table.path("rows");

    List<String> columnNames = new ArrayList<>();
    for (JsonNode col : columns) {
      columnNames.add(col.path("name").asText());
    }

    List<Map<String, String>> result = new ArrayList<>();
    for (JsonNode row : rows) {
      Map<String, String> map = new LinkedHashMap<>();
      for (int i = 0; i < columnNames.size(); i++) {
        JsonNode val = row.get(i);
        map.put(columnNames.get(i), (val == null || val.isNull()) ? "" : val.asText());
      }
      result.add(map);
    }
    return result;
  }
}
```

- [ ] **Step 4: Run query client tests**

```
./mvnw test -pl lib -Dtest=AzureMonitorQueryClientTest -am
```

Expected: BUILD SUCCESS, 7 tests passing.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorQueryClient.java
git add lib/src/test/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorQueryClientTest.java
git commit -m "feat: add AzureMonitorQueryClient"
```

---

### Task 4: AzureMonitorSourceFetcher

Pages the query results in batches. All rows are fetched on the first `fetch()` call and stored; subsequent calls return slices of size `batchSize`.

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceFetcher.java`
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceFetcherTest.java`

- [ ] **Step 1: Write the failing tests**

```java
// lib/src/test/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceFetcherTest.java
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
package io.fleak.zephflow.lib.commands.azuremonitorsource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AzureMonitorSourceFetcherTest {

  private static List<Map<String, String>> rows(int count) {
    List<Map<String, String>> list = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      list.add(Map.of("idx", String.valueOf(i)));
    }
    return list;
  }

  @Test
  void singleBatch_allRowsReturnedInOneFetch_thenExhausted() throws Exception {
    AzureMonitorQueryClient mockClient = mock(AzureMonitorQueryClient.class);
    when(mockClient.executeQuery("query")).thenReturn(rows(3));
    AzureMonitorSourceFetcher fetcher = new AzureMonitorSourceFetcher("query", 10, mockClient);

    assertFalse(fetcher.isExhausted());

    List<Map<String, String>> batch = fetcher.fetch();
    assertEquals(3, batch.size());
    assertEquals("0", batch.get(0).get("idx"));
    assertEquals("2", batch.get(2).get("idx"));

    assertTrue(fetcher.isExhausted());
    verify(mockClient, times(1)).executeQuery("query");
  }

  @Test
  void multiBatch_rowsSplitAcrossMultipleFetchCalls() throws Exception {
    AzureMonitorQueryClient mockClient = mock(AzureMonitorQueryClient.class);
    when(mockClient.executeQuery("query")).thenReturn(rows(5));
    AzureMonitorSourceFetcher fetcher = new AzureMonitorSourceFetcher("query", 2, mockClient);

    List<Map<String, String>> batch1 = fetcher.fetch();
    assertEquals(2, batch1.size());
    assertEquals("0", batch1.get(0).get("idx"));
    assertFalse(fetcher.isExhausted());

    List<Map<String, String>> batch2 = fetcher.fetch();
    assertEquals(2, batch2.size());
    assertEquals("2", batch2.get(0).get("idx"));
    assertFalse(fetcher.isExhausted());

    List<Map<String, String>> batch3 = fetcher.fetch();
    assertEquals(1, batch3.size());
    assertEquals("4", batch3.get(0).get("idx"));
    assertTrue(fetcher.isExhausted());

    // queryClient called exactly once
    verify(mockClient, times(1)).executeQuery("query");
  }

  @Test
  void emptyResult_isExhaustedImmediately_fetchReturnsEmpty() throws Exception {
    AzureMonitorQueryClient mockClient = mock(AzureMonitorQueryClient.class);
    when(mockClient.executeQuery("query")).thenReturn(List.of());
    AzureMonitorSourceFetcher fetcher = new AzureMonitorSourceFetcher("query", 10, mockClient);

    List<Map<String, String>> batch = fetcher.fetch();
    assertTrue(batch.isEmpty());
    assertTrue(fetcher.isExhausted());
  }

  @Test
  void fetchAfterExhaustion_returnsEmptyList() throws Exception {
    AzureMonitorQueryClient mockClient = mock(AzureMonitorQueryClient.class);
    when(mockClient.executeQuery("query")).thenReturn(rows(2));
    AzureMonitorSourceFetcher fetcher = new AzureMonitorSourceFetcher("query", 10, mockClient);

    fetcher.fetch(); // exhausts
    List<Map<String, String>> batch = fetcher.fetch(); // after exhaustion

    assertTrue(batch.isEmpty());
    assertTrue(fetcher.isExhausted());
    // executeQuery still called only once
    verify(mockClient, times(1)).executeQuery("query");
  }
}
```

- [ ] **Step 2: Run test — expect compile failure**

```
./mvnw test -pl lib -Dtest=AzureMonitorSourceFetcherTest -am 2>&1 | head -30
```

Expected: compilation error — `AzureMonitorSourceFetcher` not found.

- [ ] **Step 3: Implement AzureMonitorSourceFetcher**

```java
// lib/src/main/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceFetcher.java
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
package io.fleak.zephflow.lib.commands.azuremonitorsource;

import io.fleak.zephflow.lib.commands.source.CommitStrategy;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.commands.source.NoCommitStrategy;
import java.util.List;
import java.util.Map;

public class AzureMonitorSourceFetcher implements Fetcher<Map<String, String>> {

  private final String kqlQuery;
  private final int batchSize;
  private final AzureMonitorQueryClient queryClient;

  private List<Map<String, String>> allRows;
  private int offset = 0;

  public AzureMonitorSourceFetcher(
      String kqlQuery, int batchSize, AzureMonitorQueryClient queryClient) {
    this.kqlQuery = kqlQuery;
    this.batchSize = batchSize;
    this.queryClient = queryClient;
  }

  @Override
  public List<Map<String, String>> fetch() {
    if (allRows == null) {
      try {
        allRows = queryClient.executeQuery(kqlQuery);
      } catch (Exception e) {
        throw new RuntimeException("Failed to execute Azure Monitor query", e);
      }
    }
    if (isExhausted()) {
      return List.of();
    }
    int end = Math.min(offset + batchSize, allRows.size());
    List<Map<String, String>> batch = List.copyOf(allRows.subList(offset, end));
    offset = end;
    return batch;
  }

  @Override
  public boolean isExhausted() {
    return allRows != null && offset >= allRows.size();
  }

  @Override
  public CommitStrategy commitStrategy() {
    return NoCommitStrategy.INSTANCE;
  }

  @Override
  public void close() {}
}
```

- [ ] **Step 4: Run fetcher tests**

```
./mvnw test -pl lib -Dtest=AzureMonitorSourceFetcherTest -am
```

Expected: BUILD SUCCESS, 4 tests passing.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceFetcher.java
git add lib/src/test/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceFetcherTest.java
git commit -m "feat: add AzureMonitorSourceFetcher"
```

---

### Task 5: AzureMonitorSourceCommand, factory, and registration

Wire the fetcher into a `SimpleSourceCommand`, create the factory, register the command name in `MiscUtils`, and add the factory to `OperatorCommandRegistry`.

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceCommand.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceCommandFactory.java`
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java`
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java`

- [ ] **Step 1: Create AzureMonitorSourceCommand**

```java
// lib/src/main/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceCommand.java
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
package io.fleak.zephflow.lib.commands.azuremonitorsource;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.azure.EntraIdTokenProvider;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.commands.splunksource.MapRawDataConverter;
import io.fleak.zephflow.lib.commands.splunksource.MapRawDataEncoder;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

public class AzureMonitorSourceCommand extends SimpleSourceCommand<Map<String, String>> {

  public AzureMonitorSourceCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator) {
    super(nodeId, jobContext, configParser, configValidator);
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    AzureMonitorSourceDto.Config config = (AzureMonitorSourceDto.Config) commandConfig;

    UsernamePasswordCredential credential =
        lookupUsernamePasswordCredential(jobContext, config.getCredentialId());

    HttpClient httpClient =
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();

    EntraIdTokenProvider tokenProvider =
        new EntraIdTokenProvider(
            config.getTenantId(),
            credential.getUsername(),
            credential.getPassword(),
            "https://api.loganalytics.io/.default",
            httpClient);

    AzureMonitorQueryClient queryClient =
        new AzureMonitorQueryClient(config.getWorkspaceId(), tokenProvider, httpClient);

    Fetcher<Map<String, String>> fetcher =
        new AzureMonitorSourceFetcher(config.getKqlQuery(), config.getBatchSize(), queryClient);

    RawDataEncoder<Map<String, String>> encoder = new MapRawDataEncoder();
    RawDataConverter<Map<String, String>> converter = new MapRawDataConverter();

    Map<String, String> metricTags =
        basicCommandMetricTags(jobContext.getMetricTags(), commandName(), nodeId);
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

  @Override
  public SourceType sourceType() {
    return SourceType.BATCH;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_AZURE_MONITOR_SOURCE;
  }
}
```

- [ ] **Step 2: Create AzureMonitorSourceCommandFactory**

```java
// lib/src/main/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceCommandFactory.java
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
package io.fleak.zephflow.lib.commands.azuremonitorsource;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.lib.commands.JsonConfigParser;
import io.fleak.zephflow.lib.commands.source.SourceCommandFactory;

public class AzureMonitorSourceCommandFactory extends SourceCommandFactory {

  @Override
  public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
    JsonConfigParser<AzureMonitorSourceDto.Config> configParser =
        new JsonConfigParser<>(AzureMonitorSourceDto.Config.class);
    AzureMonitorSourceConfigValidator validator = new AzureMonitorSourceConfigValidator();
    return new AzureMonitorSourceCommand(nodeId, jobContext, configParser, validator);
  }
}
```

- [ ] **Step 3: Register command name in MiscUtils**

In `lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java`, add after line 101 (`COMMAND_NAME_SENTINEL_SINK = "sentinelsink";`):

```java
  String COMMAND_NAME_AZURE_MONITOR_SOURCE = "azuremonitorsource";
```

The block should look like:
```java
  String COMMAND_NAME_LDAP_SOURCE = "ldapsource";
  String COMMAND_NAME_SENTINEL_SINK = "sentinelsink";
  String COMMAND_NAME_AZURE_MONITOR_SOURCE = "azuremonitorsource";
  String METRIC_NAME_INPUT_EVENT_COUNT = "input_event_count";
```

- [ ] **Step 4: Register factory in OperatorCommandRegistry**

In `lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java`:

Add import:
```java
import io.fleak.zephflow.lib.commands.azuremonitorsource.AzureMonitorSourceCommandFactory;
```

Add entry after the `.put(COMMAND_NAME_SENTINEL_SINK, new SentinelSinkCommandFactory())` line:
```java
          .put(COMMAND_NAME_AZURE_MONITOR_SOURCE, new AzureMonitorSourceCommandFactory())
```

The block should look like:
```java
          .put(COMMAND_NAME_LDAP_SOURCE, new LdapSourceCommandFactory())
          .put(COMMAND_NAME_SENTINEL_SINK, new SentinelSinkCommandFactory())
          .put(COMMAND_NAME_AZURE_MONITOR_SOURCE, new AzureMonitorSourceCommandFactory())
          .build();
```

- [ ] **Step 5: Run full lib test suite**

```
./mvnw test -pl lib -am
```

Expected: BUILD SUCCESS — all existing tests still pass plus the new ones.

- [ ] **Step 6: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceCommand.java
git add lib/src/main/java/io/fleak/zephflow/lib/commands/azuremonitorsource/AzureMonitorSourceCommandFactory.java
git add lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java
git add lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java
git commit -m "feat: register azuremonitorsource command"
```

---

## Self-Review Checklist

**Spec coverage:**
- [x] `workspaceId`, `tenantId`, `kqlQuery`, `credentialId`, `batchSize` config fields → Task 2
- [x] `EntraIdTokenProvider` moved to `azure` package with `scope` param → Task 1
- [x] Sink passes `"https://monitor.azure.com/.default"` explicitly → Task 1 Step 5
- [x] Source passes `"https://api.loganalytics.io/.default"` → Task 5 Step 1
- [x] `AzureMonitorQueryClient` with POST + auth header + response parsing → Task 3
- [x] 401 retry pattern → Task 3
- [x] Non-200 throws with status + body → Task 3
- [x] `tables` missing/empty throws → Task 3
- [x] Null row values → empty string → Task 3
- [x] `AzureMonitorSourceFetcher` with `allRows` buffer, paging by `batchSize` → Task 4
- [x] `isExhausted()` correct semantics → Task 4
- [x] `NoCommitStrategy.INSTANCE` → Task 4
- [x] `commandName()` returns `"azuremonitorsource"` → Task 5
- [x] `sourceType()` returns `BATCH` → Task 5
- [x] `MiscUtils.COMMAND_NAME_AZURE_MONITOR_SOURCE` → Task 5
- [x] `OperatorCommandRegistry` entry → Task 5
- [x] `EntraIdTokenProviderTest` constructor call updated → Task 1 Step 2
- [x] `SentinelSinkFlusherTest` import updated → Task 1 Step 6

**Type consistency:** `EntraIdTokenProvider` constructed with 5 args everywhere (Tasks 1, 5). `AzureMonitorQueryClient` constructor matches usage in Task 5. `AzureMonitorSourceFetcher` constructor matches usage in Task 5.
