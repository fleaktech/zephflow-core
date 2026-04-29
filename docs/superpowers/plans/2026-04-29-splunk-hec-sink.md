# Splunk HEC Sink Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `splunkhecsink` operator to zephflow-core that delivers events to Splunk via the HTTP Event Collector (HEC) `/services/collector/event` endpoint.

**Architecture:** Follow the existing `SimpleSinkCommand<T>` pattern used by `ElasticsearchSink` and `AzureMonitorSink`. Build NDJSON-batched JSON-wrapped events with static `index`/`sourcetype`/`source`, post via Java's built-in `HttpClient` with `Authorization: Splunk <token>` header, and translate HEC error responses into per-record `ErrorOutput`s.

**Tech Stack:** Java 17+, Lombok, Jackson (`OBJECT_MAPPER`), Java built-in `java.net.http.HttpClient`, Apache Commons Lang `StringUtils`, JUnit 5 + Mockito.

**Spec:** `docs/superpowers/specs/2026-04-29-splunk-hec-sink-design.md`

**Conventions to follow** (verify before writing each file by skimming the matching Elasticsearch/Azure Monitor counterpart):
- Apache 2.0 license header (copy from `ElasticsearchSinkCommand.java`).
- `@Slf4j` for logging where helpful.
- Lombok `@Data @Builder @NoArgsConstructor @AllArgsConstructor` on Config classes.
- `Preconditions.checkArgument(...)` for validation.
- Tests under `lib/src/test/java/...`, mirror package layout from `lib/src/main/java/...`.

**Build/test commands** (run from repo root `/Users/dan/fleak/zephflow-core`):
- Compile only: `./gradlew :lib:compileJava`
- Compile tests: `./gradlew :lib:compileTestJava`
- Run a single test class: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.splunkhecsink.SplunkHecSinkFlusherTest"`
- Run all sink tests: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.splunkhecsink.*"`
- Full lib test suite: `./gradlew :lib:test`

---

## File Structure

All new files live in `lib/src/main/java/io/fleak/zephflow/lib/commands/splunkhecsink/` (main) and `lib/src/test/java/io/fleak/zephflow/lib/commands/splunkhecsink/` (test).

| File | Responsibility |
|---|---|
| `SplunkHecSinkDto.java` | Interface holding the `Config` DTO and `DEFAULT_BATCH_SIZE` constant. |
| `SplunkHecOutboundEvent.java` | Record holding pre-serialized NDJSON line bytes. |
| `SplunkHecSinkMessageProcessor.java` | Builds the HEC envelope per record. |
| `SplunkHecSinkConfigValidator.java` | Validates the config. |
| `SplunkHecSinkFlusher.java` | Posts the batch to HEC, translates response into `FlushResult`. |
| `SplunkHecSinkCommand.java` | Wires flusher, processor, counters; constructs `HttpClient` with optional SSL bypass. |
| `SplunkHecSinkCommandFactory.java` | Returns `SplunkHecSinkCommand`. |

Modified files:
- `lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java` — add `COMMAND_NAME_SPLUNK_HEC_SINK = "splunkhecsink"` constant.
- `lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java` — register the factory.

---

## Task 1: Create `SplunkHecSinkDto`

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/splunkhecsink/SplunkHecSinkDto.java`

Pure data class — no test needed.

- [ ] **Step 1: Create the DTO file**

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
package io.fleak.zephflow.lib.commands.splunkhecsink;

import io.fleak.zephflow.api.CommandConfig;
import lombok.*;

public interface SplunkHecSinkDto {

  int DEFAULT_BATCH_SIZE = 500;

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {
    /** Full HEC endpoint URL, e.g. https://splunk:8088/services/collector/event */
    @NonNull private String hecUrl;

    /** ID of an ApiKeyCredential whose `key` is the HEC token. */
    @NonNull private String credentialId;

    /** Optional. If null, HEC token's default index is used. */
    private String index;

    /** Optional. If null, HEC token's default sourcetype is used. */
    private String sourcetype;

    /** Optional pipeline-level source tag. */
    private String source;

    /** Defaults to true. Set false for self-signed Splunk deployments. */
    @Builder.Default private Boolean verifySsl = true;

    @Builder.Default private Integer batchSize = DEFAULT_BATCH_SIZE;
  }
}
```

- [ ] **Step 2: Compile**

Run: `./gradlew :lib:compileJava`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 3: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/splunkhecsink/SplunkHecSinkDto.java
git commit -m "feat(splunkhecsink): add Config DTO for Splunk HEC sink"
```

---

## Task 2: Create `SplunkHecOutboundEvent`

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/splunkhecsink/SplunkHecOutboundEvent.java`

Pure data record — no test needed.

- [ ] **Step 1: Create the record**

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
package io.fleak.zephflow.lib.commands.splunkhecsink;

/**
 * One pre-serialized HEC NDJSON line (JSON envelope + trailing newline) ready to be concatenated
 * into the request body.
 */
public record SplunkHecOutboundEvent(byte[] preEncodedNdjsonLine) {}
```

- [ ] **Step 2: Compile**

Run: `./gradlew :lib:compileJava`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 3: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/splunkhecsink/SplunkHecOutboundEvent.java
git commit -m "feat(splunkhecsink): add SplunkHecOutboundEvent record"
```

---

## Task 3: Create `SplunkHecSinkMessageProcessor` (TDD)

**Files:**
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/splunkhecsink/SplunkHecSinkMessageProcessorTest.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/splunkhecsink/SplunkHecSinkMessageProcessor.java`

The processor wraps each record into the HEC envelope `{"event":..., "sourcetype":"...", "index":"...", "source":"..."}` and pre-serializes to UTF-8 bytes with a trailing newline.

- [ ] **Step 1: Write the failing test**

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
package io.fleak.zephflow.lib.commands.splunkhecsink;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.JsonNode;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SplunkHecSinkMessageProcessorTest {

  private RecordFleakData sampleRecord() {
    return new RecordFleakData(Map.of("msg", new StringPrimitiveFleakData("hello")));
  }

  @Test
  void preprocess_allMetadataSet_includesAllKeysAndTrailingNewline() throws Exception {
    SplunkHecSinkMessageProcessor processor =
        new SplunkHecSinkMessageProcessor("security_logs", "paloalto:traffic", "fw01");

    SplunkHecOutboundEvent out = processor.preprocess(sampleRecord(), 0L);

    String line = new String(out.preEncodedNdjsonLine(), StandardCharsets.UTF_8);
    assertTrue(line.endsWith("\n"), "line must end with newline");

    JsonNode envelope = OBJECT_MAPPER.readTree(line);
    assertTrue(envelope.has("event"));
    assertEquals("hello", envelope.get("event").get("msg").asText());
    assertEquals("security_logs", envelope.get("index").asText());
    assertEquals("paloalto:traffic", envelope.get("sourcetype").asText());
    assertEquals("fw01", envelope.get("source").asText());
  }

  @Test
  void preprocess_allMetadataNull_omitsAllOptionalKeys() throws Exception {
    SplunkHecSinkMessageProcessor processor =
        new SplunkHecSinkMessageProcessor(null, null, null);

    SplunkHecOutboundEvent out = processor.preprocess(sampleRecord(), 0L);

    String line = new String(out.preEncodedNdjsonLine(), StandardCharsets.UTF_8);
    JsonNode envelope = OBJECT_MAPPER.readTree(line);
    assertTrue(envelope.has("event"));
    assertFalse(envelope.has("index"));
    assertFalse(envelope.has("sourcetype"));
    assertFalse(envelope.has("source"));
  }

  @Test
  void preprocess_blankMetadataTreatedAsAbsent() throws Exception {
    SplunkHecSinkMessageProcessor processor =
        new SplunkHecSinkMessageProcessor("", "  ", "");

    SplunkHecOutboundEvent out = processor.preprocess(sampleRecord(), 0L);

    JsonNode envelope =
        OBJECT_MAPPER.readTree(new String(out.preEncodedNdjsonLine(), StandardCharsets.UTF_8));
    assertFalse(envelope.has("index"));
    assertFalse(envelope.has("sourcetype"));
    assertFalse(envelope.has("source"));
  }

  @Test
  void preprocess_partialMetadata_includesOnlyNonBlankKeys() throws Exception {
    SplunkHecSinkMessageProcessor processor =
        new SplunkHecSinkMessageProcessor("security_logs", null, null);

    SplunkHecOutboundEvent out = processor.preprocess(sampleRecord(), 0L);

    JsonNode envelope =
        OBJECT_MAPPER.readTree(new String(out.preEncodedNdjsonLine(), StandardCharsets.UTF_8));
    assertEquals("security_logs", envelope.get("index").asText());
    assertFalse(envelope.has("sourcetype"));
    assertFalse(envelope.has("source"));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.splunkhecsink.SplunkHecSinkMessageProcessorTest"`
Expected: FAIL — `SplunkHecSinkMessageProcessor` class does not exist (compilation error).

- [ ] **Step 3: Implement the message processor**

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
package io.fleak.zephflow.lib.commands.splunkhecsink;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;

import com.fasterxml.jackson.databind.node.ObjectNode;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.StringUtils;

public class SplunkHecSinkMessageProcessor
    implements SimpleSinkCommand.SinkMessagePreProcessor<SplunkHecOutboundEvent> {

  private final String index;
  private final String sourcetype;
  private final String source;

  public SplunkHecSinkMessageProcessor(String index, String sourcetype, String source) {
    this.index = index;
    this.sourcetype = sourcetype;
    this.source = source;
  }

  @Override
  public SplunkHecOutboundEvent preprocess(RecordFleakData event, long ts) throws Exception {
    ObjectNode envelope = OBJECT_MAPPER.createObjectNode();
    envelope.set("event", OBJECT_MAPPER.readTree(toJsonString(event)));
    if (StringUtils.isNotBlank(sourcetype)) {
      envelope.put("sourcetype", sourcetype);
    }
    if (StringUtils.isNotBlank(index)) {
      envelope.put("index", index);
    }
    if (StringUtils.isNotBlank(source)) {
      envelope.put("source", source);
    }
    String json = OBJECT_MAPPER.writeValueAsString(envelope) + "\n";
    return new SplunkHecOutboundEvent(json.getBytes(StandardCharsets.UTF_8));
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.splunkhecsink.SplunkHecSinkMessageProcessorTest"`
Expected: PASS — all 4 tests green.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/splunkhecsink/SplunkHecSinkMessageProcessor.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/splunkhecsink/SplunkHecSinkMessageProcessorTest.java
git commit -m "feat(splunkhecsink): add message processor that builds HEC envelope"
```

---

## Task 4: Create `SplunkHecSinkConfigValidator` (TDD)

**Files:**
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/splunkhecsink/SplunkHecSinkConfigValidatorTest.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/splunkhecsink/SplunkHecSinkConfigValidator.java`

Validation rules from the spec:
1. `hecUrl` non-blank, parses as URI with `http` or `https` scheme.
2. `credentialId` non-blank; resolves to a non-null `ApiKeyCredential` (only enforced when `enforceCredentials(jobContext)` is true — same gate Elasticsearch uses).
3. `batchSize >= 1` if explicitly set.

- [ ] **Step 1: Write the failing test**

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
package io.fleak.zephflow.lib.commands.splunkhecsink;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.TestUtils;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SplunkHecSinkConfigValidatorTest {

  static final JobContext TEST_JOB_CONTEXT =
      JobContext.builder()
          .metricTags(TestUtils.JOB_CONTEXT.getMetricTags())
          .otherProperties(
              new HashMap<>(Map.of("hec_token", new HashMap<>(Map.of("key", "abcd-1234")))))
          .build();

  private final SplunkHecSinkConfigValidator validator = new SplunkHecSinkConfigValidator();

  private SplunkHecSinkDto.Config validConfig() {
    return SplunkHecSinkDto.Config.builder()
        .hecUrl("https://splunk:8088/services/collector/event")
        .credentialId("hec_token")
        .index("security_logs")
        .sourcetype("paloalto:traffic")
        .build();
  }

  @Test
  void validateConfig_minimalValid() {
    SplunkHecSinkDto.Config c =
        SplunkHecSinkDto.Config.builder()
            .hecUrl("https://splunk:8088/services/collector/event")
            .credentialId("hec_token")
            .build();
    assertDoesNotThrow(() -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_fullyPopulatedValid() {
    assertDoesNotThrow(() -> validator.validateConfig(validConfig(), "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_blankHecUrl() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setHecUrl("");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_malformedHecUrl() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setHecUrl("not a url");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_nonHttpScheme() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setHecUrl("ftp://splunk:8088/services/collector/event");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_blankCredentialId() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setCredentialId("");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_batchSizeZero() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setBatchSize(0);
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_batchSizeOne_isValid() {
    SplunkHecSinkDto.Config c = validConfig();
    c.setBatchSize(1);
    assertDoesNotThrow(() -> validator.validateConfig(c, "node", TEST_JOB_CONTEXT));
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.splunkhecsink.SplunkHecSinkConfigValidatorTest"`
Expected: FAIL — `SplunkHecSinkConfigValidator` class does not exist.

- [ ] **Step 3: Implement the validator**

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
package io.fleak.zephflow.lib.commands.splunkhecsink;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.commons.lang3.StringUtils;

public class SplunkHecSinkConfigValidator implements ConfigValidator {

  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    SplunkHecSinkDto.Config config = (SplunkHecSinkDto.Config) commandConfig;

    Preconditions.checkArgument(StringUtils.isNotBlank(config.getHecUrl()), "hecUrl is required");
    URI uri;
    try {
      uri = new URI(config.getHecUrl());
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("hecUrl is not a valid URI: " + config.getHecUrl(), e);
    }
    String scheme = uri.getScheme();
    Preconditions.checkArgument(
        "http".equalsIgnoreCase(scheme) || "https".equalsIgnoreCase(scheme),
        "hecUrl must use http or https scheme");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(uri.getHost()), "hecUrl must include a host");

    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getCredentialId()), "credentialId is required");

    if (config.getBatchSize() != null) {
      Preconditions.checkArgument(config.getBatchSize() >= 1, "batchSize must be at least 1");
    }

    if (enforceCredentials(jobContext)) {
      lookupApiKeyCredential(jobContext, config.getCredentialId());
    }
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.splunkhecsink.SplunkHecSinkConfigValidatorTest"`
Expected: PASS — all 8 tests green.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/splunkhecsink/SplunkHecSinkConfigValidator.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/splunkhecsink/SplunkHecSinkConfigValidatorTest.java
git commit -m "feat(splunkhecsink): add config validator for HEC URL, credentialId, and batchSize"
```

---

## Task 5: Create `SplunkHecSinkFlusher` (TDD)

**Files:**
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/splunkhecsink/SplunkHecSinkFlusherTest.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/splunkhecsink/SplunkHecSinkFlusher.java`

The flusher takes a pre-constructed `HttpClient` (so tests can mock it), the HEC URL, and the HEC token. On flush, it concatenates the pre-encoded NDJSON lines, posts with `Authorization: Splunk <token>`, and translates the response into a `FlushResult`. Splunk HEC returns a single status per batch — there is no per-event status. So either all records succeed or all fail.

Response handling:
- HTTP 200 → all success.
- HTTP 4xx/5xx → all failed; build the error reason as `"Splunk HEC error <status>: <text>"` where `<text>` is parsed from the HEC error JSON `{"text":"...","code":N}` if present, otherwise the raw response body (truncated to 500 chars).
- IOException / other → throw, let `SimpleSinkCommand` mark all records failed.

- [ ] **Step 1: Write the failing test**

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
package io.fleak.zephflow.lib.commands.splunkhecsink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class SplunkHecSinkFlusherTest {

  private HttpClient mockHttpClient;
  private HttpResponse<String> mockResponse;
  private SplunkHecSinkFlusher flusher;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    mockHttpClient = mock(HttpClient.class);
    mockResponse = mock(HttpResponse.class);
    flusher =
        new SplunkHecSinkFlusher(
            "https://splunk:8088/services/collector/event", "abcd-1234", mockHttpClient);
  }

  private SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> oneEvent(String line) {
    SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    RecordFleakData record =
        new RecordFleakData(Map.of("msg", new StringPrimitiveFleakData("hello")));
    events.add(record, new SplunkHecOutboundEvent(line.getBytes(StandardCharsets.UTF_8)));
    return events;
  }

  @Test
  void flush_emptyBatch_noHttpCall() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events =
        new SimpleSinkCommand.PreparedInputEvents<>();

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(0, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    verifyNoInteractions(mockHttpClient);
  }

  @Test
  @SuppressWarnings("unchecked")
  void flush_200_returnsSuccessForAllRecords() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("{\"text\":\"Success\",\"code\":0}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events =
        oneEvent("{\"event\":{\"msg\":\"hello\"}}\n");

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(1, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    assertTrue(result.flushedDataSize() > 0);
  }

  @Test
  @SuppressWarnings("unchecked")
  void flush_4xxWithHecErrorJson_returnsErrorPerRecordWithHecReason() throws Exception {
    when(mockResponse.statusCode()).thenReturn(403);
    when(mockResponse.body()).thenReturn("{\"text\":\"Invalid token\",\"code\":4}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events =
        oneEvent("{\"event\":{\"msg\":\"hello\"}}\n");

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(0, result.successCount());
    assertEquals(1, result.errorOutputList().size());
    String reason = result.errorOutputList().get(0).errorMessage();
    assertTrue(reason.contains("403"), "reason should include status code: " + reason);
    assertTrue(reason.contains("Invalid token"), "reason should include HEC text: " + reason);
  }

  @Test
  @SuppressWarnings("unchecked")
  void flush_5xxWithNonJsonBody_returnsErrorPerRecordWithStatusFallback() throws Exception {
    when(mockResponse.statusCode()).thenReturn(503);
    when(mockResponse.body()).thenReturn("Service Unavailable");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events =
        oneEvent("{\"event\":{\"msg\":\"hello\"}}\n");

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(0, result.successCount());
    assertEquals(1, result.errorOutputList().size());
    String reason = result.errorOutputList().get(0).errorMessage();
    assertTrue(reason.contains("503"), "reason should include status code: " + reason);
    assertTrue(
        reason.contains("Service Unavailable"),
        "reason should include raw body when not HEC JSON: " + reason);
  }

  @Test
  @SuppressWarnings("unchecked")
  void flush_401_returnsAuthErrorPerRecord_noRetry() throws Exception {
    when(mockResponse.statusCode()).thenReturn(401);
    when(mockResponse.body()).thenReturn("{\"text\":\"Unauthorized\",\"code\":2}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events =
        oneEvent("{\"event\":{\"msg\":\"hello\"}}\n");

    SimpleSinkCommand.FlushResult result = flusher.flush(events, Map.of());

    assertEquals(0, result.successCount());
    assertEquals(1, result.errorOutputList().size());
    assertTrue(result.errorOutputList().get(0).errorMessage().contains("401"));
    verify(mockHttpClient, times(1)).send(any(), any()); // no retry
  }

  @Test
  @SuppressWarnings("unchecked")
  void flush_networkException_propagates() throws Exception {
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new java.io.IOException("Connection refused"));

    SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events =
        oneEvent("{\"event\":{\"msg\":\"hello\"}}\n");

    Exception thrown = assertThrows(Exception.class, () -> flusher.flush(events, Map.of()));
    assertTrue(thrown.getMessage().contains("Connection refused"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void flush_buildsValidNdjsonBodyWithSplunkAuthHeader() throws Exception {
    when(mockResponse.statusCode()).thenReturn(200);
    when(mockResponse.body()).thenReturn("{\"text\":\"Success\",\"code\":0}");
    when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(mockResponse);

    SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> events =
        new SimpleSinkCommand.PreparedInputEvents<>();
    RecordFleakData rec1 = new RecordFleakData(Map.of("a", new StringPrimitiveFleakData("1")));
    RecordFleakData rec2 = new RecordFleakData(Map.of("b", new StringPrimitiveFleakData("2")));
    events.add(rec1, new SplunkHecOutboundEvent("{\"event\":{\"a\":\"1\"}}\n".getBytes()));
    events.add(rec2, new SplunkHecOutboundEvent("{\"event\":{\"b\":\"2\"}}\n".getBytes()));

    flusher.flush(events, Map.of());

    ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(mockHttpClient).send(captor.capture(), any());
    HttpRequest sent = captor.getValue();
    assertEquals(
        "Splunk abcd-1234",
        sent.headers().firstValue("Authorization").orElseThrow(),
        "Auth header must use 'Splunk <token>' scheme");
    assertEquals(
        "application/json",
        sent.headers().firstValue("Content-Type").orElseThrow());
    assertEquals(
        "https://splunk:8088/services/collector/event", sent.uri().toString());
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.splunkhecsink.SplunkHecSinkFlusherTest"`
Expected: FAIL — `SplunkHecSinkFlusher` class does not exist.

- [ ] **Step 3: Implement the flusher**

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
package io.fleak.zephflow.lib.commands.splunkhecsink;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;

import com.fasterxml.jackson.databind.JsonNode;
import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SplunkHecSinkFlusher implements SimpleSinkCommand.Flusher<SplunkHecOutboundEvent> {

  private static final int MAX_BODY_SNIPPET = 500;

  private final String hecUrl;
  private final String authHeader;
  private final HttpClient httpClient;

  public SplunkHecSinkFlusher(String hecUrl, String hecToken, HttpClient httpClient) {
    this.hecUrl = hecUrl;
    this.authHeader = "Splunk " + hecToken;
    this.httpClient = httpClient;
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> preparedInputEvents,
      Map<String, String> metricTags)
      throws Exception {
    List<SplunkHecOutboundEvent> events = preparedInputEvents.preparedList();
    if (events.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    for (SplunkHecOutboundEvent e : events) {
      buf.write(e.preEncodedNdjsonLine());
    }
    byte[] bodyBytes = buf.toByteArray();

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(hecUrl))
            .header("Authorization", authHeader)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes))
            .timeout(Duration.ofSeconds(60))
            .build();

    HttpResponse<String> response =
        httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    int status = response.statusCode();
    if (status == 200) {
      log.debug("Successfully sent {} events to Splunk HEC", events.size());
      return new SimpleSinkCommand.FlushResult(events.size(), bodyBytes.length, List.of());
    }

    String body = response.body() == null ? "" : response.body();
    String reason = "Splunk HEC error " + status + ": " + extractHecErrorText(body);
    log.error("Splunk HEC returned status {}: {}", status, body);
    List<ErrorOutput> errors =
        preparedInputEvents.rawAndPreparedList().stream()
            .map(p -> new ErrorOutput(p.getLeft(), reason))
            .toList();
    return new SimpleSinkCommand.FlushResult(0, 0, errors);
  }

  /** Returns the HEC `text` field from a JSON error body, or a truncated raw body fallback. */
  private static String extractHecErrorText(String body) {
    try {
      JsonNode node = OBJECT_MAPPER.readTree(body);
      JsonNode text = node.get("text");
      if (text != null && text.isTextual()) {
        return text.asText();
      }
    } catch (Exception ignore) {
      // Not JSON or not the expected shape — fall through to raw body.
    }
    return body.length() > MAX_BODY_SNIPPET ? body.substring(0, MAX_BODY_SNIPPET) : body;
  }

  @Override
  public void close() {}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.splunkhecsink.SplunkHecSinkFlusherTest"`
Expected: PASS — all 7 tests green.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/splunkhecsink/SplunkHecSinkFlusher.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/splunkhecsink/SplunkHecSinkFlusherTest.java
git commit -m "feat(splunkhecsink): add flusher posting NDJSON batch to HEC"
```

---

## Task 6: Create `SplunkHecSinkCommand`

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/splunkhecsink/SplunkHecSinkCommand.java`

The command class wires the flusher, message processor, and counters. It also constructs the `HttpClient` — including the trust-all variant when `verifySsl=false`.

The `COMMAND_NAME_SPLUNK_HEC_SINK` constant is referenced here; we add it to `MiscUtils` in Task 8. Until then, this file will fail to compile — we will address that ordering in Step 1 below.

- [ ] **Step 1: Add the command name constant first** (so the command compiles)

Edit `lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java`. Find the block of `COMMAND_NAME_*` constants (around line 100, near `COMMAND_NAME_AZURE_MONITOR_SINK` and `COMMAND_NAME_ELASTICSEARCH_SINK`). Add this line alongside them:

```java
String COMMAND_NAME_SPLUNK_HEC_SINK = "splunkhecsink";
```

Place it adjacent to the other sink command names (no specific ordering required by the codebase, but keep it grouped).

- [ ] **Step 2: Create the command class**

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
package io.fleak.zephflow.lib.commands.splunkhecsink;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.credentials.ApiKeyCredential;
import java.net.http.HttpClient;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public class SplunkHecSinkCommand extends SimpleSinkCommand<SplunkHecOutboundEvent> {

  protected SplunkHecSinkCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator) {
    super(nodeId, jobContext, configParser, configValidator);
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_SPLUNK_HEC_SINK;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    SinkCounters counters =
        createSinkCounters(metricClientProvider, jobContext, commandName(), nodeId);

    SplunkHecSinkDto.Config config = (SplunkHecSinkDto.Config) commandConfig;

    ApiKeyCredential credential = lookupApiKeyCredential(jobContext, config.getCredentialId());
    String hecToken = credential.getKey();

    boolean verifySsl = config.getVerifySsl() == null || config.getVerifySsl();
    HttpClient httpClient = buildHttpClient(verifySsl);

    SimpleSinkCommand.Flusher<SplunkHecOutboundEvent> flusher =
        new SplunkHecSinkFlusher(config.getHecUrl(), hecToken, httpClient);

    SimpleSinkCommand.SinkMessagePreProcessor<SplunkHecOutboundEvent> messagePreProcessor =
        new SplunkHecSinkMessageProcessor(
            config.getIndex(), config.getSourcetype(), config.getSource());

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
    SplunkHecSinkDto.Config config = (SplunkHecSinkDto.Config) commandConfig;
    return config.getBatchSize() != null
        ? config.getBatchSize()
        : SplunkHecSinkDto.DEFAULT_BATCH_SIZE;
  }

  /**
   * Builds the HttpClient. When verifySsl is false, the client trusts all certificates and skips
   * hostname verification — intended for self-signed Splunk deployments. The unsafe path is
   * deliberately confined to this method so it is easy to grep for.
   */
  static HttpClient buildHttpClient(boolean verifySsl) {
    HttpClient.Builder builder =
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30));
    if (verifySsl) {
      return builder.build();
    }
    try {
      TrustManager[] trustAll =
          new TrustManager[] {
            new X509TrustManager() {
              @Override
              public void checkClientTrusted(X509Certificate[] chain, String authType) {}

              @Override
              public void checkServerTrusted(X509Certificate[] chain, String authType) {}

              @Override
              public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
              }
            }
          };
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(null, trustAll, new SecureRandom());
      SSLParameters sslParameters = new SSLParameters();
      sslParameters.setEndpointIdentificationAlgorithm("");
      return builder.sslContext(sslContext).sslParameters(sslParameters).build();
    } catch (Exception e) {
      throw new RuntimeException("Failed to build trust-all HttpClient", e);
    }
  }
}
```

- [ ] **Step 3: Compile**

Run: `./gradlew :lib:compileJava`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 4: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java \
        lib/src/main/java/io/fleak/zephflow/lib/commands/splunkhecsink/SplunkHecSinkCommand.java
git commit -m "feat(splunkhecsink): add command class wiring flusher, processor, and HttpClient"
```

---

## Task 7: Create `SplunkHecSinkCommandFactory`

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/splunkhecsink/SplunkHecSinkCommandFactory.java`

- [ ] **Step 1: Create the factory**

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
package io.fleak.zephflow.lib.commands.splunkhecsink;

import io.fleak.zephflow.api.CommandFactory;
import io.fleak.zephflow.api.CommandType;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.lib.commands.JsonConfigParser;

public class SplunkHecSinkCommandFactory extends CommandFactory {

  @Override
  public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
    return new SplunkHecSinkCommand(
        nodeId,
        jobContext,
        new JsonConfigParser<>(SplunkHecSinkDto.Config.class),
        new SplunkHecSinkConfigValidator());
  }

  @Override
  public CommandType commandType() {
    return CommandType.SINK;
  }
}
```

- [ ] **Step 2: Compile**

Run: `./gradlew :lib:compileJava`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 3: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/splunkhecsink/SplunkHecSinkCommandFactory.java
git commit -m "feat(splunkhecsink): add CommandFactory"
```

---

## Task 8: Register the sink in `OperatorCommandRegistry`

**Files:**
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java`

- [ ] **Step 1: Add the import**

In the import block at the top of `OperatorCommandRegistry.java`, add (alphabetically grouped near the other `splunk*` import — there is already `import io.fleak.zephflow.lib.commands.splunksource.SplunkSourceCommandFactory;`):

```java
import io.fleak.zephflow.lib.commands.splunkhecsink.SplunkHecSinkCommandFactory;
```

- [ ] **Step 2: Add the registry entry**

Inside the `OPERATOR_COMMANDS` map builder (the `.put(...)` chain), add a new line — order does not matter, but place it adjacent to other sinks for readability:

```java
.put(COMMAND_NAME_SPLUNK_HEC_SINK, new SplunkHecSinkCommandFactory())
```

- [ ] **Step 3: Compile**

Run: `./gradlew :lib:compileJava`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 4: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java
git commit -m "feat(splunkhecsink): register splunkhecsink in OperatorCommandRegistry"
```

---

## Task 9: Final smoke test — full lib test suite

**Files:** None modified.

- [ ] **Step 1: Run all sink tests**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.splunkhecsink.*"`
Expected: All Splunk HEC tests pass.

- [ ] **Step 2: Run the full lib test suite**

Run: `./gradlew :lib:test`
Expected: BUILD SUCCESSFUL — no regressions in other sinks/sources.

If anything fails outside the new code, investigate before declaring done. If the failure is unrelated (e.g., a flaky test that was failing before), note it and confirm with the user before proceeding.

- [ ] **Step 3: Confirm the registration is wired end-to-end**

Run: `git log --oneline -10` — confirm all 8 commits are present in order.

Run from the repo root:
```bash
grep -n "splunkhecsink" lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java
grep -n "COMMAND_NAME_SPLUNK_HEC_SINK" lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java
```
Expected: One match in each file.

No final commit — this task only verifies.
