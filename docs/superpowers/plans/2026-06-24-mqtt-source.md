# MQTT Source Command Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a new streaming source command `mqttsource` that subscribes to an MQTT broker (Eclipse Paho v5), buffers incoming messages into a bounded queue, and emits them into the pipeline deserialized per a configurable `EncodingType`.

**Architecture:** Mirrors the existing `activemqsource` command. A `MqttClientProvider` isolates Paho client/connection-option construction. The Paho `messageArrived` callback enqueues payloads into a bounded `BlockingQueue<byte[]>`. `MqttSourceFetcher.fetch()` blocks up to `receiveTimeoutMs` for the first message then drains up to `maxBatchSize`. Acknowledgment is auto-ack with a fixed `NONE` commit strategy. `MqttSourceCommand` extends `SimpleSourceCommand<SerializedEvent>` and wires the standard metric counters, optional DLQ writer, `BytesRawDataEncoder`, and a `BytesRawDataConverter` built from `encodingType`.

**Tech Stack:** Java 21, Lombok, Eclipse Paho MQTT v5 client, JUnit 5, Mockito, Gradle version catalog.

## Global Constraints

- Every Java file begins with the standard Apache 2.0 license header used in all sibling files (copy verbatim from `ActiveMqSourceCommand.java`, "Copyright 2025 Fleak Tech Inc.").
- No explanatory inline comments in production code. Only the license header.
- Full, descriptive variable names; readable, intention-revealing method names.
- Package: `io.fleak.zephflow.lib.commands.mqttsource`.
- Configurable `EncodingType`, validated via `DeserializerFactory.validateEncodingType(...)`.
- Commit strategy is fixed to `NoCommitStrategy.INSTANCE`; no `CommitStrategyType` enum, no committer.
- Source produces `SerializedEvent` with `new SerializedEvent(null, payloadBytes, null)`.
- Run tests from the repo root with `./gradlew :lib:test --tests <FQCN>`.

---

### Task 1: Add the Paho v5 dependency

**Files:**
- Modify: `gradle/libs.versions.toml` (`[versions]` near line 41; `[libraries]` near line 179)
- Modify: `lib/build.gradle` (dependencies block near line 49)

**Interfaces:**
- Consumes: nothing.
- Produces: Gradle alias `libs.paho.mqttv5.client` resolving to `org.eclipse.paho:org.eclipse.paho.mqttv5.client:1.2.5`, available on the `lib` module compile classpath. Makes classes `org.eclipse.paho.mqttv5.client.MqttClient`, `MqttConnectionOptions`, `MqttConnectionOptionsBuilder`, `MqttCallback`, `MqttSubscription`, and `org.eclipse.paho.mqttv5.common.MqttMessage` available.

- [ ] **Step 1: Add the version to `[versions]`**

In `gradle/libs.versions.toml`, after the line `jakartaJms = "3.1.0"`, add:

```toml
pahoMqttV5 = "1.2.5"
```

- [ ] **Step 2: Add the library alias to `[libraries]`**

In `gradle/libs.versions.toml`, after the line defining `jakarta-jms-api = { ... }`, add:

```toml
paho-mqttv5-client = { module = "org.eclipse.paho:org.eclipse.paho.mqttv5.client", version.ref = "pahoMqttV5" }
```

- [ ] **Step 3: Add the implementation dependency**

In `lib/build.gradle`, immediately after the line `implementation libs.artemis.jakarta.client`, add:

```groovy
  implementation libs.paho.mqttv5.client
```

- [ ] **Step 4: Verify the dependency resolves**

Run: `./gradlew :lib:dependencies --configuration compileClasspath | grep paho`
Expected: a line containing `org.eclipse.paho.mqttv5.client:1.2.5`.

- [ ] **Step 5: Commit**

```bash
git add gradle/libs.versions.toml lib/build.gradle
git commit -m "build: add Eclipse Paho MQTT v5 client dependency"
```

---

### Task 2: Config DTO (`MqttSourceDto`)

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/mqttsource/MqttSourceDto.java`

**Interfaces:**
- Consumes: `io.fleak.zephflow.api.CommandConfig`, `io.fleak.zephflow.lib.serdes.EncodingType`.
- Produces: `MqttSourceDto.Config` (Lombok `@Data @Builder @NoArgsConstructor @AllArgsConstructor`) with getters: `getBrokerUrl()`, `getTopicFilter()`, `getEncodingType()`, `getClientId()`, `getCredentialId()`, `getQos()` (`int`), `isCleanStart()` (`boolean`), `getSessionExpiryIntervalSeconds()` (`Long`), `isUseTls()` (`boolean`), `getReceiveTimeoutMs()` (`Long`), `getMaxBatchSize()` (`Integer`), `getReceiveQueueCapacity()` (`Integer`). Constants `DEFAULT_QOS=1`, `DEFAULT_RECEIVE_TIMEOUT_MS=1000L`, `DEFAULT_MAX_BATCH_SIZE=500`, `DEFAULT_RECEIVE_QUEUE_CAPACITY=10000`.

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
package io.fleak.zephflow.lib.commands.mqttsource;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.lib.serdes.EncodingType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public interface MqttSourceDto {

  int DEFAULT_QOS = 1;
  long DEFAULT_RECEIVE_TIMEOUT_MS = 1000L;
  int DEFAULT_MAX_BATCH_SIZE = 500;
  int DEFAULT_RECEIVE_QUEUE_CAPACITY = 10000;

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {
    private String brokerUrl;
    private String topicFilter;
    private EncodingType encodingType;
    private String clientId;
    private String credentialId;

    @Builder.Default private int qos = DEFAULT_QOS;
    @Builder.Default private boolean cleanStart = true;
    private Long sessionExpiryIntervalSeconds;
    @Builder.Default private boolean useTls = false;

    @Builder.Default private Long receiveTimeoutMs = DEFAULT_RECEIVE_TIMEOUT_MS;
    @Builder.Default private Integer maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
    @Builder.Default private Integer receiveQueueCapacity = DEFAULT_RECEIVE_QUEUE_CAPACITY;
  }
}
```

- [ ] **Step 2: Compile**

Run: `./gradlew :lib:compileJava`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 3: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/mqttsource/MqttSourceDto.java
git commit -m "feat: add MqttSourceDto config"
```

---

### Task 3: Config validator (`MqttSourceConfigValidator`)

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/mqttsource/MqttSourceConfigValidator.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/mqttsource/MqttSourceConfigValidatorTest.java`

**Interfaces:**
- Consumes: `MqttSourceDto.Config`, `io.fleak.zephflow.api.ConfigValidator`, `DeserializerFactory.validateEncodingType`.
- Produces: `MqttSourceConfigValidator implements ConfigValidator` with `validateConfig(CommandConfig, String, JobContext)`.

- [ ] **Step 1: Write the failing test**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/mqttsource/MqttSourceConfigValidatorTest.java`:

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
package io.fleak.zephflow.lib.commands.mqttsource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.serdes.EncodingType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MqttSourceConfigValidatorTest {

  private MqttSourceConfigValidator validator;
  private JobContext mockJobContext;

  @BeforeEach
  void setUp() {
    validator = new MqttSourceConfigValidator();
    mockJobContext = mock(JobContext.class);
  }

  private MqttSourceDto.Config.ConfigBuilder validBuilder() {
    return MqttSourceDto.Config.builder()
        .brokerUrl("tcp://localhost:1883")
        .topicFilter("sensors/#")
        .clientId("my-client")
        .encodingType(EncodingType.JSON_OBJECT);
  }

  @Test
  void testValidConfig() {
    assertDoesNotThrow(
        () -> validator.validateConfig(validBuilder().build(), "test-node", mockJobContext));
  }

  @Test
  void testMissingBrokerUrl() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                validator.validateConfig(
                    validBuilder().brokerUrl("").build(), "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("no brokerUrl is provided"));
  }

  @Test
  void testMissingTopicFilter() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                validator.validateConfig(
                    validBuilder().topicFilter("").build(), "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("no topicFilter is provided"));
  }

  @Test
  void testMissingClientId() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                validator.validateConfig(
                    validBuilder().clientId("").build(), "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("no clientId is provided"));
  }

  @Test
  void testMissingEncodingType() {
    NullPointerException ex =
        assertThrows(
            NullPointerException.class,
            () ->
                validator.validateConfig(
                    validBuilder().encodingType(null).build(), "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("no encoding type is provided"));
  }

  @Test
  void testUnsupportedEncodingType() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                validator.validateConfig(
                    validBuilder().encodingType(EncodingType.PARQUET).build(),
                    "test-node",
                    mockJobContext));
    assertTrue(ex.getMessage().contains("Unsupported deserialization encoding type"));
  }

  @Test
  void testInvalidQos() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                validator.validateConfig(
                    validBuilder().qos(3).build(), "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("qos must be 0, 1, or 2"));
  }

  @Test
  void testInvalidMaxBatchSize() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                validator.validateConfig(
                    validBuilder().maxBatchSize(0).build(), "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("maxBatchSize must be positive"));
  }

  @Test
  void testInvalidReceiveQueueCapacity() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                validator.validateConfig(
                    validBuilder().receiveQueueCapacity(0).build(), "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("receiveQueueCapacity must be positive"));
  }

  @Test
  void testInvalidReceiveTimeoutMs() {
    IllegalArgumentException ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                validator.validateConfig(
                    validBuilder().receiveTimeoutMs(0L).build(), "test-node", mockJobContext));
    assertTrue(ex.getMessage().contains("receiveTimeoutMs must be positive"));
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./gradlew :lib:test --tests io.fleak.zephflow.lib.commands.mqttsource.MqttSourceConfigValidatorTest`
Expected: FAIL — compilation error, `MqttSourceConfigValidator` does not exist.

- [ ] **Step 3: Implement the validator**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/mqttsource/MqttSourceConfigValidator.java`:

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
package io.fleak.zephflow.lib.commands.mqttsource;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import org.apache.commons.lang3.StringUtils;

public class MqttSourceConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    MqttSourceDto.Config config = (MqttSourceDto.Config) commandConfig;

    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getBrokerUrl()), "no brokerUrl is provided");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getTopicFilter()), "no topicFilter is provided");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getClientId()), "no clientId is provided");
    Preconditions.checkNotNull(config.getEncodingType(), "no encoding type is provided");
    DeserializerFactory.validateEncodingType(config.getEncodingType());

    Preconditions.checkArgument(
        config.getQos() >= 0 && config.getQos() <= 2, "qos must be 0, 1, or 2, got: %s", config.getQos());

    if (config.getMaxBatchSize() != null) {
      Preconditions.checkArgument(
          config.getMaxBatchSize() > 0,
          "maxBatchSize must be positive, got: %s",
          config.getMaxBatchSize());
    }
    if (config.getReceiveQueueCapacity() != null) {
      Preconditions.checkArgument(
          config.getReceiveQueueCapacity() > 0,
          "receiveQueueCapacity must be positive, got: %s",
          config.getReceiveQueueCapacity());
    }
    if (config.getReceiveTimeoutMs() != null) {
      Preconditions.checkArgument(
          config.getReceiveTimeoutMs() > 0,
          "receiveTimeoutMs must be positive, got: %s",
          config.getReceiveTimeoutMs());
    }
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./gradlew :lib:test --tests io.fleak.zephflow.lib.commands.mqttsource.MqttSourceConfigValidatorTest`
Expected: PASS — all tests green.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/mqttsource/MqttSourceConfigValidator.java lib/src/test/java/io/fleak/zephflow/lib/commands/mqttsource/MqttSourceConfigValidatorTest.java
git commit -m "feat: add MqttSourceConfigValidator"
```

---

### Task 4: Fetcher (`MqttSourceFetcher`)

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/mqttsource/MqttSourceFetcher.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/mqttsource/MqttSourceFetcherTest.java`

**Interfaces:**
- Consumes: `org.eclipse.paho.mqttv5.client.MqttClient`, `java.util.concurrent.BlockingQueue<byte[]>`, `Fetcher<SerializedEvent>`, `NoCommitStrategy`, `SerializedEvent`.
- Produces: `record MqttSourceFetcher(MqttClient mqttClient, BlockingQueue<byte[]> messageQueue, long receiveTimeoutMs, int maxBatchSize) implements Fetcher<SerializedEvent>` with `fetch()`, `commitStrategy()` returning `NoCommitStrategy.INSTANCE`, `close()`, and `static void closeQuietly(MqttClient)`.

- [ ] **Step 1: Write the failing test**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/mqttsource/MqttSourceFetcherTest.java`:

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
package io.fleak.zephflow.lib.commands.mqttsource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.lib.commands.source.CommitStrategy;
import io.fleak.zephflow.lib.commands.source.NoCommitStrategy;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.junit.jupiter.api.Test;

class MqttSourceFetcherTest {

  @Test
  void testFetch_withNoMessages() {
    MqttClient mockClient = mock(MqttClient.class);
    LinkedBlockingQueue<byte[]> queue = new LinkedBlockingQueue<>();

    MqttSourceFetcher fetcher = new MqttSourceFetcher(mockClient, queue, 100L, 500);

    List<SerializedEvent> result = fetcher.fetch();

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  void testFetch_withMessages() {
    MqttClient mockClient = mock(MqttClient.class);
    LinkedBlockingQueue<byte[]> queue = new LinkedBlockingQueue<>();
    byte[] payload = "{\"id\":1}".getBytes();
    queue.offer(payload);

    MqttSourceFetcher fetcher = new MqttSourceFetcher(mockClient, queue, 1000L, 500);

    List<SerializedEvent> result = fetcher.fetch();

    assertEquals(1, result.size());
    assertNull(result.getFirst().key());
    assertArrayEquals(payload, result.getFirst().value());
  }

  @Test
  void testFetch_stopsAtMaxBatchSize() {
    MqttClient mockClient = mock(MqttClient.class);
    LinkedBlockingQueue<byte[]> queue = new LinkedBlockingQueue<>();
    for (int i = 0; i < 10; i++) {
      queue.offer(("{\"id\":" + i + "}").getBytes());
    }

    int maxBatchSize = 3;
    MqttSourceFetcher fetcher = new MqttSourceFetcher(mockClient, queue, 1000L, maxBatchSize);

    List<SerializedEvent> result = fetcher.fetch();

    assertEquals(maxBatchSize, result.size());
  }

  @Test
  void testCommitStrategy_isNone() {
    MqttClient mockClient = mock(MqttClient.class);
    MqttSourceFetcher fetcher =
        new MqttSourceFetcher(mockClient, new LinkedBlockingQueue<>(), 1000L, 500);

    assertSame(NoCommitStrategy.INSTANCE, fetcher.commitStrategy());
    assertEquals(CommitStrategy.CommitMode.NONE, fetcher.commitStrategy().getCommitMode());
  }

  @Test
  void testClose_disconnectsAndClosesClient() throws Exception {
    MqttClient mockClient = mock(MqttClient.class);
    when(mockClient.isConnected()).thenReturn(true);

    MqttSourceFetcher fetcher =
        new MqttSourceFetcher(mockClient, new LinkedBlockingQueue<>(), 1000L, 500);

    fetcher.close();

    verify(mockClient).disconnect();
    verify(mockClient).close();
  }

  @Test
  void testClose_isQuietWhenClientThrows() throws Exception {
    MqttClient mockClient = mock(MqttClient.class);
    when(mockClient.isConnected()).thenReturn(true);
    doThrow(new RuntimeException("disconnect failed")).when(mockClient).disconnect();

    MqttSourceFetcher fetcher =
        new MqttSourceFetcher(mockClient, new LinkedBlockingQueue<>(), 1000L, 500);

    assertDoesNotThrow(fetcher::close);
    verify(mockClient).close();
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./gradlew :lib:test --tests io.fleak.zephflow.lib.commands.mqttsource.MqttSourceFetcherTest`
Expected: FAIL — compilation error, `MqttSourceFetcher` does not exist.

- [ ] **Step 3: Implement the fetcher**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/mqttsource/MqttSourceFetcher.java`:

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
package io.fleak.zephflow.lib.commands.mqttsource;

import io.fleak.zephflow.lib.commands.source.CommitStrategy;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.commands.source.NoCommitStrategy;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.mqttv5.client.MqttClient;

@Slf4j
public record MqttSourceFetcher(
    MqttClient mqttClient,
    BlockingQueue<byte[]> messageQueue,
    long receiveTimeoutMs,
    int maxBatchSize)
    implements Fetcher<SerializedEvent> {

  @Override
  public List<SerializedEvent> fetch() {
    List<SerializedEvent> events = new ArrayList<>();
    try {
      byte[] firstPayload = messageQueue.poll(receiveTimeoutMs, TimeUnit.MILLISECONDS);
      if (firstPayload == null) {
        return events;
      }
      events.add(new SerializedEvent(null, firstPayload, null));

      for (int index = 1; index < maxBatchSize; index++) {
        byte[] nextPayload = messageQueue.poll();
        if (nextPayload == null) {
          break;
        }
        events.add(new SerializedEvent(null, nextPayload, null));
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    log.debug("Fetched {} messages from MQTT", events.size());
    return events;
  }

  @Override
  public CommitStrategy commitStrategy() {
    return NoCommitStrategy.INSTANCE;
  }

  @Override
  public void close() {
    closeQuietly(mqttClient);
  }

  static void closeQuietly(MqttClient client) {
    if (client == null) {
      return;
    }
    try {
      if (client.isConnected()) {
        client.disconnect();
      }
    } catch (Exception e) {
      log.warn("Failed to disconnect MQTT client", e);
    }
    try {
      client.close();
    } catch (Exception e) {
      log.warn("Failed to close MQTT client", e);
    }
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `./gradlew :lib:test --tests io.fleak.zephflow.lib.commands.mqttsource.MqttSourceFetcherTest`
Expected: PASS — all tests green.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/mqttsource/MqttSourceFetcher.java lib/src/test/java/io/fleak/zephflow/lib/commands/mqttsource/MqttSourceFetcherTest.java
git commit -m "feat: add MqttSourceFetcher"
```

---

### Task 5: Client provider (`MqttClientProvider`)

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/mqttsource/MqttClientProvider.java`

**Interfaces:**
- Consumes: `MqttSourceDto.Config`, `io.fleak.zephflow.lib.credentials.UsernamePasswordCredential`, Paho `MqttClient`, `MqttConnectionOptions`, `MqttConnectionOptionsBuilder`.
- Produces:
  - `MqttClient createClient(MqttSourceDto.Config config)` — constructs a `MqttClient` with a `MemoryPersistence`, using `config.getBrokerUrl()` and `config.getClientId()`.
  - `MqttConnectionOptions buildConnectionOptions(MqttSourceDto.Config config, UsernamePasswordCredential credential)` — builds options from `cleanStart`, `sessionExpiryIntervalSeconds`, `useTls`, and optional username/password.

- [ ] **Step 1: Create the provider**

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
package io.fleak.zephflow.lib.commands.mqttsource;

import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import java.nio.charset.StandardCharsets;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptionsBuilder;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;

public class MqttClientProvider {

  public MqttClient createClient(MqttSourceDto.Config config) throws MqttException {
    return new MqttClient(config.getBrokerUrl(), config.getClientId(), new MemoryPersistence());
  }

  public MqttConnectionOptions buildConnectionOptions(
      MqttSourceDto.Config config, UsernamePasswordCredential credential) {
    MqttConnectionOptionsBuilder optionsBuilder =
        new MqttConnectionOptionsBuilder().cleanStart(config.isCleanStart());

    if (config.getSessionExpiryIntervalSeconds() != null) {
      optionsBuilder.sessionExpiryInterval(config.getSessionExpiryIntervalSeconds());
    }

    if (credential != null) {
      optionsBuilder.username(credential.getUsername());
      optionsBuilder.password(credential.getPassword().getBytes(StandardCharsets.UTF_8));
    }

    return optionsBuilder.build();
  }
}
```

- [ ] **Step 2: Compile**

Run: `./gradlew :lib:compileJava`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 3: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/mqttsource/MqttClientProvider.java
git commit -m "feat: add MqttClientProvider"
```

---

### Task 6: Command (`MqttSourceCommand`) and registration

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/mqttsource/MqttSourceCommand.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/mqttsource/MqttSourceCommandFactory.java`
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java` (after line 94, the `COMMAND_NAME_ACTIVEMQ_SOURCE` declaration)
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java` (after line 84, the `COMMAND_NAME_ACTIVEMQ_SOURCE` registration)
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/mqttsource/MqttSourceCommandTest.java`

**Interfaces:**
- Consumes: `MqttSourceDto.Config`, `MqttClientProvider`, `MqttSourceFetcher`, `SimpleSourceCommand`, `SourceExecutionContext`, `BytesRawDataEncoder`, `BytesRawDataConverter`, `DeserializerFactory`, `lookupUsernamePasswordCredential` from `MiscUtils`, `DlqWriterFactory`.
- Produces:
  - Constant `COMMAND_NAME_MQTT_SOURCE = "mqttsource"` in `MiscUtils`.
  - `MqttSourceCommand extends SimpleSourceCommand<SerializedEvent>` with constructor `(String nodeId, JobContext jobContext, ConfigParser configParser, ConfigValidator configValidator, MqttClientProvider mqttClientProvider)`; `commandName()` returns `COMMAND_NAME_MQTT_SOURCE`; `sourceType()` returns `SourceType.STREAMING`.
  - `MqttSourceCommandFactory extends SourceCommandFactory` with `createCommand(String, JobContext)`.
  - Registry entry mapping `COMMAND_NAME_MQTT_SOURCE` to `new MqttSourceCommandFactory()`.

- [ ] **Step 1: Add the command name constant**

In `lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java`, immediately after the line `String COMMAND_NAME_ACTIVEMQ_SOURCE = "activemqsource";`, add:

```java
  String COMMAND_NAME_MQTT_SOURCE = "mqttsource";
```

- [ ] **Step 2: Create the command**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/mqttsource/MqttSourceCommand.java`:

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
package io.fleak.zephflow.lib.commands.mqttsource;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.eclipse.paho.mqttv5.client.MqttCallback;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.eclipse.paho.mqttv5.client.MqttDisconnectResponse;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

@Slf4j
public class MqttSourceCommand extends SimpleSourceCommand<SerializedEvent> {

  private final MqttClientProvider mqttClientProvider;

  public MqttSourceCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      MqttClientProvider mqttClientProvider) {
    super(nodeId, jobContext, configParser, configValidator);
    this.mqttClientProvider = mqttClientProvider;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    MqttSourceDto.Config config = (MqttSourceDto.Config) commandConfig;

    Fetcher<SerializedEvent> fetcher = createMqttFetcher(config, jobContext);
    RawDataEncoder<SerializedEvent> encoder = new BytesRawDataEncoder();
    RawDataConverter<SerializedEvent> converter = createRawDataConverter(config);

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

  private Fetcher<SerializedEvent> createMqttFetcher(
      MqttSourceDto.Config config, JobContext jobContext) {
    MqttClient mqttClient = null;
    try {
      mqttClient = mqttClientProvider.createClient(config);

      UsernamePasswordCredential credential = null;
      if (StringUtils.isNotBlank(config.getCredentialId())) {
        credential = lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
      }
      MqttConnectionOptions connectionOptions =
          mqttClientProvider.buildConnectionOptions(config, credential);

      BlockingQueue<byte[]> messageQueue =
          new LinkedBlockingQueue<>(config.getReceiveQueueCapacity());
      mqttClient.setCallback(new EnqueueingCallback(messageQueue));
      mqttClient.connect(connectionOptions);
      mqttClient.subscribe(config.getTopicFilter(), config.getQos());

      return new MqttSourceFetcher(
          mqttClient, messageQueue, config.getReceiveTimeoutMs(), config.getMaxBatchSize());
    } catch (Exception e) {
      MqttSourceFetcher.closeQuietly(mqttClient);
      throw new RuntimeException("Failed to create MQTT fetcher", e);
    }
  }

  private RawDataConverter<SerializedEvent> createRawDataConverter(MqttSourceDto.Config config) {
    FleakDeserializer<?> deserializer =
        DeserializerFactory.createDeserializerFactory(config.getEncodingType())
            .createDeserializer();
    return new BytesRawDataConverter(deserializer);
  }

  @Override
  public SourceType sourceType() {
    return SourceType.STREAMING;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_MQTT_SOURCE;
  }

  private record EnqueueingCallback(BlockingQueue<byte[]> messageQueue) implements MqttCallback {

    @Override
    public void messageArrived(String topic, MqttMessage message) {
      if (!messageQueue.offer(message.getPayload())) {
        log.warn("MQTT receive queue is full, dropping message from topic {}", topic);
      }
    }

    @Override
    public void disconnected(MqttDisconnectResponse disconnectResponse) {
      log.warn("MQTT client disconnected: {}", disconnectResponse.getReasonString());
    }

    @Override
    public void mqttErrorOccurred(MqttException exception) {
      log.error("MQTT error occurred", exception);
    }

    @Override
    public void deliveryComplete(org.eclipse.paho.mqttv5.client.IMqttToken token) {}

    @Override
    public void connectComplete(boolean reconnect, String serverUri) {}

    @Override
    public void authPacketArrived(int reasonCode, MqttProperties properties) {}
  }
}
```

- [ ] **Step 3: Create the factory**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/mqttsource/MqttSourceCommandFactory.java`:

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
package io.fleak.zephflow.lib.commands.mqttsource;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.JsonConfigParser;
import io.fleak.zephflow.lib.commands.source.SourceCommandFactory;

public class MqttSourceCommandFactory extends SourceCommandFactory {

  @Override
  public MqttSourceCommand createCommand(String nodeId, JobContext jobContext) {
    JsonConfigParser<MqttSourceDto.Config> configParser =
        new JsonConfigParser<>(MqttSourceDto.Config.class);
    MqttSourceConfigValidator validator = new MqttSourceConfigValidator();
    MqttClientProvider mqttClientProvider = new MqttClientProvider();
    return new MqttSourceCommand(nodeId, jobContext, configParser, validator, mqttClientProvider);
  }
}
```

- [ ] **Step 4: Register the factory**

In `lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java`, immediately after the line `.put(COMMAND_NAME_ACTIVEMQ_SOURCE, new ActiveMqSourceCommandFactory())`, add:

```java
          .put(COMMAND_NAME_MQTT_SOURCE, new MqttSourceCommandFactory())
```

Confirm the import is resolvable: the registry uses `import static ...MiscUtils.*` and `import io.fleak.zephflow.lib.commands.mqttsource.MqttSourceCommandFactory;` — add the latter import alongside the existing source-factory imports if the file uses explicit imports. Check the existing `ActiveMqSourceCommandFactory` import style at the top of the file and match it.

- [ ] **Step 5: Write the command unit test**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/mqttsource/MqttSourceCommandTest.java`:

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
package io.fleak.zephflow.lib.commands.mqttsource;

import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_MQTT_SOURCE;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceCommand;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.util.concurrent.LinkedBlockingQueue;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.MqttConnectionOptions;
import org.junit.jupiter.api.Test;

class MqttSourceCommandTest {

  @Test
  void testCommandMetadata() {
    MqttSourceCommandFactory factory = new MqttSourceCommandFactory();
    MqttSourceCommand command = factory.createCommand("my_node", TestUtils.JOB_CONTEXT);

    assertEquals(COMMAND_NAME_MQTT_SOURCE, command.commandName());
    assertEquals(SourceCommand.SourceType.STREAMING, command.sourceType());
  }

  @Test
  void testCreateExecutionContext_wiresFetcherAndConverter() throws Exception {
    MqttClientProvider mockProvider = mock(MqttClientProvider.class);
    MqttClient mockClient = mock(MqttClient.class);
    when(mockProvider.createClient(any())).thenReturn(mockClient);
    when(mockProvider.buildConnectionOptions(any(), any()))
        .thenReturn(new MqttConnectionOptions());

    MqttSourceConfigValidator validator = new MqttSourceConfigValidator();
    MqttSourceCommand command =
        new MqttSourceCommand(
            "my_node",
            TestUtils.JOB_CONTEXT,
            new io.fleak.zephflow.lib.commands.JsonConfigParser<>(MqttSourceDto.Config.class),
            validator,
            mockProvider);

    MqttSourceDto.Config config =
        MqttSourceDto.Config.builder()
            .brokerUrl("tcp://localhost:1883")
            .topicFilter("sensors/#")
            .clientId("my-client")
            .encodingType(EncodingType.JSON_OBJECT)
            .build();

    SourceExecutionContext<SerializedEvent> context =
        (SourceExecutionContext<SerializedEvent>)
            command.createExecutionContext(
                new MetricClientProvider.NoopMetricClientProvider(),
                TestUtils.JOB_CONTEXT,
                config,
                "my_node");

    assertNotNull(context.fetcher());
    assertInstanceOf(MqttSourceFetcher.class, context.fetcher());
    assertNotNull(context.converter());
    assertNotNull(context.encoder());

    verify(mockClient).connect(any(MqttConnectionOptions.class));
    verify(mockClient).subscribe("sensors/#", 1);
    verify(mockClient).setCallback(any());
  }
}
```

Note: `createExecutionContext` is `protected`; the test is in the same package as the command, so it has access.

- [ ] **Step 6: Run the new tests to verify they pass**

Run: `./gradlew :lib:test --tests io.fleak.zephflow.lib.commands.mqttsource.MqttSourceCommandTest`
Expected: PASS — both tests green.

- [ ] **Step 7: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/mqttsource/MqttSourceCommand.java lib/src/main/java/io/fleak/zephflow/lib/commands/mqttsource/MqttSourceCommandFactory.java lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java lib/src/test/java/io/fleak/zephflow/lib/commands/mqttsource/MqttSourceCommandTest.java
git commit -m "feat: add MqttSourceCommand and register mqttsource"
```

---

### Task 7: Full verification

**Files:** none (verification only).

**Interfaces:**
- Consumes: all prior tasks.
- Produces: confirmation the module builds, all `mqttsource` tests pass, formatting is clean.

- [ ] **Step 1: Apply code formatting**

Run: `./gradlew spotlessApply`
Expected: BUILD SUCCESSFUL (the project uses spotless/google-java-format; this normalizes the new files).

If `spotlessApply` is not a task, skip this step — the source above already follows google-java-format conventions.

- [ ] **Step 2: Run the full mqttsource test package**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.mqttsource.*"`
Expected: PASS — all three test classes green.

- [ ] **Step 3: Build the lib module**

Run: `./gradlew :lib:build -x test`
Expected: BUILD SUCCESSFUL.

- [ ] **Step 4: Commit any formatting changes**

```bash
git add -A
git commit -m "chore: apply formatting to mqttsource" || echo "nothing to commit"
```

---

## Self-Review

**Spec coverage:**
- EncodingType + validation → Tasks 2, 3, 6 ✓
- Paho v5 library → Task 1 ✓
- Auto-ack / NONE commit → Task 4 (`commitStrategy()` returns `NoCommitStrategy.INSTANCE`, no committer) ✓
- Internal blocking queue receive model → Tasks 4, 6 (`LinkedBlockingQueue`, `EnqueueingCallback`, drain loop) ✓
- Config surface (brokerUrl, topicFilter, encodingType, clientId, credentialId, qos, cleanStart, sessionExpiryIntervalSeconds, useTls, receiveTimeoutMs, maxBatchSize, receiveQueueCapacity) → Task 2 ✓
- Credential lookup → Task 6 (`lookupUsernamePasswordCredential`) ✓
- Components: MqttSourceDto, MqttClientProvider, MqttSourceFetcher, MqttSourceConfigValidator, MqttSourceCommand, MqttSourceCommandFactory → Tasks 2–6 ✓
- Registration in MiscUtils + OperatorCommandRegistry → Task 6 ✓
- Full queue → log.warn back-pressure → Task 6 (`EnqueueingCallback`) ✓
- Error handling on client construction → Task 6 (`createMqttFetcher` catch + `closeQuietly`) ✓
- Tests mirroring activemqsource (validator, fetcher, command) → Tasks 3, 4, 6 ✓
- No comments / full names / readable methods → Global Constraints, enforced in every code block ✓

**Placeholder scan:** No TBD/TODO/"similar to"/"add error handling" — every code step contains complete source. The one conditional instruction (Task 6 Step 4 import style) names the exact action and reference file. ✓

**Type consistency:** `MqttSourceFetcher` record signature `(MqttClient, BlockingQueue<byte[]>, long, int)` is identical in Task 4 production, Task 4 tests, and Task 6 command. `MqttClientProvider.createClient`/`buildConnectionOptions` signatures match between Task 5 and Tasks 6 usage/tests. `COMMAND_NAME_MQTT_SOURCE` consistent across MiscUtils, command, registry, test. Config getters (`isCleanStart`, `isUseTls`, `getQos`) match Lombok generation for the field types in Task 2. ✓

**Note on TLS:** `useTls` is carried on the config and validated, but Paho derives the transport from the `ssl://` URL scheme; `buildConnectionOptions` does not set explicit socket factories (keystore config is out of scope per the spec). This is intentional and matches the design's "rely on the broker URL scheme" decision.
