# Google Pub/Sub Source and Sink Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `pubsubsource` (synchronous-pull streaming source) and `pubsubsink` (atomic batch publish sink) operator commands to zephflow-core, mirroring the existing SQS connectors and reusing the existing `GcpCredential` model.

**Architecture:** New packages `lib/.../commands/pubsubsource/` and `lib/.../commands/pubsubsink/` parallel to `sqssource/` and `sqssink/`, plus `lib/.../gcp/PubSubClientFactory` parallel to `GcsClientFactory`. Source uses `SubscriberStub.pullCallable()` with `PerRecordCommitStrategy`; sink uses `PublisherStub.publishCallable()` with all-or-nothing batch semantics. New command names registered in `MiscUtils` and `OperatorCommandRegistry`.

**Tech Stack:** Java 21, Gradle (Kotlin/Groovy hybrid), `com.google.cloud:google-cloud-pubsub` (synchronous low-level stubs), JUnit 5, Mockito, Lombok.

**Reference spec:** `docs/superpowers/specs/2026-05-01-pubsub-source-sink-design.md`

---

## File Structure

**New files (production):**
- `lib/src/main/java/io/fleak/zephflow/lib/gcp/PubSubClientFactory.java`
- `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceDto.java`
- `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceConfigValidator.java`
- `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubReceivedMessage.java`
- `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubRawDataEncoder.java`
- `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubRawDataConverter.java`
- `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceFetcher.java`
- `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceCommand.java`
- `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceCommandFactory.java`
- `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkDto.java`
- `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkConfigValidator.java`
- `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubOutboundMessage.java`
- `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkMessageProcessor.java`
- `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkFlusher.java`
- `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkCommand.java`
- `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkCommandFactory.java`

**New files (tests):**
- `lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceDtoTest.java`
- `lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceConfigValidatorTest.java`
- `lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceFetcherTest.java`
- `lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceFetcherCommitTest.java`
- `lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkDtoTest.java`
- `lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkConfigValidatorTest.java`
- `lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkMessageProcessorTest.java`
- `lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkFlusherTest.java`

**Modified files:**
- `gradle/libs.versions.toml` (add `pubsub` version + `gcs-pubsub` library)
- `lib/build.gradle` (add `implementation libs.gcs.pubsub`)
- `lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java` (add 2 command-name constants)
- `lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java` (register 2 factories)

**License header for every new Java file:**
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
```

---

## Task 1: Add `google-cloud-pubsub` dependency

**Files:**
- Modify: `gradle/libs.versions.toml`
- Modify: `lib/build.gradle`

- [ ] **Step 1: Add version entry**

In `gradle/libs.versions.toml`, in the `[versions]` block, add a new line directly **after** the existing `gcs = "2.49.0"` line:

```toml
pubsub = "1.131.0"
```

- [ ] **Step 2: Add library entry**

In `gradle/libs.versions.toml`, in the `[libraries]` block, add a new line directly **after** the existing `gcs-storage = { module = "com.google.cloud:google-cloud-storage", version.ref = "gcs" }` line:

```toml
gcs-pubsub = { module = "com.google.cloud:google-cloud-pubsub", version.ref = "pubsub" }
```

- [ ] **Step 3: Reference the dependency in the lib build**

In `lib/build.gradle`, find the existing line `implementation libs.gcs.storage` and add immediately after it:

```groovy
  implementation libs.gcs.pubsub
```

- [ ] **Step 4: Verify dependency resolves**

Run from the repo root:

```
./gradlew :lib:dependencies --configuration runtimeClasspath | grep google-cloud-pubsub
```

Expected: a line like `+--- com.google.cloud:google-cloud-pubsub:1.131.0` appears in the output.

- [ ] **Step 5: Commit**

```bash
git add gradle/libs.versions.toml lib/build.gradle
git commit -m "Add google-cloud-pubsub dependency"
```

---

## Task 2: Create `PubSubClientFactory`

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/gcp/PubSubClientFactory.java`

This factory mirrors `GcsClientFactory` exactly: same constructor pattern, same `resolveCredentials(...)` switch over `GcpCredential.AuthType`. No unit test (parallel to `GcsClientFactory`, which has none).

- [ ] **Step 1: Write the file**

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
package io.fleak.zephflow.lib.gcp;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.stub.GrpcPublisherStub;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.PublisherStub;
import com.google.cloud.pubsub.v1.stub.PublisherStubSettings;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import io.fleak.zephflow.lib.credentials.GcpCredential;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PubSubClientFactory implements Serializable {

  public SubscriberStub createSubscriberStub() {
    try {
      GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
      SubscriberStubSettings settings =
          SubscriberStubSettings.newBuilder()
              .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
              .build();
      return GrpcSubscriberStub.create(settings);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to create Pub/Sub subscriber stub from Application Default Credentials", e);
    }
  }

  public SubscriberStub createSubscriberStub(GcpCredential credential) {
    try {
      GoogleCredentials credentials = resolveCredentials(credential);
      SubscriberStubSettings settings =
          SubscriberStubSettings.newBuilder()
              .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
              .build();
      return GrpcSubscriberStub.create(settings);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create Pub/Sub subscriber stub", e);
    }
  }

  public PublisherStub createPublisherStub() {
    try {
      GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
      PublisherStubSettings settings =
          PublisherStubSettings.newBuilder()
              .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
              .build();
      return GrpcPublisherStub.create(settings);
    } catch (IOException e) {
      throw new RuntimeException(
          "Failed to create Pub/Sub publisher stub from Application Default Credentials", e);
    }
  }

  public PublisherStub createPublisherStub(GcpCredential credential) {
    try {
      GoogleCredentials credentials = resolveCredentials(credential);
      PublisherStubSettings settings =
          PublisherStubSettings.newBuilder()
              .setCredentialsProvider(FixedCredentialsProvider.create(credentials))
              .build();
      return GrpcPublisherStub.create(settings);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create Pub/Sub publisher stub", e);
    }
  }

  private GoogleCredentials resolveCredentials(GcpCredential credential) throws IOException {
    return switch (credential.getAuthType()) {
      case SERVICE_ACCOUNT_JSON_KEYFILE -> {
        log.info("Creating Pub/Sub client using service account JSON keyfile");
        yield ServiceAccountCredentials.fromStream(
            new ByteArrayInputStream(
                credential.getJsonKeyContent().getBytes(StandardCharsets.UTF_8)));
      }
      case ACCESS_TOKEN -> {
        log.info("Creating Pub/Sub client using OAuth access token");
        yield GoogleCredentials.create(new AccessToken(credential.getAccessToken(), null));
      }
      case APPLICATION_DEFAULT -> {
        log.info("Creating Pub/Sub client using Application Default Credentials");
        yield GoogleCredentials.getApplicationDefault();
      }
    };
  }
}
```

- [ ] **Step 2: Verify it compiles**

Run from the repo root:

```
./gradlew :lib:compileJava
```

Expected: BUILD SUCCESSFUL with no errors. (Imports for `com.google.cloud.pubsub.v1.stub.*` and `com.google.api.gax.core.FixedCredentialsProvider` resolve from the dependency added in Task 1.)

- [ ] **Step 3: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/gcp/PubSubClientFactory.java
git commit -m "Add PubSubClientFactory for SubscriberStub and PublisherStub"
```

---

## Task 3: Source DTO + JSON round-trip test

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceDto.java`
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceDtoTest.java`

- [ ] **Step 1: Write the failing test**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceDtoTest.java`:

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
package io.fleak.zephflow.lib.commands.pubsubsource;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.serdes.EncodingType;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PubSubSourceDtoTest {

  @Test
  void testConfigBuilder() {
    PubSubSourceDto.Config config =
        PubSubSourceDto.Config.builder()
            .projectId("my-project")
            .subscription("my-sub")
            .encodingType(EncodingType.JSON_OBJECT)
            .credentialId("my-cred")
            .maxMessages(50)
            .returnImmediately(true)
            .ackDeadlineExtensionSeconds(120)
            .build();

    assertEquals("my-project", config.getProjectId());
    assertEquals("my-sub", config.getSubscription());
    assertEquals(EncodingType.JSON_OBJECT, config.getEncodingType());
    assertEquals("my-cred", config.getCredentialId());
    assertEquals(50, config.getMaxMessages());
    assertTrue(config.getReturnImmediately());
    assertEquals(120, config.getAckDeadlineExtensionSeconds());
  }

  @Test
  void testDefaultValues() {
    PubSubSourceDto.Config config =
        PubSubSourceDto.Config.builder()
            .subscription("my-sub")
            .encodingType(EncodingType.JSON_OBJECT)
            .build();

    assertEquals(PubSubSourceDto.DEFAULT_MAX_MESSAGES, config.getMaxMessages());
    assertFalse(config.getReturnImmediately());
    assertNull(config.getCredentialId());
    assertNull(config.getProjectId());
    assertNull(config.getAckDeadlineExtensionSeconds());
  }

  @Test
  void testJsonParsing() {
    Map<String, Object> jsonMap =
        Map.of(
            "projectId", "p",
            "subscription", "s",
            "encodingType", "JSON_OBJECT",
            "maxMessages", 200,
            "returnImmediately", false,
            "ackDeadlineExtensionSeconds", 60);

    PubSubSourceDto.Config config =
        OBJECT_MAPPER.convertValue(jsonMap, PubSubSourceDto.Config.class);

    assertEquals("p", config.getProjectId());
    assertEquals("s", config.getSubscription());
    assertEquals(EncodingType.JSON_OBJECT, config.getEncodingType());
    assertEquals(200, config.getMaxMessages());
    assertFalse(config.getReturnImmediately());
    assertEquals(60, config.getAckDeadlineExtensionSeconds());
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

```
./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.pubsubsource.PubSubSourceDtoTest'
```

Expected: FAIL with compilation error — `PubSubSourceDto` cannot be resolved.

- [ ] **Step 3: Write the DTO**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceDto.java`:

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
package io.fleak.zephflow.lib.commands.pubsubsource;

import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.lib.serdes.EncodingType;
import lombok.*;

public interface PubSubSourceDto {

  int DEFAULT_MAX_MESSAGES = 100;
  int MAX_MAX_MESSAGES = 1000;
  int MAX_ACK_DEADLINE_EXTENSION_SECONDS = 600;

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {
    private String projectId;
    @NonNull private String subscription;
    @NonNull private EncodingType encodingType;
    private String credentialId;
    @Builder.Default private Integer maxMessages = DEFAULT_MAX_MESSAGES;
    @Builder.Default private Boolean returnImmediately = false;
    private Integer ackDeadlineExtensionSeconds;
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

```
./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.pubsubsource.PubSubSourceDtoTest'
```

Expected: 3 tests, all PASS.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceDto.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceDtoTest.java
git commit -m "Add PubSubSourceDto with config defaults and JSON round-trip test"
```

---

## Task 4: Source config validator + tests

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceConfigValidator.java`
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceConfigValidatorTest.java`

- [ ] **Step 1: Write the failing tests**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceConfigValidatorTest.java`:

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
package io.fleak.zephflow.lib.commands.pubsubsource;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.credentials.GcpCredential;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PubSubSourceConfigValidatorTest {

  static final JobContext TEST_JOB_CONTEXT =
      JobContext.builder()
          .metricTags(TestUtils.JOB_CONTEXT.getMetricTags())
          .otherProperties(
              new HashMap<>(
                  Map.of(
                      "example-credential-id",
                      new HashMap<>(
                          Map.of(
                              "authType",
                              GcpCredential.AuthType.APPLICATION_DEFAULT.name(),
                              "projectId",
                              "test-project")))))
          .build();

  @Test
  void validateConfig_validConfig() {
    PubSubSourceConfigValidator validator = new PubSubSourceConfigValidator();
    PubSubSourceDto.Config config =
        PubSubSourceDto.Config.builder()
            .projectId("p")
            .subscription("my-sub")
            .encodingType(EncodingType.JSON_OBJECT)
            .credentialId("example-credential-id")
            .build();
    assertDoesNotThrow(() -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_blankSubscription() {
    PubSubSourceConfigValidator validator = new PubSubSourceConfigValidator();
    PubSubSourceDto.Config config = new PubSubSourceDto.Config();
    config.setSubscription("");
    config.setEncodingType(EncodingType.JSON_OBJECT);
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_invalidMaxMessagesAboveCap() {
    PubSubSourceConfigValidator validator = new PubSubSourceConfigValidator();
    PubSubSourceDto.Config config =
        PubSubSourceDto.Config.builder()
            .subscription("s")
            .encodingType(EncodingType.JSON_OBJECT)
            .maxMessages(1001)
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_zeroMaxMessages() {
    PubSubSourceConfigValidator validator = new PubSubSourceConfigValidator();
    PubSubSourceDto.Config config =
        PubSubSourceDto.Config.builder()
            .subscription("s")
            .encodingType(EncodingType.JSON_OBJECT)
            .maxMessages(0)
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_negativeAckDeadlineExtension() {
    PubSubSourceConfigValidator validator = new PubSubSourceConfigValidator();
    PubSubSourceDto.Config config =
        PubSubSourceDto.Config.builder()
            .subscription("s")
            .encodingType(EncodingType.JSON_OBJECT)
            .ackDeadlineExtensionSeconds(-1)
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_ackDeadlineExtensionAboveCap() {
    PubSubSourceConfigValidator validator = new PubSubSourceConfigValidator();
    PubSubSourceDto.Config config =
        PubSubSourceDto.Config.builder()
            .subscription("s")
            .encodingType(EncodingType.JSON_OBJECT)
            .ackDeadlineExtensionSeconds(601)
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_validBoundaryValues() {
    PubSubSourceConfigValidator validator = new PubSubSourceConfigValidator();
    PubSubSourceDto.Config config =
        PubSubSourceDto.Config.builder()
            .subscription("s")
            .encodingType(EncodingType.JSON_OBJECT)
            .maxMessages(1000)
            .ackDeadlineExtensionSeconds(0)
            .build();
    assertDoesNotThrow(() -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

```
./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.pubsubsource.PubSubSourceConfigValidatorTest'
```

Expected: FAIL with compilation error — `PubSubSourceConfigValidator` cannot be resolved.

- [ ] **Step 3: Write the validator**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceConfigValidator.java`:

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
package io.fleak.zephflow.lib.commands.pubsubsource;

import static io.fleak.zephflow.lib.utils.MiscUtils.enforceCredentials;
import static io.fleak.zephflow.lib.utils.MiscUtils.lookupGcpCredential;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import org.apache.commons.lang3.StringUtils;

public class PubSubSourceConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    PubSubSourceDto.Config config = (PubSubSourceDto.Config) commandConfig;
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getSubscription()), "no subscription is provided");
    Preconditions.checkNotNull(config.getEncodingType(), "no encoding type is provided");
    DeserializerFactory.validateEncodingType(config.getEncodingType());

    if (config.getMaxMessages() != null) {
      Preconditions.checkArgument(
          config.getMaxMessages() >= 1
              && config.getMaxMessages() <= PubSubSourceDto.MAX_MAX_MESSAGES,
          "maxMessages must be between 1 and %s",
          PubSubSourceDto.MAX_MAX_MESSAGES);
    }

    if (config.getAckDeadlineExtensionSeconds() != null) {
      Preconditions.checkArgument(
          config.getAckDeadlineExtensionSeconds() >= 0
              && config.getAckDeadlineExtensionSeconds()
                  <= PubSubSourceDto.MAX_ACK_DEADLINE_EXTENSION_SECONDS,
          "ackDeadlineExtensionSeconds must be between 0 and %s",
          PubSubSourceDto.MAX_ACK_DEADLINE_EXTENSION_SECONDS);
    }

    if (StringUtils.trimToNull(config.getCredentialId()) != null
        && enforceCredentials(jobContext)) {
      lookupGcpCredential(jobContext, config.getCredentialId());
    }
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

```
./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.pubsubsource.PubSubSourceConfigValidatorTest'
```

Expected: 7 tests, all PASS.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceConfigValidator.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceConfigValidatorTest.java
git commit -m "Add PubSubSourceConfigValidator with range and credential checks"
```

---

## Task 5: Source raw data types and converter

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubReceivedMessage.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubRawDataEncoder.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubRawDataConverter.java`

These are tiny adapters with no dedicated tests (parallel to the Sqs equivalents, which also have none — they're exercised indirectly through fetcher/command tests).

- [ ] **Step 1: Write `PubSubReceivedMessage`**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubReceivedMessage.java`:

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
package io.fleak.zephflow.lib.commands.pubsubsource;

import java.util.Map;

public record PubSubReceivedMessage(
    byte[] body, String messageId, String ackId, Map<String, String> attributes) {}
```

- [ ] **Step 2: Write `PubSubRawDataEncoder`**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubRawDataEncoder.java`:

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
package io.fleak.zephflow.lib.commands.pubsubsource;

import io.fleak.zephflow.lib.commands.source.RawDataEncoder;
import io.fleak.zephflow.lib.serdes.SerializedEvent;

public class PubSubRawDataEncoder implements RawDataEncoder<PubSubReceivedMessage> {
  @Override
  public SerializedEvent serialize(PubSubReceivedMessage sourceRecord) {
    return new SerializedEvent(null, sourceRecord.body(), sourceRecord.attributes());
  }
}
```

- [ ] **Step 3: Write `PubSubRawDataConverter`**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubRawDataConverter.java`:

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
package io.fleak.zephflow.lib.commands.pubsubsource;

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;
import static io.fleak.zephflow.lib.utils.MiscUtils.getCallingUserTagAndEventTags;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.source.ConvertedResult;
import io.fleak.zephflow.lib.commands.source.RawDataConverter;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PubSubRawDataConverter implements RawDataConverter<PubSubReceivedMessage> {

  private final FleakDeserializer<?> fleakDeserializer;

  public PubSubRawDataConverter(FleakDeserializer<?> fleakDeserializer) {
    this.fleakDeserializer = fleakDeserializer;
  }

  @Override
  public ConvertedResult<PubSubReceivedMessage> convert(
      PubSubReceivedMessage sourceRecord, SourceExecutionContext<?> sourceInitializedConfig) {
    try {
      SerializedEvent serializedEvent =
          new SerializedEvent(null, sourceRecord.body(), sourceRecord.attributes());
      List<RecordFleakData> events = fleakDeserializer.deserialize(serializedEvent);

      Map<String, String> eventTags =
          getCallingUserTagAndEventTags(null, events.isEmpty() ? null : events.getFirst());

      sourceInitializedConfig.dataSizeCounter().increase(sourceRecord.body().length, eventTags);
      sourceInitializedConfig.inputEventCounter().increase(events.size(), eventTags);
      if (log.isDebugEnabled()) {
        events.forEach(e -> log.debug("got message: {}", toJsonString(e)));
      }
      return ConvertedResult.success(events, sourceRecord);
    } catch (Exception e) {
      sourceInitializedConfig.dataSizeCounter().increase(sourceRecord.body().length, Map.of());
      sourceInitializedConfig.deserializeFailureCounter().increase(Map.of());
      log.error("failed to deserialize Pub/Sub message: {}", sourceRecord.messageId());
      return ConvertedResult.failure(e, sourceRecord);
    }
  }
}
```

- [ ] **Step 4: Verify compile**

```
./gradlew :lib:compileJava
```

Expected: BUILD SUCCESSFUL.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubReceivedMessage.java \
        lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubRawDataEncoder.java \
        lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubRawDataConverter.java
git commit -m "Add PubSub source raw data types, encoder, and converter"
```

---

## Task 6: Source fetcher — fetch behavior + tests

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceFetcher.java`
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceFetcherTest.java`

The fetcher invokes `subscriberStub.pullCallable().call(PullRequest)` and (when extension is configured) `modifyAckDeadlineCallable().call(...)`. To test, mock the stub and have each `*Callable()` getter return a mocked `UnaryCallable`.

- [ ] **Step 1: Write the failing tests**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceFetcherTest.java`:

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
package io.fleak.zephflow.lib.commands.pubsubsource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PubSubSourceFetcherTest {

  private static final String SUBSCRIPTION_PATH = "projects/p/subscriptions/s";

  private SubscriberStub stub;
  private UnaryCallable<PullRequest, PullResponse> pullCallable;
  private UnaryCallable<ModifyAckDeadlineRequest, Empty> modAckCallable;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    stub = mock(SubscriberStub.class);
    pullCallable = (UnaryCallable<PullRequest, PullResponse>) mock(UnaryCallable.class);
    modAckCallable = (UnaryCallable<ModifyAckDeadlineRequest, Empty>) mock(UnaryCallable.class);
    when(stub.pullCallable()).thenReturn(pullCallable);
    when(stub.modifyAckDeadlineCallable()).thenReturn(modAckCallable);
  }

  private static ReceivedMessage receivedMessage(
      String ackId, String messageId, String body, Map<String, String> attrs, String orderingKey) {
    PubsubMessage.Builder mb =
        PubsubMessage.newBuilder()
            .setMessageId(messageId)
            .setData(ByteString.copyFromUtf8(body))
            .putAllAttributes(attrs);
    if (orderingKey != null) {
      mb.setOrderingKey(orderingKey);
    }
    return ReceivedMessage.newBuilder().setAckId(ackId).setMessage(mb.build()).build();
  }

  @Test
  void testFetchReturnsMessages() {
    PubSubSourceFetcher fetcher =
        new PubSubSourceFetcher(stub, SUBSCRIPTION_PATH, 100, false, 0);

    PullResponse response =
        PullResponse.newBuilder()
            .addReceivedMessages(receivedMessage("a-1", "m-1", "{\"k\":1}", Map.of(), null))
            .addReceivedMessages(receivedMessage("a-2", "m-2", "{\"k\":2}", Map.of(), null))
            .build();
    when(pullCallable.call(any(PullRequest.class))).thenReturn(response);

    List<PubSubReceivedMessage> result = fetcher.fetch();

    assertEquals(2, result.size());
    assertEquals("m-1", result.get(0).messageId());
    assertEquals("a-1", result.get(0).ackId());
    assertArrayEquals("{\"k\":1}".getBytes(), result.get(0).body());
    assertEquals("m-1", result.get(0).attributes().get("messageId"));

    verify(pullCallable)
        .call(
            argThat(
                (PullRequest req) -> {
                  assertEquals(SUBSCRIPTION_PATH, req.getSubscription());
                  assertEquals(100, req.getMaxMessages());
                  assertFalse(req.getReturnImmediately());
                  return true;
                }));
    verify(modAckCallable, never()).call(any());
  }

  @Test
  void testFetchPropagatesAttributes() {
    PubSubSourceFetcher fetcher =
        new PubSubSourceFetcher(stub, SUBSCRIPTION_PATH, 10, false, 0);

    PullResponse response =
        PullResponse.newBuilder()
            .addReceivedMessages(
                receivedMessage("a-1", "m-1", "body", Map.of("attr1", "v1"), "ord-1"))
            .build();
    when(pullCallable.call(any(PullRequest.class))).thenReturn(response);

    List<PubSubReceivedMessage> result = fetcher.fetch();

    assertEquals(1, result.size());
    Map<String, String> attrs = result.get(0).attributes();
    assertEquals("v1", attrs.get("attr1"));
    assertEquals("m-1", attrs.get("messageId"));
    assertEquals("ord-1", attrs.get("orderingKey"));
  }

  @Test
  void testFetchReturnsEmptyWhenNoMessages() {
    PubSubSourceFetcher fetcher =
        new PubSubSourceFetcher(stub, SUBSCRIPTION_PATH, 10, false, 0);

    when(pullCallable.call(any(PullRequest.class)))
        .thenReturn(PullResponse.getDefaultInstance());

    List<PubSubReceivedMessage> result = fetcher.fetch();

    assertTrue(result.isEmpty());
    verify(modAckCallable, never()).call(any());
  }

  @Test
  void testFetchExtendsAckDeadlineWhenConfigured() {
    PubSubSourceFetcher fetcher =
        new PubSubSourceFetcher(stub, SUBSCRIPTION_PATH, 10, false, 120);

    PullResponse response =
        PullResponse.newBuilder()
            .addReceivedMessages(receivedMessage("a-1", "m-1", "body", Map.of(), null))
            .addReceivedMessages(receivedMessage("a-2", "m-2", "body", Map.of(), null))
            .build();
    when(pullCallable.call(any(PullRequest.class))).thenReturn(response);
    when(modAckCallable.call(any(ModifyAckDeadlineRequest.class)))
        .thenReturn(Empty.getDefaultInstance());

    fetcher.fetch();

    verify(modAckCallable)
        .call(
            argThat(
                (ModifyAckDeadlineRequest req) -> {
                  assertEquals(SUBSCRIPTION_PATH, req.getSubscription());
                  assertEquals(120, req.getAckDeadlineSeconds());
                  assertEquals(List.of("a-1", "a-2"), req.getAckIdsList());
                  return true;
                }));
  }

  @Test
  void testFetchSwallowsModifyAckDeadlineFailure() {
    PubSubSourceFetcher fetcher =
        new PubSubSourceFetcher(stub, SUBSCRIPTION_PATH, 10, false, 60);

    PullResponse response =
        PullResponse.newBuilder()
            .addReceivedMessages(receivedMessage("a-1", "m-1", "body", Map.of(), null))
            .build();
    when(pullCallable.call(any(PullRequest.class))).thenReturn(response);
    when(modAckCallable.call(any(ModifyAckDeadlineRequest.class)))
        .thenThrow(new RuntimeException("transient"));

    List<PubSubReceivedMessage> result = fetcher.fetch();

    assertEquals(1, result.size());
  }

  @Test
  void testIsExhaustedReturnsFalse() {
    PubSubSourceFetcher fetcher =
        new PubSubSourceFetcher(stub, SUBSCRIPTION_PATH, 10, false, 0);
    assertFalse(fetcher.isExhausted());
  }

  @Test
  void testCommitStrategyIsPerRecord() {
    PubSubSourceFetcher fetcher =
        new PubSubSourceFetcher(stub, SUBSCRIPTION_PATH, 10, false, 0);
    assertEquals(
        io.fleak.zephflow.lib.commands.source.PerRecordCommitStrategy.INSTANCE,
        fetcher.commitStrategy());
  }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```
./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.pubsubsource.PubSubSourceFetcherTest'
```

Expected: FAIL with compilation error — `PubSubSourceFetcher` cannot be resolved.

- [ ] **Step 3: Write the fetcher**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceFetcher.java`:

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
package io.fleak.zephflow.lib.commands.pubsubsource;

import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import io.fleak.zephflow.lib.commands.source.CommitStrategy;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.commands.source.PerRecordCommitStrategy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PubSubSourceFetcher implements Fetcher<PubSubReceivedMessage> {

  private static final int ACK_CHUNK_SIZE = 1000;

  private final SubscriberStub subscriberStub;
  private final String subscriptionPath;
  private final int maxMessages;
  private final boolean returnImmediately;
  private final int ackDeadlineExtensionSeconds;
  private final ConcurrentLinkedQueue<String> pendingAckIds = new ConcurrentLinkedQueue<>();

  public PubSubSourceFetcher(
      SubscriberStub subscriberStub,
      String subscriptionPath,
      int maxMessages,
      boolean returnImmediately,
      int ackDeadlineExtensionSeconds) {
    this.subscriberStub = subscriberStub;
    this.subscriptionPath = subscriptionPath;
    this.maxMessages = maxMessages;
    this.returnImmediately = returnImmediately;
    this.ackDeadlineExtensionSeconds = ackDeadlineExtensionSeconds;
  }

  @Override
  public List<PubSubReceivedMessage> fetch() {
    PullRequest request =
        PullRequest.newBuilder()
            .setSubscription(subscriptionPath)
            .setMaxMessages(maxMessages)
            .setReturnImmediately(returnImmediately)
            .build();

    PullResponse response = subscriberStub.pullCallable().call(request);
    List<ReceivedMessage> received = response.getReceivedMessagesList();

    if (received.isEmpty()) {
      log.debug("No messages received from Pub/Sub subscription: {}", subscriptionPath);
      return List.of();
    }

    if (ackDeadlineExtensionSeconds > 0) {
      List<String> ackIds = received.stream().map(ReceivedMessage::getAckId).toList();
      try {
        subscriberStub
            .modifyAckDeadlineCallable()
            .call(
                ModifyAckDeadlineRequest.newBuilder()
                    .setSubscription(subscriptionPath)
                    .addAllAckIds(ackIds)
                    .setAckDeadlineSeconds(ackDeadlineExtensionSeconds)
                    .build());
      } catch (Exception e) {
        log.warn(
            "Failed to extend ack deadline for {} messages on subscription {}",
            ackIds.size(),
            subscriptionPath,
            e);
      }
    }

    List<PubSubReceivedMessage> result = new ArrayList<>(received.size());
    for (ReceivedMessage rm : received) {
      PubsubMessage message = rm.getMessage();
      Map<String, String> attributes = new HashMap<>(message.getAttributesMap());
      attributes.put("messageId", message.getMessageId());
      if (!message.getOrderingKey().isEmpty()) {
        attributes.put("orderingKey", message.getOrderingKey());
      }
      if (message.hasPublishTime()) {
        attributes.put(
            "publishTime",
            com.google.protobuf.util.Timestamps.toString(message.getPublishTime()));
      }

      result.add(
          new PubSubReceivedMessage(
              message.getData().toByteArray(),
              message.getMessageId(),
              rm.getAckId(),
              attributes));
      pendingAckIds.add(rm.getAckId());
    }

    log.debug("Fetched {} messages from Pub/Sub subscription: {}", result.size(), subscriptionPath);
    return result;
  }

  @Override
  public boolean isExhausted() {
    return false;
  }

  @Override
  public Committer committer() {
    return () -> {
      List<String> ackIds = new ArrayList<>();
      String ackId;
      while ((ackId = pendingAckIds.poll()) != null) {
        ackIds.add(ackId);
        if (ackIds.size() >= ACK_CHUNK_SIZE) {
          ackChunk(ackIds);
          ackIds.clear();
        }
      }
      if (!ackIds.isEmpty()) {
        ackChunk(ackIds);
      }
    };
  }

  private void ackChunk(List<String> ackIds) {
    try {
      subscriberStub
          .acknowledgeCallable()
          .call(
              AcknowledgeRequest.newBuilder()
                  .setSubscription(subscriptionPath)
                  .addAllAckIds(ackIds)
                  .build());
    } catch (Exception e) {
      log.error(
          "Failed to acknowledge {} Pub/Sub messages on subscription {}",
          ackIds.size(),
          subscriptionPath,
          e);
    }
  }

  @Override
  public CommitStrategy commitStrategy() {
    return PerRecordCommitStrategy.INSTANCE;
  }

  @Override
  public void close() throws IOException {
    if (subscriberStub != null) {
      subscriberStub.close();
    }
  }
}
```

Note on the `attributes` keys used (`messageId`, `orderingKey`, `publishTime`): these are convenience entries we add in addition to the message's own user-supplied attributes. The convenience keys are documented through the test assertions.

- [ ] **Step 4: Run the tests to verify they pass**

```
./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.pubsubsource.PubSubSourceFetcherTest'
```

Expected: 7 tests, all PASS.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceFetcher.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceFetcherTest.java
git commit -m "Add PubSubSourceFetcher with synchronous pull and ack deadline extension"
```

---

## Task 7: Source fetcher commit tests

**Files:**
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceFetcherCommitTest.java`

The fetcher already implements commit (Task 6); this task adds the dedicated commit-path tests.

- [ ] **Step 1: Write the tests**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceFetcherCommitTest.java`:

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
package io.fleak.zephflow.lib.commands.pubsubsource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PubSubSourceFetcherCommitTest {

  private static final String SUBSCRIPTION_PATH = "projects/p/subscriptions/s";

  private SubscriberStub stub;
  private UnaryCallable<PullRequest, PullResponse> pullCallable;
  private UnaryCallable<AcknowledgeRequest, Empty> ackCallable;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    stub = mock(SubscriberStub.class);
    pullCallable = (UnaryCallable<PullRequest, PullResponse>) mock(UnaryCallable.class);
    ackCallable = (UnaryCallable<AcknowledgeRequest, Empty>) mock(UnaryCallable.class);
    when(stub.pullCallable()).thenReturn(pullCallable);
    when(stub.acknowledgeCallable()).thenReturn(ackCallable);
    UnaryCallable<ModifyAckDeadlineRequest, Empty> modAck =
        (UnaryCallable<ModifyAckDeadlineRequest, Empty>) mock(UnaryCallable.class);
    when(stub.modifyAckDeadlineCallable()).thenReturn(modAck);
  }

  private static ReceivedMessage rm(String ackId, String id) {
    return ReceivedMessage.newBuilder()
        .setAckId(ackId)
        .setMessage(
            PubsubMessage.newBuilder()
                .setMessageId(id)
                .setData(ByteString.copyFromUtf8("body"))
                .build())
        .build();
  }

  @Test
  void testCommitAcknowledgesPulledIds() throws Exception {
    PubSubSourceFetcher fetcher =
        new PubSubSourceFetcher(stub, SUBSCRIPTION_PATH, 10, false, 0);

    PullResponse response =
        PullResponse.newBuilder()
            .addReceivedMessages(rm("a-1", "m-1"))
            .addReceivedMessages(rm("a-2", "m-2"))
            .build();
    when(pullCallable.call(any(PullRequest.class))).thenReturn(response);
    when(ackCallable.call(any(AcknowledgeRequest.class))).thenReturn(Empty.getDefaultInstance());

    fetcher.fetch();
    Fetcher.Committer committer = fetcher.committer();
    committer.commit();

    verify(ackCallable)
        .call(
            argThat(
                (AcknowledgeRequest req) -> {
                  assertEquals(SUBSCRIPTION_PATH, req.getSubscription());
                  assertEquals(2, req.getAckIdsCount());
                  assertTrue(req.getAckIdsList().contains("a-1"));
                  assertTrue(req.getAckIdsList().contains("a-2"));
                  return true;
                }));
  }

  @Test
  void testCommitWithNoMessages() throws Exception {
    PubSubSourceFetcher fetcher =
        new PubSubSourceFetcher(stub, SUBSCRIPTION_PATH, 10, false, 0);

    when(pullCallable.call(any(PullRequest.class))).thenReturn(PullResponse.getDefaultInstance());

    fetcher.fetch();
    fetcher.committer().commit();

    verify(ackCallable, never()).call(any(AcknowledgeRequest.class));
  }

  @Test
  void testCommitChunksAtOneThousand() throws Exception {
    PubSubSourceFetcher fetcher =
        new PubSubSourceFetcher(stub, SUBSCRIPTION_PATH, 1500, false, 0);

    PullResponse.Builder builder = PullResponse.newBuilder();
    for (int i = 0; i < 1500; i++) {
      builder.addReceivedMessages(rm("a-" + i, "m-" + i));
    }
    when(pullCallable.call(any(PullRequest.class))).thenReturn(builder.build());
    when(ackCallable.call(any(AcknowledgeRequest.class))).thenReturn(Empty.getDefaultInstance());

    fetcher.fetch();
    fetcher.committer().commit();

    verify(ackCallable, times(2)).call(any(AcknowledgeRequest.class));
  }

  @Test
  void testCommitHandlesAckFailureGracefully() throws Exception {
    PubSubSourceFetcher fetcher =
        new PubSubSourceFetcher(stub, SUBSCRIPTION_PATH, 10, false, 0);

    PullResponse response =
        PullResponse.newBuilder().addReceivedMessages(rm("a-1", "m-1")).build();
    when(pullCallable.call(any(PullRequest.class))).thenReturn(response);
    when(ackCallable.call(any(AcknowledgeRequest.class)))
        .thenThrow(new RuntimeException("ack failed"));

    fetcher.fetch();

    assertDoesNotThrow(() -> fetcher.committer().commit());
    verify(ackCallable).call(any(AcknowledgeRequest.class));
  }
}
```

- [ ] **Step 2: Run the tests**

```
./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.pubsubsource.PubSubSourceFetcherCommitTest'
```

Expected: 4 tests, all PASS.

- [ ] **Step 3: Commit**

```bash
git add lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceFetcherCommitTest.java
git commit -m "Add PubSubSourceFetcher commit-path tests"
```

---

## Task 8: Source command + factory

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceCommand.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceCommandFactory.java`

No dedicated test (parallel to `SqsSourceCommand` / `GcsSourceCommand` — both are untested directly). The command is registered in Task 12, and a smoke check happens in the final build step.

- [ ] **Step 1: Write the command**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceCommand.java`:

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
package io.fleak.zephflow.lib.commands.pubsubsource;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.credentials.GcpCredential;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
import io.fleak.zephflow.lib.gcp.PubSubClientFactory;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

public class PubSubSourceCommand extends SimpleSourceCommand<PubSubReceivedMessage> {

  private final PubSubClientFactory pubSubClientFactory;

  public PubSubSourceCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      PubSubClientFactory pubSubClientFactory) {
    super(nodeId, jobContext, configParser, configValidator);
    this.pubSubClientFactory = pubSubClientFactory;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    PubSubSourceDto.Config config = (PubSubSourceDto.Config) commandConfig;

    Fetcher<PubSubReceivedMessage> fetcher = createFetcher(config, jobContext);
    RawDataEncoder<PubSubReceivedMessage> encoder = new PubSubRawDataEncoder();
    RawDataConverter<PubSubReceivedMessage> converter = createConverter(config);

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

  private Fetcher<PubSubReceivedMessage> createFetcher(
      PubSubSourceDto.Config config, JobContext jobContext) {
    Optional<GcpCredential> credentialOpt = Optional.empty();
    if (StringUtils.isNotBlank(config.getCredentialId())) {
      credentialOpt = lookupGcpCredentialOpt(jobContext, config.getCredentialId());
    }

    String projectId =
        StringUtils.firstNonBlank(
            config.getProjectId(), credentialOpt.map(GcpCredential::getProjectId).orElse(null));
    if (StringUtils.isBlank(projectId)) {
      throw new IllegalArgumentException(
          "projectId required: set Config.projectId or GcpCredential.projectId");
    }
    String subscriptionPath = "projects/" + projectId + "/subscriptions/" + config.getSubscription();

    SubscriberStub stub =
        credentialOpt
            .map(pubSubClientFactory::createSubscriberStub)
            .orElseGet(pubSubClientFactory::createSubscriberStub);

    int maxMessages =
        config.getMaxMessages() != null
            ? config.getMaxMessages()
            : PubSubSourceDto.DEFAULT_MAX_MESSAGES;
    boolean returnImmediately =
        config.getReturnImmediately() != null && config.getReturnImmediately();
    int ackExtension =
        config.getAckDeadlineExtensionSeconds() != null
            ? config.getAckDeadlineExtensionSeconds()
            : 0;

    return new PubSubSourceFetcher(
        stub, subscriptionPath, maxMessages, returnImmediately, ackExtension);
  }

  private RawDataConverter<PubSubReceivedMessage> createConverter(PubSubSourceDto.Config config) {
    FleakDeserializer<?> deserializer =
        DeserializerFactory.createDeserializerFactory(config.getEncodingType())
            .createDeserializer();
    return new PubSubRawDataConverter(deserializer);
  }

  @Override
  public SourceType sourceType() {
    return SourceType.STREAMING;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_PUBSUB_SOURCE;
  }
}
```

- [ ] **Step 2: Write the factory**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceCommandFactory.java`:

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
package io.fleak.zephflow.lib.commands.pubsubsource;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.JsonConfigParser;
import io.fleak.zephflow.lib.commands.source.SourceCommandFactory;
import io.fleak.zephflow.lib.gcp.PubSubClientFactory;

public class PubSubSourceCommandFactory extends SourceCommandFactory {
  @Override
  public PubSubSourceCommand createCommand(String nodeId, JobContext jobContext) {
    return new PubSubSourceCommand(
        nodeId,
        jobContext,
        new JsonConfigParser<>(PubSubSourceDto.Config.class),
        new PubSubSourceConfigValidator(),
        new PubSubClientFactory());
  }
}
```

Note: `COMMAND_NAME_PUBSUB_SOURCE` does not exist yet — Task 12 adds it. The compile will fail until then. We bridge by referencing the constant before it exists; this is intentional and is fixed by Task 12 before the next build verification.

- [ ] **Step 3: Skip standalone build verification**

Compilation will fail because `COMMAND_NAME_PUBSUB_SOURCE` is not yet defined. We will compile after Task 12 wires the constants in. Do NOT run `./gradlew :lib:compileJava` here.

- [ ] **Step 4: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceCommand.java \
        lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsource/PubSubSourceCommandFactory.java
git commit -m "Add PubSubSourceCommand and factory"
```

---

## Task 9: Sink DTO + JSON round-trip test

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkDto.java`
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkDtoTest.java`

- [ ] **Step 1: Write the failing test**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkDtoTest.java`:

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
package io.fleak.zephflow.lib.commands.pubsubsink;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import org.junit.jupiter.api.Test;

class PubSubSinkDtoTest {

  @Test
  void testConfigBuilder() {
    PubSubSinkDto.Config config =
        PubSubSinkDto.Config.builder()
            .projectId("my-project")
            .topic("my-topic")
            .encodingType("JSON_OBJECT")
            .credentialId("my-cred")
            .orderingKeyExpression("$.tenantId")
            .batchSize(250)
            .build();

    assertEquals("my-project", config.getProjectId());
    assertEquals("my-topic", config.getTopic());
    assertEquals("JSON_OBJECT", config.getEncodingType());
    assertEquals("my-cred", config.getCredentialId());
    assertEquals("$.tenantId", config.getOrderingKeyExpression());
    assertEquals(250, config.getBatchSize());
  }

  @Test
  void testDefaultValues() {
    PubSubSinkDto.Config config =
        PubSubSinkDto.Config.builder().topic("t").encodingType("JSON_OBJECT").build();

    assertEquals(PubSubSinkDto.DEFAULT_BATCH_SIZE, config.getBatchSize());
    assertNull(config.getCredentialId());
    assertNull(config.getProjectId());
    assertNull(config.getOrderingKeyExpression());
  }

  @Test
  void testJsonParsing() {
    Map<String, Object> jsonMap =
        Map.of(
            "projectId", "p",
            "topic", "t",
            "encodingType", "JSON_OBJECT",
            "batchSize", 50,
            "orderingKeyExpression", "$.id");

    PubSubSinkDto.Config config = OBJECT_MAPPER.convertValue(jsonMap, PubSubSinkDto.Config.class);

    assertEquals("p", config.getProjectId());
    assertEquals("t", config.getTopic());
    assertEquals("JSON_OBJECT", config.getEncodingType());
    assertEquals(50, config.getBatchSize());
    assertEquals("$.id", config.getOrderingKeyExpression());
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

```
./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.pubsubsink.PubSubSinkDtoTest'
```

Expected: FAIL with compilation error — `PubSubSinkDto` cannot be resolved.

- [ ] **Step 3: Write the DTO**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkDto.java`:

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
package io.fleak.zephflow.lib.commands.pubsubsink;

import io.fleak.zephflow.api.CommandConfig;
import lombok.*;

public interface PubSubSinkDto {

  int DEFAULT_BATCH_SIZE = 100;
  int MAX_BATCH_SIZE = 1000;

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  class Config implements CommandConfig {
    private String projectId;
    @NonNull private String topic;
    @NonNull private String encodingType;
    private String credentialId;
    private String orderingKeyExpression;
    @Builder.Default private Integer batchSize = DEFAULT_BATCH_SIZE;
  }
}
```

- [ ] **Step 4: Run the test to verify it passes**

```
./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.pubsubsink.PubSubSinkDtoTest'
```

Expected: 3 tests, all PASS.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkDto.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkDtoTest.java
git commit -m "Add PubSubSinkDto with config defaults and JSON round-trip test"
```

---

## Task 10: Sink config validator + tests

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkConfigValidator.java`
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkConfigValidatorTest.java`

- [ ] **Step 1: Write the failing tests**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkConfigValidatorTest.java`:

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
package io.fleak.zephflow.lib.commands.pubsubsink;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.credentials.GcpCredential;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PubSubSinkConfigValidatorTest {

  static final JobContext TEST_JOB_CONTEXT =
      JobContext.builder()
          .metricTags(TestUtils.JOB_CONTEXT.getMetricTags())
          .otherProperties(
              new HashMap<>(
                  Map.of(
                      "example-credential-id",
                      new HashMap<>(
                          Map.of(
                              "authType",
                              GcpCredential.AuthType.APPLICATION_DEFAULT.name(),
                              "projectId",
                              "test-project")))))
          .build();

  @Test
  void validateConfig_valid() {
    PubSubSinkConfigValidator validator = new PubSubSinkConfigValidator();
    PubSubSinkDto.Config config =
        PubSubSinkDto.Config.builder()
            .projectId("p")
            .topic("t")
            .encodingType(EncodingType.JSON_OBJECT.name())
            .credentialId("example-credential-id")
            .build();
    assertDoesNotThrow(() -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_blankTopic() {
    PubSubSinkConfigValidator validator = new PubSubSinkConfigValidator();
    PubSubSinkDto.Config config = new PubSubSinkDto.Config();
    config.setTopic("");
    config.setEncodingType(EncodingType.JSON_OBJECT.name());
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_blankEncoding() {
    PubSubSinkConfigValidator validator = new PubSubSinkConfigValidator();
    PubSubSinkDto.Config config = new PubSubSinkDto.Config();
    config.setTopic("t");
    config.setEncodingType("");
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_unsupportedEncoding() {
    PubSubSinkConfigValidator validator = new PubSubSinkConfigValidator();
    PubSubSinkDto.Config config =
        PubSubSinkDto.Config.builder()
            .topic("t")
            .encodingType(EncodingType.STRING_LINE.name())
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_allSupportedEncodings() {
    PubSubSinkConfigValidator validator = new PubSubSinkConfigValidator();
    for (EncodingType type : SerializerFactory.SUPPORTED_ENCODING_TYPES) {
      PubSubSinkDto.Config config =
          PubSubSinkDto.Config.builder().topic("t").encodingType(type.name()).build();
      assertDoesNotThrow(() -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
    }
  }

  @Test
  void validateConfig_invalidBatchSizeAboveCap() {
    PubSubSinkConfigValidator validator = new PubSubSinkConfigValidator();
    PubSubSinkDto.Config config =
        PubSubSinkDto.Config.builder()
            .topic("t")
            .encodingType(EncodingType.JSON_OBJECT.name())
            .batchSize(1001)
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_zeroBatchSize() {
    PubSubSinkConfigValidator validator = new PubSubSinkConfigValidator();
    PubSubSinkDto.Config config =
        PubSubSinkDto.Config.builder()
            .topic("t")
            .encodingType(EncodingType.JSON_OBJECT.name())
            .batchSize(0)
            .build();
    assertThrows(
        IllegalArgumentException.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }

  @Test
  void validateConfig_invalidOrderingKeyExpression() {
    PubSubSinkConfigValidator validator = new PubSubSinkConfigValidator();
    PubSubSinkDto.Config config =
        PubSubSinkDto.Config.builder()
            .topic("t")
            .encodingType(EncodingType.JSON_OBJECT.name())
            .orderingKeyExpression("not a valid expression")
            .build();
    assertThrows(
        Exception.class,
        () -> validator.validateConfig(config, "nodeId", TEST_JOB_CONTEXT));
  }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```
./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.pubsubsink.PubSubSinkConfigValidatorTest'
```

Expected: FAIL with compilation error — `PubSubSinkConfigValidator` cannot be resolved.

- [ ] **Step 3: Write the validator**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkConfigValidator.java`:

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
package io.fleak.zephflow.lib.commands.pubsubsink;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import com.google.common.base.Preconditions;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.api.ConfigValidator;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import org.apache.commons.lang3.StringUtils;

public class PubSubSinkConfigValidator implements ConfigValidator {
  @Override
  public void validateConfig(CommandConfig commandConfig, String nodeId, JobContext jobContext) {
    PubSubSinkDto.Config config = (PubSubSinkDto.Config) commandConfig;
    Preconditions.checkArgument(StringUtils.isNotBlank(config.getTopic()), "no topic is provided");
    Preconditions.checkArgument(
        StringUtils.isNotBlank(config.getEncodingType()), "no encoding type is provided");

    EncodingType encodingType = parseEnum(EncodingType.class, config.getEncodingType());
    SerializerFactory.validateEncodingType(encodingType);

    if (config.getBatchSize() != null) {
      Preconditions.checkArgument(
          config.getBatchSize() >= 1 && config.getBatchSize() <= PubSubSinkDto.MAX_BATCH_SIZE,
          "batchSize must be between 1 and %s",
          PubSubSinkDto.MAX_BATCH_SIZE);
    }

    if (StringUtils.isNotBlank(config.getOrderingKeyExpression())) {
      PathExpression.fromString(config.getOrderingKeyExpression());
    }

    if (StringUtils.trimToNull(config.getCredentialId()) != null
        && enforceCredentials(jobContext)) {
      lookupGcpCredential(jobContext, config.getCredentialId());
    }
  }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

```
./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.pubsubsink.PubSubSinkConfigValidatorTest'
```

Expected: 8 tests, all PASS.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkConfigValidator.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkConfigValidatorTest.java
git commit -m "Add PubSubSinkConfigValidator with field, range, and expression checks"
```

---

## Task 11: Sink message processor + tests

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubOutboundMessage.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkMessageProcessor.java`
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkMessageProcessorTest.java`

- [ ] **Step 1: Write the outbound record**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubOutboundMessage.java`:

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
package io.fleak.zephflow.lib.commands.pubsubsink;

public record PubSubOutboundMessage(String body, String orderingKey) {}
```

- [ ] **Step 2: Write the failing test**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkMessageProcessorTest.java`:

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
package io.fleak.zephflow.lib.commands.pubsubsink;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PubSubSinkMessageProcessorTest {

  @Test
  void testPreprocessWithoutOrderingKey() {
    PubSubSinkMessageProcessor processor = new PubSubSinkMessageProcessor(null);

    RecordFleakData event =
        new RecordFleakData(
            Map.of(
                "name", new StringPrimitiveFleakData("test"),
                "value", new StringPrimitiveFleakData("123")));

    PubSubOutboundMessage result = processor.preprocess(event, System.currentTimeMillis());

    assertNotNull(result.body());
    assertTrue(result.body().contains("\"name\""));
    assertTrue(result.body().contains("\"test\""));
    assertNull(result.orderingKey());
  }

  @Test
  void testPreprocessWithOrderingKey() {
    PathExpression keyExpr = PathExpression.fromString("$.tenantId");
    PubSubSinkMessageProcessor processor = new PubSubSinkMessageProcessor(keyExpr);

    RecordFleakData event =
        new RecordFleakData(
            Map.of(
                "tenantId", new StringPrimitiveFleakData("acme"),
                "data", new StringPrimitiveFleakData("payload")));

    PubSubOutboundMessage result = processor.preprocess(event, System.currentTimeMillis());

    assertEquals("acme", result.orderingKey());
    assertTrue(result.body().contains("\"tenantId\""));
  }

  @Test
  void testPreprocessWithMissingOrderingKeyField() {
    PathExpression keyExpr = PathExpression.fromString("$.tenantId");
    PubSubSinkMessageProcessor processor = new PubSubSinkMessageProcessor(keyExpr);

    RecordFleakData event =
        new RecordFleakData(Map.of("data", new StringPrimitiveFleakData("payload")));

    PubSubOutboundMessage result = processor.preprocess(event, System.currentTimeMillis());

    assertNotNull(result.body());
    assertNull(result.orderingKey());
  }

  @Test
  void testPreprocessBodyIsValidJson() {
    PubSubSinkMessageProcessor processor = new PubSubSinkMessageProcessor(null);

    RecordFleakData event =
        new RecordFleakData(Map.of("key", new StringPrimitiveFleakData("value")));

    PubSubOutboundMessage result = processor.preprocess(event, System.currentTimeMillis());

    assertTrue(result.body().startsWith("{"));
    assertTrue(result.body().endsWith("}"));
    assertTrue(result.body().contains("\"key\""));
    assertTrue(result.body().contains("\"value\""));
  }
}
```

- [ ] **Step 3: Run the test to verify it fails**

```
./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.pubsubsink.PubSubSinkMessageProcessorTest'
```

Expected: FAIL with compilation error — `PubSubSinkMessageProcessor` cannot be resolved.

- [ ] **Step 4: Write the processor**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkMessageProcessor.java`:

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
package io.fleak.zephflow.lib.commands.pubsubsink;

import static io.fleak.zephflow.lib.utils.JsonUtils.toJsonString;

import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import javax.annotation.Nullable;

public class PubSubSinkMessageProcessor
    implements SimpleSinkCommand.SinkMessagePreProcessor<PubSubOutboundMessage> {

  private final PathExpression orderingKeyExpression;

  public PubSubSinkMessageProcessor(@Nullable PathExpression orderingKeyExpression) {
    this.orderingKeyExpression = orderingKeyExpression;
  }

  @Override
  public PubSubOutboundMessage preprocess(RecordFleakData event, long ts) {
    String body = toJsonString(event);
    String orderingKey =
        orderingKeyExpression != null
            ? orderingKeyExpression.getStringValueFromEventOrDefault(event, null)
            : null;
    return new PubSubOutboundMessage(body, orderingKey);
  }
}
```

- [ ] **Step 5: Run the test to verify it passes**

```
./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.pubsubsink.PubSubSinkMessageProcessorTest'
```

Expected: 4 tests, all PASS.

- [ ] **Step 6: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubOutboundMessage.java \
        lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkMessageProcessor.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkMessageProcessorTest.java
git commit -m "Add PubSub sink outbound message and message processor"
```

---

## Task 12: Sink flusher + tests

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkFlusher.java`
- Create: `lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkFlusherTest.java`

- [ ] **Step 1: Write the failing tests**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkFlusherTest.java`:

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
package io.fleak.zephflow.lib.commands.pubsubsink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.pubsub.v1.stub.PublisherStub;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.api.structure.StringPrimitiveFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PubSubSinkFlusherTest {

  private static final String TOPIC_PATH = "projects/p/topics/t";

  private PublisherStub stub;
  private UnaryCallable<PublishRequest, PublishResponse> publishCallable;
  private PubSubSinkFlusher flusher;

  @BeforeEach
  @SuppressWarnings("unchecked")
  void setUp() {
    stub = mock(PublisherStub.class);
    publishCallable = (UnaryCallable<PublishRequest, PublishResponse>) mock(UnaryCallable.class);
    when(stub.publishCallable()).thenReturn(publishCallable);
    flusher = new PubSubSinkFlusher(stub, TOPIC_PATH);
  }

  @Test
  void testSuccessfulFlush() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<PubSubOutboundMessage> prepared =
        new SimpleSinkCommand.PreparedInputEvents<>();

    RecordFleakData r1 = new RecordFleakData(Map.of("d", new StringPrimitiveFleakData("v1")));
    RecordFleakData r2 = new RecordFleakData(Map.of("d", new StringPrimitiveFleakData("v2")));
    prepared.add(r1, new PubSubOutboundMessage("{\"d\":\"v1\"}", null));
    prepared.add(r2, new PubSubOutboundMessage("{\"d\":\"v2\"}", null));

    when(publishCallable.call(any(PublishRequest.class)))
        .thenReturn(PublishResponse.newBuilder().addMessageIds("m1").addMessageIds("m2").build());

    SimpleSinkCommand.FlushResult result = flusher.flush(prepared, Map.of());

    assertEquals(2, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    assertTrue(result.flushedDataSize() > 0);

    verify(publishCallable)
        .call(
            argThat(
                (PublishRequest req) -> {
                  assertEquals(TOPIC_PATH, req.getTopic());
                  assertEquals(2, req.getMessagesCount());
                  assertEquals("{\"d\":\"v1\"}", req.getMessages(0).getData().toStringUtf8());
                  assertEquals("{\"d\":\"v2\"}", req.getMessages(1).getData().toStringUtf8());
                  return true;
                }));
  }

  @Test
  void testFlushWithOrderingKey() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<PubSubOutboundMessage> prepared =
        new SimpleSinkCommand.PreparedInputEvents<>();

    RecordFleakData r = new RecordFleakData(Map.of("d", new StringPrimitiveFleakData("v")));
    prepared.add(r, new PubSubOutboundMessage("{\"d\":\"v\"}", "tenant-1"));

    when(publishCallable.call(any(PublishRequest.class)))
        .thenReturn(PublishResponse.newBuilder().addMessageIds("m1").build());

    SimpleSinkCommand.FlushResult result = flusher.flush(prepared, Map.of());

    assertEquals(1, result.successCount());

    verify(publishCallable)
        .call(
            argThat(
                (PublishRequest req) -> {
                  assertEquals("tenant-1", req.getMessages(0).getOrderingKey());
                  return true;
                }));
  }

  @Test
  void testFlushWithoutOrderingKey() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<PubSubOutboundMessage> prepared =
        new SimpleSinkCommand.PreparedInputEvents<>();

    RecordFleakData r = new RecordFleakData(Map.of("d", new StringPrimitiveFleakData("v")));
    prepared.add(r, new PubSubOutboundMessage("{\"d\":\"v\"}", null));

    when(publishCallable.call(any(PublishRequest.class)))
        .thenReturn(PublishResponse.newBuilder().addMessageIds("m1").build());

    flusher.flush(prepared, Map.of());

    verify(publishCallable)
        .call(
            argThat(
                (PublishRequest req) -> {
                  assertEquals("", req.getMessages(0).getOrderingKey());
                  return true;
                }));
  }

  @Test
  void testEmptyBatch() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<PubSubOutboundMessage> prepared =
        new SimpleSinkCommand.PreparedInputEvents<>();

    SimpleSinkCommand.FlushResult result = flusher.flush(prepared, Map.of());

    assertEquals(0, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    verify(publishCallable, never()).call(any(PublishRequest.class));
  }

  @Test
  void testPublisherException() throws Exception {
    SimpleSinkCommand.PreparedInputEvents<PubSubOutboundMessage> prepared =
        new SimpleSinkCommand.PreparedInputEvents<>();

    RecordFleakData r1 = new RecordFleakData(Map.of("d", new StringPrimitiveFleakData("v1")));
    RecordFleakData r2 = new RecordFleakData(Map.of("d", new StringPrimitiveFleakData("v2")));
    prepared.add(r1, new PubSubOutboundMessage("{\"d\":\"v1\"}", null));
    prepared.add(r2, new PubSubOutboundMessage("{\"d\":\"v2\"}", null));

    when(publishCallable.call(any(PublishRequest.class)))
        .thenThrow(new RuntimeException("Connection timeout"));

    SimpleSinkCommand.FlushResult result = flusher.flush(prepared, Map.of());

    assertEquals(0, result.successCount());
    assertEquals(2, result.errorOutputList().size());
    assertTrue(
        result.errorOutputList().get(0).errorMessage().contains("Pub/Sub publish failed"));
  }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

```
./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.pubsubsink.PubSubSinkFlusherTest'
```

Expected: FAIL with compilation error — `PubSubSinkFlusher` cannot be resolved.

- [ ] **Step 3: Write the flusher**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkFlusher.java`:

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
package io.fleak.zephflow.lib.commands.pubsubsink;

import com.google.cloud.pubsub.v1.stub.PublisherStub;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PubsubMessage;
import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

@Slf4j
public class PubSubSinkFlusher implements SimpleSinkCommand.Flusher<PubSubOutboundMessage> {

  private final PublisherStub publisherStub;
  private final String topicPath;

  public PubSubSinkFlusher(PublisherStub publisherStub, String topicPath) {
    this.publisherStub = publisherStub;
    this.topicPath = topicPath;
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<PubSubOutboundMessage> preparedInputEvents,
      Map<String, String> metricTags)
      throws Exception {

    List<PubSubOutboundMessage> messages = preparedInputEvents.preparedList();
    if (messages.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    List<PubsubMessage> pubsubMessages = new ArrayList<>(messages.size());
    long flushedDataSize = 0;
    for (PubSubOutboundMessage message : messages) {
      PubsubMessage.Builder builder =
          PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8(message.body()));
      if (message.orderingKey() != null) {
        builder.setOrderingKey(message.orderingKey());
      }
      pubsubMessages.add(builder.build());
      flushedDataSize += message.body().getBytes(StandardCharsets.UTF_8).length;
    }

    PublishRequest request =
        PublishRequest.newBuilder().setTopic(topicPath).addAllMessages(pubsubMessages).build();

    try {
      publisherStub.publishCallable().call(request);
      log.debug("Pub/Sub flush completed: {} messages to {}", messages.size(), topicPath);
      return new SimpleSinkCommand.FlushResult(messages.size(), flushedDataSize, List.of());
    } catch (Exception e) {
      log.error("Pub/Sub publish failed for topic {}", topicPath, e);
      List<ErrorOutput> errorOutputs = new ArrayList<>(messages.size());
      for (Pair<RecordFleakData, PubSubOutboundMessage> pair :
          preparedInputEvents.rawAndPreparedList()) {
        errorOutputs.add(
            new ErrorOutput(pair.getLeft(), "Pub/Sub publish failed: " + e.getMessage()));
      }
      return new SimpleSinkCommand.FlushResult(0, 0, errorOutputs);
    }
  }

  @Override
  public void close() {
    if (publisherStub != null) {
      publisherStub.close();
    }
  }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

```
./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.pubsubsink.PubSubSinkFlusherTest'
```

Expected: 5 tests, all PASS.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkFlusher.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkFlusherTest.java
git commit -m "Add PubSubSinkFlusher with atomic batch publish"
```

---

## Task 13: Sink command + factory

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkCommand.java`
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkCommandFactory.java`

No dedicated test (parallel to `SqsSinkCommand` / `GcsSinkCommand`).

- [ ] **Step 1: Write the command**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkCommand.java`:

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
package io.fleak.zephflow.lib.commands.pubsubsink;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import com.google.cloud.pubsub.v1.stub.PublisherStub;
import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.credentials.GcpCredential;
import io.fleak.zephflow.lib.gcp.PubSubClientFactory;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

public class PubSubSinkCommand extends SimpleSinkCommand<PubSubOutboundMessage> {

  private final PubSubClientFactory pubSubClientFactory;

  protected PubSubSinkCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      PubSubClientFactory pubSubClientFactory) {
    super(nodeId, jobContext, configParser, configValidator);
    this.pubSubClientFactory = pubSubClientFactory;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_PUBSUB_SINK;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    SinkCounters counters =
        createSinkCounters(metricClientProvider, jobContext, commandName(), nodeId);

    PubSubSinkDto.Config config = (PubSubSinkDto.Config) commandConfig;
    SimpleSinkCommand.Flusher<PubSubOutboundMessage> flusher = createFlusher(config, jobContext);
    SimpleSinkCommand.SinkMessagePreProcessor<PubSubOutboundMessage> preProcessor =
        createPreProcessor(config);

    return new SinkExecutionContext<>(
        flusher,
        preProcessor,
        counters.inputMessageCounter(),
        counters.errorCounter(),
        counters.sinkOutputCounter(),
        counters.outputSizeCounter(),
        counters.sinkErrorCounter());
  }

  private SimpleSinkCommand.Flusher<PubSubOutboundMessage> createFlusher(
      PubSubSinkDto.Config config, JobContext jobContext) {
    Optional<GcpCredential> credentialOpt = Optional.empty();
    if (StringUtils.isNotBlank(config.getCredentialId())) {
      credentialOpt = lookupGcpCredentialOpt(jobContext, config.getCredentialId());
    }

    String projectId =
        StringUtils.firstNonBlank(
            config.getProjectId(), credentialOpt.map(GcpCredential::getProjectId).orElse(null));
    if (StringUtils.isBlank(projectId)) {
      throw new IllegalArgumentException(
          "projectId required: set Config.projectId or GcpCredential.projectId");
    }
    String topicPath = "projects/" + projectId + "/topics/" + config.getTopic();

    PublisherStub stub =
        credentialOpt
            .map(pubSubClientFactory::createPublisherStub)
            .orElseGet(pubSubClientFactory::createPublisherStub);

    return new PubSubSinkFlusher(stub, topicPath);
  }

  private SimpleSinkCommand.SinkMessagePreProcessor<PubSubOutboundMessage> createPreProcessor(
      PubSubSinkDto.Config config) {
    PathExpression orderingKeyExpression =
        StringUtils.isNotBlank(config.getOrderingKeyExpression())
            ? PathExpression.fromString(config.getOrderingKeyExpression())
            : null;
    return new PubSubSinkMessageProcessor(orderingKeyExpression);
  }

  @Override
  protected int batchSize() {
    PubSubSinkDto.Config config = (PubSubSinkDto.Config) commandConfig;
    return config.getBatchSize() != null
        ? config.getBatchSize()
        : PubSubSinkDto.DEFAULT_BATCH_SIZE;
  }
}
```

- [ ] **Step 2: Write the factory**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkCommandFactory.java`:

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
package io.fleak.zephflow.lib.commands.pubsubsink;

import io.fleak.zephflow.api.CommandFactory;
import io.fleak.zephflow.api.CommandType;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.OperatorCommand;
import io.fleak.zephflow.lib.commands.JsonConfigParser;
import io.fleak.zephflow.lib.gcp.PubSubClientFactory;

public class PubSubSinkCommandFactory extends CommandFactory {
  @Override
  public OperatorCommand createCommand(String nodeId, JobContext jobContext) {
    JsonConfigParser<PubSubSinkDto.Config> configParser =
        new JsonConfigParser<>(PubSubSinkDto.Config.class);
    PubSubSinkConfigValidator validator = new PubSubSinkConfigValidator();
    PubSubClientFactory pubSubClientFactory = new PubSubClientFactory();
    return new PubSubSinkCommand(nodeId, jobContext, configParser, validator, pubSubClientFactory);
  }

  @Override
  public CommandType commandType() {
    return CommandType.SINK;
  }
}
```

- [ ] **Step 3: Skip standalone build verification**

Compilation will fail because `COMMAND_NAME_PUBSUB_SINK` is not yet defined. Continue to Task 14.

- [ ] **Step 4: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkCommand.java \
        lib/src/main/java/io/fleak/zephflow/lib/commands/pubsubsink/PubSubSinkCommandFactory.java
git commit -m "Add PubSubSinkCommand and factory"
```

---

## Task 14: Wire command names + register in `OperatorCommandRegistry`

**Files:**
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java`
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java`

- [ ] **Step 1: Add command-name constants**

In `lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java`, find the existing line:

```java
  String COMMAND_NAME_AZURE_BLOB_SINK = "azureblobsink";
```

and add immediately after it (before the `METRIC_NAME_*` block):

```java
  String COMMAND_NAME_PUBSUB_SOURCE = "pubsubsource";
  String COMMAND_NAME_PUBSUB_SINK = "pubsubsink";
```

- [ ] **Step 2: Register both factories**

In `lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java`:

a) Add these imports next to the other `pubsub*` neighbors (alphabetic placement keeps the existing import grouping):

```java
import io.fleak.zephflow.lib.commands.pubsubsink.PubSubSinkCommandFactory;
import io.fleak.zephflow.lib.commands.pubsubsource.PubSubSourceCommandFactory;
```

b) Find the existing line:

```java
          .put(COMMAND_NAME_AZURE_BLOB_SINK, new AzureBlobSinkCommandFactory())
```

and add immediately after it (before `.build();`):

```java
          .put(COMMAND_NAME_PUBSUB_SOURCE, new PubSubSourceCommandFactory())
          .put(COMMAND_NAME_PUBSUB_SINK, new PubSubSinkCommandFactory())
```

- [ ] **Step 3: Verify lib compiles end-to-end**

```
./gradlew :lib:compileJava
```

Expected: BUILD SUCCESSFUL.

- [ ] **Step 4: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java \
        lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java
git commit -m "Register pubsubsource and pubsubsink in OperatorCommandRegistry"
```

---

## Task 15: Full test + registry sanity check

**Files:**
- (no edits)

- [ ] **Step 1: Run full lib test suite**

```
./gradlew :lib:test
```

Expected: BUILD SUCCESSFUL with all tests passing. Watch for new tests reported under `pubsubsource` and `pubsubsink`.

- [ ] **Step 2: Confirm registry sees both commands**

Run a one-off check via the existing `OperatorCommandRegistryTest` (already in the repo):

```
./gradlew :lib:test --tests 'io.fleak.zephflow.lib.commands.OperatorCommandRegistryTest'
```

Expected: PASS. (If this test enumerates registered command names, both new entries should appear in any output. If it does not exist or this command shows zero matches, fall back to:)

```
./gradlew :lib:test
```

and confirm the suite still passes.

- [ ] **Step 3: Confirm no orphaned imports or unused code**

```
./gradlew :lib:spotlessCheck
```

Expected: BUILD SUCCESSFUL. If formatting drift is reported, run `./gradlew :lib:spotlessApply`, review the diff, and amend it into the previous commit if it's small. If the change is non-trivial, make a separate `Format new pubsub sources` commit.

- [ ] **Step 4: Final review of git history**

```
git log --oneline main..HEAD
```

Expected: 14 commits total (one per task that produced source). Use this list for the PR description.

---

## Self-Review

**Spec coverage check:**
- ✅ Sync-pull source — Task 6 (`PubSubSourceFetcher.fetch()` calls `pullCallable().call(...)`)
- ✅ Per-record commit / batched ack — Task 6 + Task 7 (`committer()` chunks at 1000)
- ✅ Atomic batch publish — Task 12 (`PubSubSinkFlusher.flush()` single `publishCallable().call(...)`)
- ✅ Separate `projectId` + short name addressing with credential fallback — Task 8 (source) and Task 13 (sink)
- ✅ Ordering key (sink) — Task 11 (processor) + Task 12 (flusher sets `setOrderingKey`)
- ✅ Source tunables (`maxMessages`, `returnImmediately`, `ackDeadlineExtensionSeconds`) — Task 3 (DTO) + Task 4 (validator) + Task 6 (fetcher honors all three)
- ✅ `GcpCredential` reuse — Task 2 (`PubSubClientFactory`)
- ✅ Module wiring (`MiscUtils` + `OperatorCommandRegistry`) — Task 14
- ✅ Dependency — Task 1
- ✅ Source DLQ + counters — Task 8
- ✅ Sink counters — Task 13
- ✅ All test classes from spec — Tasks 3, 4, 6, 7, 9, 10, 11, 12

**Type/method consistency check:**
- `PubSubSourceFetcher` constructor: `(SubscriberStub, String, int, boolean, int)` — matches across Task 6 (definition), Task 6/7 (test), Task 8 (call site).
- `PubSubSinkFlusher` constructor: `(PublisherStub, String)` — matches Task 12 (definition + test) and Task 13 (call site).
- `PubSubOutboundMessage` shape `(String body, String orderingKey)` — used identically in Tasks 11 (creation), 12 (consumption), 13 (preproc → flusher pipeline).
- `PubSubReceivedMessage` shape `(byte[] body, String messageId, String ackId, Map<String,String> attributes)` — used identically across Tasks 5, 6, 7, 8.
- `COMMAND_NAME_PUBSUB_SOURCE` / `COMMAND_NAME_PUBSUB_SINK` referenced in Tasks 8 and 13 before being defined in Task 14 — handled by deferring compile verification to Task 14 (Step 3).

**No-placeholder scan:** No "TBD", "TODO", "etc.", or "similar to" references remain. Every code step contains the actual code.
