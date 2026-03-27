# GCS DLQ Writer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add GCS (Google Cloud Storage) support for the Dead Letter Queue writer, with a centralized `DlqWriterFactory` to eliminate duplicated dispatch logic across command classes.

**Architecture:** New `GcsDlqWriter` mirrors `S3DlqWriter`, reusing `BufferedWriter<DeadLetter>` for batching and the Avro serializer. A new `DlqWriterFactory` centralizes the `instanceof DlqConfig` dispatch currently duplicated in ~10 command classes. `GcsClientFactory` handles GCS client creation from a service account JSON string.

**Tech Stack:** Java 21, Google Cloud Storage SDK (`google-cloud-storage`), Testcontainers with `fake-gcs-server` for integration testing, Avro for serialization.

**Spec:** `docs/superpowers/specs/2026-03-27-gcs-dlq-writer-design.md`

---

### Task 1: Add GCS dependencies to version catalog and build files

**Files:**
- Modify: `gradle/libs.versions.toml`
- Modify: `lib/build.gradle`

- [ ] **Step 1: Add GCS version and library entries to version catalog**

In `gradle/libs.versions.toml`, add the GCS version under `[versions]`:

```toml
gcs = "2.49.0"
```

And add library entries under `[libraries]`:

```toml
gcs-storage = { module = "com.google.cloud:google-cloud-storage", version.ref = "gcs" }
testcontainers-gcloud = { module = "org.testcontainers:gcloud", version.ref = "testContainers" }
```

- [ ] **Step 2: Add GCS dependencies to lib/build.gradle**

In `lib/build.gradle`, add under `dependencies`:

```groovy
implementation libs.gcs.storage
testImplementation libs.testcontainers.gcloud
```

- [ ] **Step 3: Verify compilation**

Run: `./gradlew :lib:compileJava`
Expected: BUILD SUCCESSFUL

- [ ] **Step 4: Commit**

```bash
git add gradle/libs.versions.toml lib/build.gradle
git commit -m "Add GCS storage and testcontainers-gcloud dependencies"
```

---

### Task 2: Add GcsDlqConfig to JobContext

**Files:**
- Modify: `api/src/main/java/io/fleak/zephflow/api/JobContext.java`

- [ ] **Step 1: Add GcsDlqConfig class and register in JsonSubTypes**

In `JobContext.java`, update the `@JsonSubTypes` annotation on the `DlqConfig` interface to include a `"gcs"` type:

```java
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(value = S3DlqConfig.class, name = "s3"),
    @JsonSubTypes.Type(value = GcsDlqConfig.class, name = "gcs"),
})
public interface DlqConfig {}
```

Add the `GcsDlqConfig` class after `S3DlqConfig`:

```java
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public static class GcsDlqConfig implements DlqConfig {
    private String bucket;
    private String serviceAccountJson;
    private int batchSize;
    private int flushIntervalMillis;
    private @Builder.Default long rawDataSampleIntervalMs = 60_000;
}
```

- [ ] **Step 2: Verify compilation**

Run: `./gradlew :api:compileJava`
Expected: BUILD SUCCESSFUL

- [ ] **Step 3: Commit**

```bash
git add api/src/main/java/io/fleak/zephflow/api/JobContext.java
git commit -m "Add GcsDlqConfig to JobContext for GCS DLQ support"
```

---

### Task 3: Rename DeadLetterS3CommiterSerializer to DeadLetterCommiterSerializer

**Files:**
- Rename: `lib/src/main/java/io/fleak/zephflow/lib/dlq/DeadLetterS3CommiterSerializer.java` -> `lib/src/main/java/io/fleak/zephflow/lib/dlq/DeadLetterCommiterSerializer.java`
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/dlq/S3DlqWriter.java`

- [ ] **Step 1: Rename the class file**

```bash
cd /Users/dan/fleak/zephflow-core
git mv lib/src/main/java/io/fleak/zephflow/lib/dlq/DeadLetterS3CommiterSerializer.java lib/src/main/java/io/fleak/zephflow/lib/dlq/DeadLetterCommiterSerializer.java
```

- [ ] **Step 2: Update the class name inside the renamed file**

In `lib/src/main/java/io/fleak/zephflow/lib/dlq/DeadLetterCommiterSerializer.java`, change:

```java
public class DeadLetterS3CommiterSerializer implements S3CommiterSerializer<DeadLetter> {
```

to:

```java
public class DeadLetterCommiterSerializer implements S3CommiterSerializer<DeadLetter> {
```

- [ ] **Step 3: Update the reference in S3DlqWriter**

In `lib/src/main/java/io/fleak/zephflow/lib/dlq/S3DlqWriter.java`, change the field declaration on line 50:

```java
private final DeadLetterS3CommiterSerializer serializer;
```

to:

```java
private final DeadLetterCommiterSerializer serializer;
```

And change the instantiation on line 160:

```java
this.serializer = new DeadLetterS3CommiterSerializer();
```

to:

```java
this.serializer = new DeadLetterCommiterSerializer();
```

- [ ] **Step 4: Verify compilation and existing tests pass**

Run: `./gradlew :lib:compileJava`
Expected: BUILD SUCCESSFUL

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.dlq.*"`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/dlq/DeadLetterCommiterSerializer.java lib/src/main/java/io/fleak/zephflow/lib/dlq/DeadLetterS3CommiterSerializer.java lib/src/main/java/io/fleak/zephflow/lib/dlq/S3DlqWriter.java
git commit -m "Rename DeadLetterS3CommiterSerializer to DeadLetterCommiterSerializer"
```

---

### Task 4: Create GcsClientFactory

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/gcp/GcsClientFactory.java`
- Create: `lib/src/test/java/io/fleak/zephflow/lib/gcp/GcsClientFactoryTest.java`

- [ ] **Step 1: Write the test**

Create `lib/src/test/java/io/fleak/zephflow/lib/gcp/GcsClientFactoryTest.java`:

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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class GcsClientFactoryTest {

  @Test
  void testCreateStorageClientWithInvalidJson() {
    GcsClientFactory factory = new GcsClientFactory();
    assertThrows(
        RuntimeException.class,
        () -> factory.createStorageClient("not-valid-json"));
  }

  @Test
  void testCreateStorageClientWithEmptyJson() {
    GcsClientFactory factory = new GcsClientFactory();
    assertThrows(
        RuntimeException.class,
        () -> factory.createStorageClient("{}"));
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.gcp.GcsClientFactoryTest"`
Expected: FAIL (class not found)

- [ ] **Step 3: Write the implementation**

Create `lib/src/main/java/io/fleak/zephflow/lib/gcp/GcsClientFactory.java`:

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

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

public class GcsClientFactory implements Serializable {

  public Storage createStorageClient(String serviceAccountJson) {
    try {
      ServiceAccountCredentials credentials =
          ServiceAccountCredentials.fromStream(
              new ByteArrayInputStream(serviceAccountJson.getBytes(StandardCharsets.UTF_8)));
      return StorageOptions.newBuilder().setCredentials(credentials).build().getService();
    } catch (IOException e) {
      throw new RuntimeException("Failed to create GCS storage client from service account JSON", e);
    }
  }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.gcp.GcsClientFactoryTest"`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/gcp/GcsClientFactory.java lib/src/test/java/io/fleak/zephflow/lib/gcp/GcsClientFactoryTest.java
git commit -m "Add GcsClientFactory for creating GCS Storage clients"
```

---

### Task 5: Create GcsDlqWriter with unit tests

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/dlq/GcsDlqWriter.java`
- Create: `lib/src/test/java/io/fleak/zephflow/lib/dlq/GcsDlqWriterKeyPrefixTest.java`

- [ ] **Step 1: Write the key prefix unit tests**

Create `lib/src/test/java/io/fleak/zephflow/lib/dlq/GcsDlqWriterKeyPrefixTest.java`:

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
package io.fleak.zephflow.lib.dlq;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class GcsDlqWriterKeyPrefixTest {

  private static final String BUCKET_NAME = "test-bucket";
  private static final long FIXED_TIMESTAMP = 1700000000000L;

  private GcsDlqWriter createWriter(String keyPrefix) {
    return new GcsDlqWriter(null, BUCKET_NAME, 1, 1000, keyPrefix, "dead-letters", "deadletter");
  }

  @Test
  void testCleanPrefix() {
    GcsDlqWriter writer = createWriter("env/pipeline/deployment");
    String key = writer.generateObjectKey(FIXED_TIMESTAMP);
    assertTrue(key.startsWith("env/pipeline/deployment/dead-letters/"));
    assertFalse(key.contains("//"));
  }

  @Test
  void testLeadingAndTrailingSlashes() {
    GcsDlqWriter writer = createWriter("/leading/slash/");
    String key = writer.generateObjectKey(FIXED_TIMESTAMP);
    assertTrue(key.startsWith("leading/slash/dead-letters/"));
    assertFalse(key.contains("//"));
  }

  @Test
  void testWhitespaceOnlyPrefix() {
    GcsDlqWriter writer = createWriter("   ");
    String key = writer.generateObjectKey(FIXED_TIMESTAMP);
    assertTrue(key.startsWith("dead-letters/"));
  }

  @Test
  void testAllSlashesPrefix() {
    GcsDlqWriter writer = createWriter("////");
    String key = writer.generateObjectKey(FIXED_TIMESTAMP);
    assertTrue(key.startsWith("dead-letters/"));
  }

  @Test
  void testNullPrefix() {
    GcsDlqWriter writer = createWriter(null);
    String key = writer.generateObjectKey(FIXED_TIMESTAMP);
    assertTrue(key.startsWith("dead-letters/"));
  }

  @Test
  void testPrefixWithSurroundingWhitespaceAndSlashes() {
    GcsDlqWriter writer = createWriter("  /prod/data/  ");
    String key = writer.generateObjectKey(FIXED_TIMESTAMP);
    assertTrue(key.startsWith("prod/data/dead-letters/"));
    assertFalse(key.contains("//"));
  }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.dlq.GcsDlqWriterKeyPrefixTest"`
Expected: FAIL (class not found)

- [ ] **Step 3: Write the GcsDlqWriter implementation**

Create `lib/src/main/java/io/fleak/zephflow/lib/dlq/GcsDlqWriter.java`:

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
package io.fleak.zephflow.lib.dlq;

import static io.fleak.zephflow.lib.utils.MiscUtils.threadSleep;
import static io.fleak.zephflow.lib.utils.MiscUtils.toBase64String;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.deadletter.DeadLetter;
import io.fleak.zephflow.lib.gcp.GcsClientFactory;
import io.fleak.zephflow.lib.utils.BufferedWriter;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GcsDlqWriter extends DlqWriter {
  private static final int MAX_RETRIES = 3;
  private static final String DEFAULT_PATH_SEGMENT = "dead-letters";
  private static final String DEFAULT_FILE_PREFIX = "deadletter";

  @VisibleForTesting final BufferedWriter<DeadLetter> bufferedWriter;
  @VisibleForTesting final Storage storage;
  private final String bucketName;
  private final String keyPrefix;
  private final String pathSegment;
  private final String filePrefix;
  private final DeadLetterCommiterSerializer serializer;

  public static GcsDlqWriter createGcsDlqWriter(
      JobContext.GcsDlqConfig gcsDlqConfig, String keyPrefix) {
    return createWriter(
        gcsDlqConfig.getServiceAccountJson(),
        gcsDlqConfig.getBucket(),
        gcsDlqConfig.getBatchSize(),
        gcsDlqConfig.getFlushIntervalMillis(),
        keyPrefix,
        DEFAULT_PATH_SEGMENT,
        DEFAULT_FILE_PREFIX);
  }

  public static GcsDlqWriter createGcsSampleWriter(
      JobContext.GcsDlqConfig gcsDlqConfig, String keyPrefix) {
    return createWriter(
        gcsDlqConfig.getServiceAccountJson(),
        gcsDlqConfig.getBucket(),
        gcsDlqConfig.getBatchSize(),
        gcsDlqConfig.getFlushIntervalMillis(),
        keyPrefix,
        "raw-data-samples",
        "sample");
  }

  private static GcsDlqWriter createWriter(
      String serviceAccountJson,
      String bucketName,
      int batchSize,
      long flushIntervalMillis,
      String keyPrefix,
      String pathSegment,
      String filePrefix) {
    Preconditions.checkArgument(
        batchSize > 0, "batchSize must be positive but provided %d", batchSize);
    Preconditions.checkArgument(
        flushIntervalMillis > 0,
        "flushIntervalMillis must be positive but provided %d",
        flushIntervalMillis);
    Storage storage = new GcsClientFactory().createStorageClient(serviceAccountJson);
    return new GcsDlqWriter(
        storage, bucketName, batchSize, flushIntervalMillis, keyPrefix, pathSegment, filePrefix);
  }

  @VisibleForTesting
  GcsDlqWriter(
      Storage storage,
      String bucketName,
      int batchSize,
      long flushIntervalMillis,
      String keyPrefix,
      String pathSegment,
      String filePrefix) {
    this.storage = storage;
    this.bucketName = bucketName;
    this.keyPrefix = sanitizeKeyPrefix(keyPrefix);
    this.pathSegment = pathSegment;
    this.filePrefix = filePrefix;
    this.serializer = new DeadLetterCommiterSerializer();
    this.bufferedWriter =
        new BufferedWriter<>(
            batchSize, flushIntervalMillis, this::uploadToGcs, "gcs-" + pathSegment + "-flusher");
  }

  @Override
  public void open() {
    bufferedWriter.start();
  }

  @Override
  protected void doWrite(DeadLetter deadLetter) {
    bufferedWriter.write(deadLetter);
  }

  @Override
  public void close() {
    bufferedWriter.close();
    try {
      storage.close();
    } catch (Exception e) {
      log.warn("Failed to close GCS storage client", e);
    }
  }

  private static String sanitizeKeyPrefix(String prefix) {
    if (prefix == null) return null;
    String sanitized = prefix.strip();
    while (sanitized.startsWith("/")) sanitized = sanitized.substring(1);
    while (sanitized.endsWith("/")) sanitized = sanitized.substring(0, sanitized.length() - 1);
    return sanitized.isEmpty() ? null : sanitized;
  }

  private void uploadToGcs(List<DeadLetter> batch) {
    long timestamp = System.currentTimeMillis();

    byte[] data;
    try {
      data = serializer.serialize(batch);
    } catch (Exception e) {
      log.error("failed to serialize {}, dropping {} records", pathSegment, batch.size(), e);
      return;
    }

    String objectName = generateObjectKey(timestamp);
    try {
      uploadToGcsWithRetry(data, objectName);
      log.info("Uploaded {} records to gs://{}/{}", batch.size(), bucketName, objectName);
    } catch (Exception e) {
      log.error("failed to write to GCS {}. data: {}", pathSegment, toBase64String(data), e);
    }
  }

  @VisibleForTesting
  String generateObjectKey(long timestamp) {
    Instant instant = Instant.ofEpochMilli(timestamp);
    ZonedDateTime utcDateTime = instant.atZone(ZoneOffset.UTC);

    String date =
        String.format(
            "%04d-%02d-%02d",
            utcDateTime.getYear(), utcDateTime.getMonthValue(), utcDateTime.getDayOfMonth());
    String hour = String.format("%02d", utcDateTime.getHour());
    String uuid = UUID.randomUUID().toString();

    String timePath =
        String.format(
            "%s/dt=%s/hr=%s/%s-%d_%s.avro", pathSegment, date, hour, filePrefix, timestamp, uuid);
    if (keyPrefix != null && !keyPrefix.isEmpty()) {
      return keyPrefix + "/" + timePath;
    }
    return timePath;
  }

  private void uploadToGcsWithRetry(byte[] data, String objectName) {
    int attempt = 0;
    while (true) {
      try {
        BlobInfo blobInfo = BlobInfo.newBuilder(bucketName, objectName).build();
        storage.create(blobInfo, data);
        return;
      } catch (Exception e) {
        if (attempt >= MAX_RETRIES) {
          throw e;
        }
        log.warn(
            "GCS upload failed (attempt {}/{}), retrying: {}",
            attempt + 1,
            MAX_RETRIES,
            e.getMessage());
        threadSleep(1000L * (attempt + 1));
      }
      attempt++;
    }
  }
}
```

- [ ] **Step 4: Run the key prefix tests to verify they pass**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.dlq.GcsDlqWriterKeyPrefixTest"`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/dlq/GcsDlqWriter.java lib/src/test/java/io/fleak/zephflow/lib/dlq/GcsDlqWriterKeyPrefixTest.java
git commit -m "Add GcsDlqWriter with key prefix unit tests"
```

---

### Task 6: Create GcsDlqWriter integration tests

**Files:**
- Create: `lib/src/test/java/io/fleak/zephflow/lib/dlq/GcsDlqWriterTest.java`

- [ ] **Step 1: Write the integration test class**

Create `lib/src/test/java/io/fleak/zephflow/lib/dlq/GcsDlqWriterTest.java`:

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
package io.fleak.zephflow.lib.dlq;

import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.fleak.zephflow.lib.deadletter.DeadLetter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class GcsDlqWriterTest {

  private static final String BUCKET_NAME = "test-dlq-bucket";
  private static final String TEST_KEY_PREFIX = "test-env/pipeline-1/deployment-1/run-1";
  private static final int FAKE_GCS_PORT = 4443;

  private GcsDlqWriter dlqWriter;
  private Storage gcsClient;

  @Container
  protected static GenericContainer<?> fakeGcsServer =
      new GenericContainer<>(DockerImageName.parse("fsouza/fake-gcs-server:latest"))
          .withExposedPorts(FAKE_GCS_PORT)
          .withCreateContainerCmdModifier(
              cmd -> cmd.withEntrypoint("/bin/fake-gcs-server", "-scheme", "http", "-port", String.valueOf(FAKE_GCS_PORT)));

  private static String getGcsEndpoint() {
    return String.format(
        "http://%s:%d", fakeGcsServer.getHost(), fakeGcsServer.getMappedPort(FAKE_GCS_PORT));
  }

  private Storage createGcsClient() {
    return StorageOptions.newBuilder()
        .setHost(getGcsEndpoint())
        .setProjectId("test-project")
        .build()
        .getService();
  }

  private void createAndOpenDlqWriter(int batchSize, long flushIntervalMillis) {
    createAndOpenDlqWriter(batchSize, flushIntervalMillis, TEST_KEY_PREFIX);
  }

  private void createAndOpenDlqWriter(int batchSize, long flushIntervalMillis, String keyPrefix) {
    gcsClient = createGcsClient();
    gcsClient.create(com.google.cloud.storage.BucketInfo.of(BUCKET_NAME));

    Storage writerClient = createGcsClient();
    dlqWriter =
        new GcsDlqWriter(
            writerClient, BUCKET_NAME, batchSize, flushIntervalMillis, keyPrefix, "dead-letters", "deadletter");
    dlqWriter.open();
  }

  private void createAndOpenSampleWriter(
      int batchSize, long flushIntervalMillis, String keyPrefix) {
    gcsClient = createGcsClient();
    gcsClient.create(com.google.cloud.storage.BucketInfo.of(BUCKET_NAME));

    Storage writerClient = createGcsClient();
    dlqWriter =
        new GcsDlqWriter(
            writerClient, BUCKET_NAME, batchSize, flushIntervalMillis, keyPrefix, "raw-data-samples", "sample");
    dlqWriter.open();
  }

  @AfterEach
  public void afterEach() {
    if (dlqWriter != null) {
      dlqWriter.close();
    }
    if (gcsClient != null) {
      deleteAllObjectsInBucket();
      try {
        gcsClient.delete(BUCKET_NAME);
      } catch (Exception e) {
        // ignore
      }
      try {
        gcsClient.close();
      } catch (Exception e) {
        // ignore
      }
    }
  }

  @Test
  void testDeadLettersFlushedOnBatchSize() throws InterruptedException {
    int batchSize = 5;
    long flushIntervalMillis = 10000;
    createAndOpenDlqWriter(batchSize, flushIntervalMillis);

    writeDeadLetters(5);
    Thread.sleep(2000);

    List<String> objectNames = listGcsObjects();
    assertEquals(1, objectNames.size(), "Expected one object in GCS after batch size reached.");
  }

  @Test
  void testDeadLettersFlushedOnFlushInterval() throws InterruptedException {
    int batchSize = 5;
    long flushIntervalMillis = 1000;
    createAndOpenDlqWriter(batchSize, flushIntervalMillis);

    writeDeadLetters(2);
    Thread.sleep(3000);

    List<String> objectNames = listGcsObjects();
    assertEquals(1, objectNames.size(), "Expected one object in GCS after flush interval.");
  }

  @Test
  void testGcsObjectKeyFormat() throws InterruptedException {
    int batchSize = 1;
    long flushIntervalMillis = 10000;
    createAndOpenDlqWriter(batchSize, flushIntervalMillis);
    writeDeadLetters(batchSize);
    Thread.sleep(2000);

    List<String> objectNames = listGcsObjects();
    assertEquals(1, objectNames.size(), "Expected one object in GCS.");

    String objectName = objectNames.get(0);
    String regex =
        TEST_KEY_PREFIX
            + "/dead-letters/dt=\\d{4}-\\d{2}-\\d{2}/hr=\\d{2}/deadletter-\\d+_[0-9a-f\\-]+\\.avro";
    assertTrue(objectName.matches(regex), "Object key does not match expected format: " + objectName);
  }

  @Test
  void testGcsObjectKeyFormatWithNullPrefix() throws InterruptedException {
    int batchSize = 1;
    long flushIntervalMillis = 10000;
    createAndOpenDlqWriter(batchSize, flushIntervalMillis, null);
    writeDeadLetters(batchSize);
    Thread.sleep(2000);

    List<String> objectNames = listGcsObjects();
    assertEquals(1, objectNames.size());

    String objectName = objectNames.get(0);
    String regex =
        "dead-letters/dt=\\d{4}-\\d{2}-\\d{2}/hr=\\d{2}/deadletter-\\d+_[0-9a-f\\-]+\\.avro";
    assertTrue(objectName.matches(regex), "Object key does not match expected format: " + objectName);
  }

  @Test
  void testSampleWriterGcsObjectKeyFormat() throws InterruptedException {
    int batchSize = 1;
    long flushIntervalMillis = 10000;
    createAndOpenSampleWriter(batchSize, flushIntervalMillis, TEST_KEY_PREFIX);
    writeDeadLetters(batchSize);
    Thread.sleep(2000);

    List<String> objectNames = listGcsObjects();
    assertEquals(1, objectNames.size(), "Expected one object in GCS.");

    String objectName = objectNames.get(0);
    String regex =
        TEST_KEY_PREFIX
            + "/raw-data-samples/dt=\\d{4}-\\d{2}-\\d{2}/hr=\\d{2}/sample-\\d+_[0-9a-f\\-]+\\.avro";
    assertTrue(objectName.matches(regex), "Object key does not match expected format: " + objectName);
  }

  @Test
  void testSampleWriterGcsObjectKeyFormatWithNullPrefix() throws InterruptedException {
    int batchSize = 1;
    long flushIntervalMillis = 10000;
    createAndOpenSampleWriter(batchSize, flushIntervalMillis, null);
    writeDeadLetters(batchSize);
    Thread.sleep(2000);

    List<String> objectNames = listGcsObjects();
    assertEquals(1, objectNames.size());

    String objectName = objectNames.get(0);
    String regex =
        "raw-data-samples/dt=\\d{4}-\\d{2}-\\d{2}/hr=\\d{2}/sample-\\d+_[0-9a-f\\-]+\\.avro";
    assertTrue(objectName.matches(regex), "Object key does not match expected format: " + objectName);
  }

  @Test
  void testUploadedDeadLettersContent() throws IOException, InterruptedException {
    int batchSize = 3;
    long flushIntervalMillis = 10000;
    createAndOpenDlqWriter(batchSize, flushIntervalMillis);
    List<SerializedEvent> writtenDeadLetters = new ArrayList<>();
    writeDeadLetters(batchSize, writtenDeadLetters);

    Thread.sleep(2000);

    List<String> objectNames = listGcsObjects();
    assertEquals(1, objectNames.size(), "Expected one object in GCS after batch size reached.");

    List<SerializedEvent> readDeadLetters = readDeadLettersFromGcs(objectNames.get(0));

    assertEquals(
        writtenDeadLetters.size(), readDeadLetters.size(), "Mismatch in number of dead letters.");

    for (int i = 0; i < batchSize; i++) {
      SerializedEvent written = writtenDeadLetters.get(i);
      SerializedEvent read = readDeadLetters.get(i);
      assertArrayEquals(written.key(), read.key(), "Mismatch in dead letter key.");
      assertArrayEquals(written.value(), read.value(), "Mismatch in dead letter value.");
      assertEquals(written.metadata(), read.metadata(), "Mismatch in dead letter metadata.");
    }
  }

  @Test
  void testUploadedDeadLettersContainNodeId() throws IOException, InterruptedException {
    int batchSize = 2;
    long flushIntervalMillis = 10000;
    createAndOpenDlqWriter(batchSize, flushIntervalMillis);
    writeDeadLetters(batchSize);

    Thread.sleep(2000);

    List<String> objectNames = listGcsObjects();
    assertEquals(1, objectNames.size());

    List<DeadLetter> deadLetters = readRawDeadLettersFromGcs(objectNames.get(0));
    for (DeadLetter dl : deadLetters) {
      assertEquals("test-node", dl.getNodeId().toString());
    }
  }

  private List<String> listGcsObjects() {
    List<String> names = new ArrayList<>();
    for (Blob blob : gcsClient.list(BUCKET_NAME).iterateAll()) {
      names.add(blob.getName());
    }
    return names;
  }

  private void writeDeadLetters(int count) {
    writeDeadLetters(count, null);
  }

  private void writeDeadLetters(int count, List<SerializedEvent> writtenDeadLetters) {
    for (int i = 0; i < count; i++) {
      SerializedEvent serializedEvent =
          i == 0
              ? new SerializedEvent(null, null, null)
              : new SerializedEvent(
                  ("key-" + i).getBytes(),
                  ("value-" + i).getBytes(),
                  Map.of("metadata_value", "metadata_value"));
      dlqWriter.writeToDlq(i, serializedEvent, "msg" + i, "test-node");
      if (writtenDeadLetters != null) {
        writtenDeadLetters.add(serializedEvent);
      }
    }
  }

  private List<DeadLetter> readRawDeadLettersFromGcs(String objectName) throws IOException {
    byte[] data = gcsClient.readAllBytes(BUCKET_NAME, objectName);
    SpecificDatumReader<DeadLetter> datumReader = new SpecificDatumReader<>(DeadLetter.class);
    try (SeekableByteArrayInput input = new SeekableByteArrayInput(data);
        DataFileReader<DeadLetter> reader = new DataFileReader<>(input, datumReader)) {
      List<DeadLetter> deadLetters = new ArrayList<>();
      while (reader.hasNext()) {
        deadLetters.add(reader.next());
      }
      return deadLetters;
    }
  }

  private List<SerializedEvent> readDeadLettersFromGcs(String objectName) throws IOException {
    byte[] data = gcsClient.readAllBytes(BUCKET_NAME, objectName);
    SpecificDatumReader<DeadLetter> datumReader = new SpecificDatumReader<>(DeadLetter.class);
    try (SeekableByteArrayInput input = new SeekableByteArrayInput(data);
        DataFileReader<DeadLetter> reader = new DataFileReader<>(input, datumReader)) {
      List<SerializedEvent> deadLetters = new ArrayList<>();
      while (reader.hasNext()) {
        DeadLetter deadLetter = reader.next();
        deadLetters.add(
            new SerializedEvent(
                Optional.ofNullable(deadLetter.getKey()).map(ByteBuffer::array).orElse(null),
                Optional.ofNullable(deadLetter.getValue()).map(ByteBuffer::array).orElse(null),
                deadLetter.getMetadata()));
      }
      return deadLetters;
    }
  }

  private void deleteAllObjectsInBucket() {
    try {
      for (Blob blob : gcsClient.list(BUCKET_NAME).iterateAll()) {
        gcsClient.delete(blob.getBlobId());
      }
    } catch (Exception e) {
      // ignore cleanup errors
    }
  }
}
```

- [ ] **Step 2: Run the integration tests**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.dlq.GcsDlqWriterTest"`
Expected: All tests PASS (requires Docker running)

- [ ] **Step 3: Commit**

```bash
git add lib/src/test/java/io/fleak/zephflow/lib/dlq/GcsDlqWriterTest.java
git commit -m "Add GcsDlqWriter integration tests with fake-gcs-server"
```

---

### Task 7: Create DlqWriterFactory

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/dlq/DlqWriterFactory.java`
- Create: `lib/src/test/java/io/fleak/zephflow/lib/dlq/DlqWriterFactoryTest.java`

- [ ] **Step 1: Write the factory tests**

Create `lib/src/test/java/io/fleak/zephflow/lib/dlq/DlqWriterFactoryTest.java`:

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
package io.fleak.zephflow.lib.dlq;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import org.junit.jupiter.api.Test;

class DlqWriterFactoryTest {

  @Test
  void testCreateDlqWriterWithS3Config() {
    JobContext.S3DlqConfig config =
        JobContext.S3DlqConfig.builder()
            .region("us-east-1")
            .bucket("test-bucket")
            .batchSize(10)
            .flushIntervalMillis(1000)
            .accessKeyId("testKey")
            .secretAccessKey("testSecret")
            .s3EndpointOverride("http://localhost:9000")
            .build();
    DlqWriter writer = DlqWriterFactory.createDlqWriter(config, "test-prefix");
    assertInstanceOf(S3DlqWriter.class, writer);
    writer.close();
  }

  @Test
  void testCreateSampleWriterWithS3Config() {
    JobContext.S3DlqConfig config =
        JobContext.S3DlqConfig.builder()
            .region("us-east-1")
            .bucket("test-bucket")
            .batchSize(10)
            .flushIntervalMillis(1000)
            .accessKeyId("testKey")
            .secretAccessKey("testSecret")
            .s3EndpointOverride("http://localhost:9000")
            .build();
    DlqWriter writer = DlqWriterFactory.createSampleWriter(config, "test-prefix");
    assertInstanceOf(S3DlqWriter.class, writer);
    writer.close();
  }

  @Test
  void testCreateDlqWriterWithUnsupportedConfigThrows() {
    JobContext.DlqConfig unknownConfig = new JobContext.DlqConfig() {};
    assertThrows(
        UnsupportedOperationException.class,
        () -> DlqWriterFactory.createDlqWriter(unknownConfig, "test-prefix"));
  }

  @Test
  void testCreateSampleWriterWithUnsupportedConfigThrows() {
    JobContext.DlqConfig unknownConfig = new JobContext.DlqConfig() {};
    assertThrows(
        UnsupportedOperationException.class,
        () -> DlqWriterFactory.createSampleWriter(unknownConfig, "test-prefix"));
  }
}
```

Note: GCS config tests are omitted here because `createGcsDlqWriter` requires a valid service account JSON to build the `Storage` client. The GCS path is already tested by `GcsDlqWriterTest` integration tests.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.dlq.DlqWriterFactoryTest"`
Expected: FAIL (class not found)

- [ ] **Step 3: Write the DlqWriterFactory implementation**

Create `lib/src/main/java/io/fleak/zephflow/lib/dlq/DlqWriterFactory.java`:

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
package io.fleak.zephflow.lib.dlq;

import io.fleak.zephflow.api.JobContext;

public class DlqWriterFactory {

  public static DlqWriter createDlqWriter(JobContext.DlqConfig dlqConfig, String keyPrefix) {
    if (dlqConfig instanceof JobContext.S3DlqConfig s3DlqConfig) {
      return S3DlqWriter.createS3DlqWriter(s3DlqConfig, keyPrefix);
    }
    if (dlqConfig instanceof JobContext.GcsDlqConfig gcsDlqConfig) {
      return GcsDlqWriter.createGcsDlqWriter(gcsDlqConfig, keyPrefix);
    }
    throw new UnsupportedOperationException("unsupported dlq type: " + dlqConfig);
  }

  public static DlqWriter createSampleWriter(JobContext.DlqConfig dlqConfig, String keyPrefix) {
    if (dlqConfig instanceof JobContext.S3DlqConfig s3DlqConfig) {
      return S3DlqWriter.createS3SampleWriter(s3DlqConfig, keyPrefix);
    }
    if (dlqConfig instanceof JobContext.GcsDlqConfig gcsDlqConfig) {
      return GcsDlqWriter.createGcsSampleWriter(gcsDlqConfig, keyPrefix);
    }
    throw new UnsupportedOperationException("unsupported dlq type: " + dlqConfig);
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.dlq.DlqWriterFactoryTest"`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/dlq/DlqWriterFactory.java lib/src/test/java/io/fleak/zephflow/lib/dlq/DlqWriterFactoryTest.java
git commit -m "Add DlqWriterFactory to centralize DLQ dispatch logic"
```

---

### Task 8: Migrate source commands to DlqWriterFactory

**Files:**
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/kafkasource/KafkaSourceCommand.java` (lines 109-114, 72-76)
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/kinesissource/KinesisSourceCommand.java` (lines 117-122, 66-70)
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/splunksource/SplunkSourceCommand.java` (lines 107-112, 65-69)
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/filesource/FileSourceCommand.java` (lines 89-94, 63-67)
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/stdin/StdInSourceCommand.java` (lines 88-93, 62-66)
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/reader/ReaderCommand.java` (lines 82-87, 63-67)
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/syslogudp/SyslogUdpCommand.java` (lines 95-100, 72-76)

For each of these 7 files, apply the same mechanical transformation:

- [ ] **Step 1: Replace createDlqWriter in all source commands**

In each file:

1. **Remove** the private `createDlqWriter` method entirely (the one that does `instanceof S3DlqConfig` check)
2. **Remove** the `import io.fleak.zephflow.lib.dlq.S3DlqWriter;` line
3. **Add** the import `import io.fleak.zephflow.lib.dlq.DlqWriterFactory;`
4. **Replace** the call site `.map(c -> createDlqWriter(c, keyPrefix))` with `.map(c -> DlqWriterFactory.createDlqWriter(c, keyPrefix))`

For example, in `KafkaSourceCommand.java`:

Replace the call site (around line 75):
```java
.map(c -> createDlqWriter(c, keyPrefix))
```
with:
```java
.map(c -> DlqWriterFactory.createDlqWriter(c, keyPrefix))
```

Delete the method (lines 109-114):
```java
private DlqWriter createDlqWriter(JobContext.DlqConfig dlqConfig, String keyPrefix) {
    if (dlqConfig instanceof JobContext.S3DlqConfig s3DlqConfig) {
      return S3DlqWriter.createS3DlqWriter(s3DlqConfig, keyPrefix);
    }
    throw new UnsupportedOperationException("unsupported dlq type: " + dlqConfig);
  }
```

Replace import:
```java
// Remove:
import io.fleak.zephflow.lib.dlq.S3DlqWriter;
// Add:
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
```

Apply this identical pattern to all 7 files.

- [ ] **Step 2: Verify compilation**

Run: `./gradlew :lib:compileJava`
Expected: BUILD SUCCESSFUL

- [ ] **Step 3: Run existing tests for affected commands**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.kafkasource.*" --tests "io.fleak.zephflow.lib.commands.kinesissource.*" --tests "io.fleak.zephflow.lib.commands.splunksource.*" --tests "io.fleak.zephflow.lib.commands.filesource.*" --tests "io.fleak.zephflow.lib.commands.stdin.*" --tests "io.fleak.zephflow.lib.commands.reader.*" --tests "io.fleak.zephflow.lib.commands.syslogudp.*"`
Expected: All existing tests PASS

- [ ] **Step 4: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/kafkasource/KafkaSourceCommand.java \
  lib/src/main/java/io/fleak/zephflow/lib/commands/kinesissource/KinesisSourceCommand.java \
  lib/src/main/java/io/fleak/zephflow/lib/commands/splunksource/SplunkSourceCommand.java \
  lib/src/main/java/io/fleak/zephflow/lib/commands/filesource/FileSourceCommand.java \
  lib/src/main/java/io/fleak/zephflow/lib/commands/stdin/StdInSourceCommand.java \
  lib/src/main/java/io/fleak/zephflow/lib/commands/reader/ReaderCommand.java \
  lib/src/main/java/io/fleak/zephflow/lib/commands/syslogudp/SyslogUdpCommand.java
git commit -m "Migrate source commands to use DlqWriterFactory"
```

---

### Task 9: Migrate sink commands to DlqWriterFactory

**Files:**
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/s3/S3SinkCommand.java` (lines 94-95)
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/databrickssink/DatabricksSinkCommand.java` (lines 94-106)
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/deltalakesink/DeltaLakeSinkCommand.java` (lines 79-92)

These sink commands have a slightly different pattern than sources — they handle null dlqConfig and call `writer.open()` inline.

- [ ] **Step 1: Update S3SinkCommand**

In `lib/src/main/java/io/fleak/zephflow/lib/commands/s3/S3SinkCommand.java`:

Replace import:
```java
// Remove:
import io.fleak.zephflow.lib.dlq.S3DlqWriter;
// Add:
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
```

Replace the DLQ writer creation block (around lines 93-96):
```java
if (jobContext.getDlqConfig() instanceof JobContext.S3DlqConfig s3DlqConfig) {
    dlqWriter = S3DlqWriter.createS3DlqWriter(s3DlqConfig, keyPrefix);
}
```
with:
```java
if (jobContext.getDlqConfig() != null) {
    dlqWriter = DlqWriterFactory.createDlqWriter(jobContext.getDlqConfig(), keyPrefix);
}
```

- [ ] **Step 2: Update DatabricksSinkCommand**

In `lib/src/main/java/io/fleak/zephflow/lib/commands/databrickssink/DatabricksSinkCommand.java`:

Replace import:
```java
// Remove:
import io.fleak.zephflow.lib.dlq.S3DlqWriter;
// Add:
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
```

Replace the `createDlqWriter()` method (lines 94-106):
```java
private DlqWriter createDlqWriter() {
    JobContext.DlqConfig dlqConfig = jobContext.getDlqConfig();
    if (dlqConfig == null) {
      return null;
    }
    if (dlqConfig instanceof JobContext.S3DlqConfig s3DlqConfig) {
      String keyPrefix = (String) jobContext.getOtherProperties().get(JobContext.DATA_KEY_PREFIX);
      DlqWriter writer = S3DlqWriter.createS3DlqWriter(s3DlqConfig, keyPrefix);
      writer.open();
      return writer;
    }
    throw new UnsupportedOperationException("Unsupported DLQ type: " + dlqConfig);
  }
```
with:
```java
private DlqWriter createDlqWriter() {
    JobContext.DlqConfig dlqConfig = jobContext.getDlqConfig();
    if (dlqConfig == null) {
      return null;
    }
    String keyPrefix = (String) jobContext.getOtherProperties().get(JobContext.DATA_KEY_PREFIX);
    DlqWriter writer = DlqWriterFactory.createDlqWriter(dlqConfig, keyPrefix);
    writer.open();
    return writer;
  }
```

- [ ] **Step 3: Update DeltaLakeSinkCommand**

In `lib/src/main/java/io/fleak/zephflow/lib/commands/deltalakesink/DeltaLakeSinkCommand.java`:

Replace import:
```java
// Remove:
import io.fleak.zephflow.lib.dlq.S3DlqWriter;
// Add:
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
```

Replace the `createDlqWriter()` method (lines 79-92):
```java
private DlqWriter createDlqWriter() {
    JobContext.DlqConfig dlqConfig = jobContext.getDlqConfig();
    if (dlqConfig == null) {
      return null;
    }
    if (dlqConfig instanceof JobContext.S3DlqConfig s3DlqConfig) {
      String keyPrefix = (String) jobContext.getOtherProperties().get(JobContext.DATA_KEY_PREFIX);
      DlqWriter writer = S3DlqWriter.createS3DlqWriter(s3DlqConfig, keyPrefix);
      writer.open();
      return writer;
    }
    log.warn("Unsupported DLQ config type: {}", dlqConfig.getClass());
    return null;
  }
```
with:
```java
private DlqWriter createDlqWriter() {
    JobContext.DlqConfig dlqConfig = jobContext.getDlqConfig();
    if (dlqConfig == null) {
      return null;
    }
    String keyPrefix = (String) jobContext.getOtherProperties().get(JobContext.DATA_KEY_PREFIX);
    DlqWriter writer = DlqWriterFactory.createDlqWriter(dlqConfig, keyPrefix);
    writer.open();
    return writer;
  }
```

- [ ] **Step 4: Verify compilation**

Run: `./gradlew :lib:compileJava`
Expected: BUILD SUCCESSFUL

- [ ] **Step 5: Run existing tests for affected sink commands**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.s3.S3SinkCommandTest" --tests "io.fleak.zephflow.lib.commands.databrickssink.DatabricksSinkCommandTest" --tests "io.fleak.zephflow.lib.commands.deltalakesink.*"`
Expected: All existing tests PASS

- [ ] **Step 6: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/s3/S3SinkCommand.java \
  lib/src/main/java/io/fleak/zephflow/lib/commands/databrickssink/DatabricksSinkCommand.java \
  lib/src/main/java/io/fleak/zephflow/lib/commands/deltalakesink/DeltaLakeSinkCommand.java
git commit -m "Migrate sink commands to use DlqWriterFactory"
```

---

### Task 10: Migrate SimpleSourceCommand raw data sampler to DlqWriterFactory

**Files:**
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/source/SimpleSourceCommand.java` (lines 195-204)

- [ ] **Step 1: Update createRawDataSampler to use DlqWriterFactory**

In `lib/src/main/java/io/fleak/zephflow/lib/commands/source/SimpleSourceCommand.java`:

Replace import:
```java
// Remove:
import io.fleak.zephflow.lib.dlq.S3DlqWriter;
// Add:
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
```

Replace the `createRawDataSampler` method (lines 195-204):
```java
private RawDataSampler<T> createRawDataSampler(RawDataEncoder<T> encoder) {
    JobContext.DlqConfig dlqConfig = jobContext.getDlqConfig();
    if (!(dlqConfig instanceof JobContext.S3DlqConfig s3DlqConfig)) {
      return RawDataSampler.noOp();
    }
    String keyPrefix = (String) jobContext.getOtherProperties().get(JobContext.DATA_KEY_PREFIX);
    DlqWriter sampleWriter = S3DlqWriter.createS3SampleWriter(s3DlqConfig, keyPrefix);
    return new RawDataSampler<>(
        encoder, sampleWriter, nodeId, s3DlqConfig.getRawDataSampleIntervalMs());
  }
```
with:
```java
private RawDataSampler<T> createRawDataSampler(RawDataEncoder<T> encoder) {
    JobContext.DlqConfig dlqConfig = jobContext.getDlqConfig();
    if (dlqConfig == null) {
      return RawDataSampler.noOp();
    }
    long sampleIntervalMs = getSampleIntervalMs(dlqConfig);
    String keyPrefix = (String) jobContext.getOtherProperties().get(JobContext.DATA_KEY_PREFIX);
    DlqWriter sampleWriter = DlqWriterFactory.createSampleWriter(dlqConfig, keyPrefix);
    return new RawDataSampler<>(encoder, sampleWriter, nodeId, sampleIntervalMs);
  }

  private static long getSampleIntervalMs(JobContext.DlqConfig dlqConfig) {
    if (dlqConfig instanceof JobContext.S3DlqConfig s3) {
      return s3.getRawDataSampleIntervalMs();
    }
    if (dlqConfig instanceof JobContext.GcsDlqConfig gcs) {
      return gcs.getRawDataSampleIntervalMs();
    }
    return 60_000;
  }
```

- [ ] **Step 2: Verify compilation**

Run: `./gradlew :lib:compileJava`
Expected: BUILD SUCCESSFUL

- [ ] **Step 3: Run existing source command tests**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.source.*"`
Expected: All tests PASS

- [ ] **Step 4: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/source/SimpleSourceCommand.java
git commit -m "Migrate SimpleSourceCommand raw data sampler to DlqWriterFactory"
```

---

### Task 11: Run full test suite and verify

- [ ] **Step 1: Run the full lib test suite**

Run: `./gradlew :lib:test`
Expected: All tests PASS

- [ ] **Step 2: Run the full project build**

Run: `./gradlew build`
Expected: BUILD SUCCESSFUL

- [ ] **Step 3: Verify no remaining references to S3DlqWriter outside of S3DlqWriter itself and tests**

Search for direct S3DlqWriter usage in command classes (should find none):
```bash
grep -r "S3DlqWriter" lib/src/main/java/io/fleak/zephflow/lib/commands/ --include="*.java"
```
Expected: No results

Search for remaining `DeadLetterS3CommiterSerializer` references (should find none):
```bash
grep -r "DeadLetterS3CommiterSerializer" lib/src/main/java/ --include="*.java"
```
Expected: No results

- [ ] **Step 4: Final commit if any formatting fixes needed**

Run: `./gradlew spotlessApply`
Then if changes:
```bash
git add -A
git commit -m "Apply spotless formatting"
```
