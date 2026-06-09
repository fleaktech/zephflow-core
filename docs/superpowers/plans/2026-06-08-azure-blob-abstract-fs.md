# Azure Blob Abstract FS Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add Azure Blob Storage as a first-class `FsBackend` under the abstract FS system, using native `https://` Azure URNs, with both connection-string and account-key auth, then delete the legacy `azureblobsource` package.

**Architecture:** A new `backend/azblob/` package implements `FsBackend` / `FileLister` / `FileReader` exactly like S3 and GCS. `FileKey.backend` is set to `"azblob"` explicitly (not derived from the https:// URN scheme). Credential resolution (connection string vs. credentialId lookup) happens in `FsSourceCommand` which has `JobContext`; the resolved values are stored in `AzureBackendConfig`. `PostActions` gains an `"azblob"` case for delete and move.

**Tech Stack:** Azure Storage Blob SDK (`com.azure:azure-storage-blob` v12.28.0, already in build.gradle), Azurite Docker image for integration tests via Testcontainers `GenericContainer`.

---

## File Map

| Action | File |
|--------|------|
| Create | `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/azblob/AzureBackendConfig.java` |
| Create | `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/azblob/AzureBackend.java` |
| Create | `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/azblob/AzureLister.java` |
| Create | `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/azblob/AzureReader.java` |
| Modify | `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/PostActions.java` |
| Modify | `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommandFactory.java` |
| Modify | `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommand.java` |
| Modify | `lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java` |
| Modify | `lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java` |
| Delete | `lib/src/main/java/io/fleak/zephflow/lib/commands/azureblobsource/` (entire package) |
| Create | `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/azblob/AzureBackendIntegrationTest.java` |

---

### Task 1: Create git branch

**Files:** none

- [ ] **Step 1: Create and switch to feature branch**

```bash
git checkout -b feature/azure-blob-abstract-fs
```

Expected: `Switched to a new branch 'feature/azure-blob-abstract-fs'`

---

### Task 2: Create `AzureBackendConfig`

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/azblob/AzureBackendConfig.java`

- [ ] **Step 1: Write failing test for config construction**

File: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/azblob/AzureBackendIntegrationTest.java`

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
package io.fleak.zephflow.lib.commands.fssource.backend.azblob;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
class AzureBackendIntegrationTest {

  @Test
  void configHoldsConnectionString() {
    AzureBackendConfig cfg = new AzureBackendConfig("DefaultEndpoints=...", null, null);
    assertEquals("DefaultEndpoints=...", cfg.connectionString());
    assertNull(cfg.accountName());
    assertNull(cfg.accountKey());
  }

  @Test
  void configHoldsAccountKey() {
    AzureBackendConfig cfg = new AzureBackendConfig(null, "myaccount", "mykey");
    assertNull(cfg.connectionString());
    assertEquals("myaccount", cfg.accountName());
    assertEquals("mykey", cfg.accountKey());
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /Users/dan/fleak/zephflow-core && ./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.backend.azblob.AzureBackendIntegrationTest.configHoldsConnectionString" 2>&1 | tail -20
```

Expected: compilation failure — `AzureBackendConfig` does not exist.

- [ ] **Step 3: Create `AzureBackendConfig`**

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
package io.fleak.zephflow.lib.commands.fssource.backend.azblob;

import io.fleak.zephflow.lib.commands.fssource.api.FsBackendConfig;

public record AzureBackendConfig(String connectionString, String accountName, String accountKey)
    implements FsBackendConfig {}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /Users/dan/fleak/zephflow-core && ./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.backend.azblob.AzureBackendIntegrationTest.configHolds*" 2>&1 | tail -20
```

Expected: `2 tests completed, 0 failed`

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/azblob/AzureBackendConfig.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/azblob/AzureBackendIntegrationTest.java
git commit -m "feat(fssource): add AzureBackendConfig record"
```

---

### Task 3: Create `AzureBackend`

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/azblob/AzureBackend.java`

- [ ] **Step 1: Create `AzureBackend`**

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
package io.fleak.zephflow.lib.commands.fssource.backend.azblob;

import com.azure.storage.blob.BlobServiceClient;
import io.fleak.zephflow.lib.azure.AzureClientFactory;
import io.fleak.zephflow.lib.commands.fssource.api.*;
import java.util.Set;

public final class AzureBackend implements FsBackend {

  public static final String SCHEME = "azblob";

  private static final AzureClientFactory FACTORY = new AzureClientFactory();

  @Override
  public String scheme() {
    return SCHEME;
  }

  @Override
  public FileLister createLister(FsBackendConfig cfg) {
    return new AzureLister(client((AzureBackendConfig) cfg));
  }

  @Override
  public FileReader createReader(FsBackendConfig cfg) {
    return new AzureReader(client((AzureBackendConfig) cfg));
  }

  @Override
  public Set<Capability> capabilities() {
    return Set.of(Capability.DELETE, Capability.MOVE, Capability.RANGE_READ);
  }

  public static BlobServiceClient client(AzureBackendConfig cfg) {
    if (cfg.connectionString() != null && !cfg.connectionString().isBlank()) {
      return FACTORY.createBlobServiceClientFromConnectionString(cfg.connectionString());
    }
    return FACTORY.createBlobServiceClientFromAccountKey(cfg.accountName(), cfg.accountKey());
  }
}
```

- [ ] **Step 2: Verify it compiles**

```bash
cd /Users/dan/fleak/zephflow-core && ./gradlew :lib:compileJava 2>&1 | tail -20
```

Expected: `BUILD SUCCESSFUL`

- [ ] **Step 3: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/azblob/AzureBackend.java
git commit -m "feat(fssource): add AzureBackend (stub — no lister/reader yet)"
```

---

### Task 4: Create `AzureLister`

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/azblob/AzureLister.java`
- Modify: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/azblob/AzureBackendIntegrationTest.java`

URN convention: `FileKey.backend = "azblob"`, `FileKey.urn = https://account.blob.core.windows.net/container/blobName`

URL parsing for `https://account.blob.core.windows.net/container/prefix/`:
- `URI.create(urn).getHost()` → `account.blob.core.windows.net`
- `URI.create(urn).getPath().substring(1)` → `container/prefix/`  (strip leading `/`)
- First path segment = container name
- Remaining path = blob prefix

- [ ] **Step 1: Add listing test to `AzureBackendIntegrationTest`**

Replace the full test file content:

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
package io.fleak.zephflow.lib.commands.fssource.backend.azblob;

import static org.junit.jupiter.api.Assertions.*;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import io.fleak.zephflow.lib.azure.AzureClientFactory;
import io.fleak.zephflow.lib.commands.fssource.api.*;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.regex.Pattern;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Tag("integration")
@Testcontainers
class AzureBackendIntegrationTest {

  private static final int AZURITE_BLOB_PORT = 10000;
  private static final String AZURITE_ACCOUNT = "devstoreaccount1";
  private static final String AZURITE_KEY =
      "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KanKHom+oSA==";

  @Container
  static GenericContainer<?> AZURITE =
      new GenericContainer<>(DockerImageName.parse("mcr.microsoft.com/azure-storage/azurite:3.33.0"))
          .withExposedPorts(AZURITE_BLOB_PORT)
          .withCommand("azurite-blob", "--blobHost", "0.0.0.0");

  private static String connectionString() {
    return "DefaultEndpointsProtocol=http;AccountName="
        + AZURITE_ACCOUNT
        + ";AccountKey="
        + AZURITE_KEY
        + ";BlobEndpoint=http://"
        + AZURITE.getHost()
        + ":"
        + AZURITE.getMappedPort(AZURITE_BLOB_PORT)
        + "/"
        + AZURITE_ACCOUNT
        + ";";
  }

  private static BlobServiceClient serviceClient() {
    return new AzureClientFactory().createBlobServiceClientFromConnectionString(connectionString());
  }

  @BeforeEach
  void setup() {
    BlobServiceClient svc = serviceClient();
    BlobContainerClient container = svc.createBlobContainerIfNotExists("test-bkt");
    container.getBlobClient("data/evt_1.log").upload(
        new java.io.ByteArrayInputStream("hello".getBytes(StandardCharsets.UTF_8)), 5, true);
    container.getBlobClient("data/evt_2.log").upload(
        new java.io.ByteArrayInputStream("world".getBytes(StandardCharsets.UTF_8)), 5, true);
    container.getBlobClient("data/skip.txt").upload(
        new java.io.ByteArrayInputStream("nope".getBytes(StandardCharsets.UTF_8)), 4, true);
  }

  @AfterEach
  void teardown() {
    serviceClient().deleteBlobContainerIfExists("test-bkt");
  }

  private static String rootUrn() {
    // Use canonical Azure URL form; the BlobServiceClient routes through Azurite via connection string
    return "https://" + AZURITE_ACCOUNT + ".blob.core.windows.net/test-bkt/data/";
  }

  @Test
  void listsMatchingBlobs() {
    AzureBackendConfig cfg = new AzureBackendConfig(connectionString(), null, null);
    AzureBackend backend = new AzureBackend();
    FileLister lister = backend.createLister(cfg);

    List<FileEntry> entries =
        lister
            .list(new ListRequest(rootUrn(), Pattern.compile("evt_\\d+\\.log")))
            .toList();

    assertEquals(2, entries.size());
    assertTrue(entries.stream().allMatch(e -> e.key().backend().equals("azblob")));
    assertTrue(entries.stream().allMatch(e -> e.key().urn().startsWith("https://")));
  }

  @Test
  void listsAndReadsBlob() throws Exception {
    AzureBackendConfig cfg = new AzureBackendConfig(connectionString(), null, null);
    AzureBackend backend = new AzureBackend();
    FileLister lister = backend.createLister(cfg);
    FileReader reader = backend.createReader(cfg);

    List<FileEntry> entries =
        lister
            .list(new ListRequest(rootUrn(), Pattern.compile("evt_\\d+\\.log")))
            .toList();
    assertEquals(2, entries.size());

    try (InputStream in = reader.open(entries.get(0).key(), 0)) {
      String body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      assertTrue(body.equals("hello") || body.equals("world"));
    }
  }

  @Test
  void readsWithOffset() throws Exception {
    AzureBackendConfig cfg = new AzureBackendConfig(connectionString(), null, null);
    AzureBackend backend = new AzureBackend();
    FileLister lister = backend.createLister(cfg);
    FileReader reader = backend.createReader(cfg);

    List<FileEntry> all =
        lister.list(new ListRequest(rootUrn(), null)).toList();
    FileEntry evt1 =
        all.stream()
            .filter(e -> e.displayPath().endsWith("evt_1.log"))
            .findFirst()
            .orElseThrow();

    try (InputStream in = reader.open(evt1.key(), 2)) {
      String body = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      assertEquals("llo", body);
    }
  }

  @Test
  void schemeIsAzblob() {
    assertEquals("azblob", new AzureBackend().scheme());
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /Users/dan/fleak/zephflow-core && ./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.backend.azblob.AzureBackendIntegrationTest.listsMatchingBlobs" -Dgroups=integration 2>&1 | tail -30
```

Expected: compilation failure — `AzureLister` does not exist.

- [ ] **Step 3: Create `AzureLister`**

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
package io.fleak.zephflow.lib.commands.fssource.backend.azblob;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import io.fleak.zephflow.lib.commands.fssource.api.*;
import java.net.URI;
import java.time.Instant;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public final class AzureLister implements FileLister {

  private final BlobServiceClient serviceClient;

  public AzureLister(BlobServiceClient serviceClient) {
    this.serviceClient = serviceClient;
  }

  @Override
  public Stream<FileEntry> list(ListRequest req) {
    URI uri = URI.create(req.root());
    String host = uri.getHost(); // e.g. account.blob.core.windows.net
    String path = uri.getPath().substring(1); // strip leading /
    int slash = path.indexOf('/');
    String container = slash < 0 ? path : path.substring(0, slash);
    String prefix = slash < 0 ? "" : path.substring(slash + 1);

    ListBlobsOptions options = new ListBlobsOptions().setPrefix(prefix);
    return StreamSupport.stream(
            serviceClient
                .getBlobContainerClient(container)
                .listBlobs(options, null)
                .spliterator(),
            false)
        .filter(
            b ->
                req.fileNameRegex() == null
                    || req.fileNameRegex().matcher(filename(b.getName())).matches())
        .map(b -> toEntry(host, container, b));
  }

  @Override
  public FileEntry stat(FileKey key) {
    URI uri = URI.create(key.urn());
    String path = uri.getPath().substring(1);
    int slash = path.indexOf('/');
    String container = path.substring(0, slash);
    String blobName = path.substring(slash + 1);
    var props =
        serviceClient
            .getBlobContainerClient(container)
            .getBlobClient(blobName)
            .getProperties();
    return new FileEntry(
        key,
        props.getBlobSize(),
        props.getLastModified() == null ? Instant.EPOCH : props.getLastModified().toInstant(),
        key.urn());
  }

  private static FileEntry toEntry(String host, String container, BlobItem b) {
    String urn = "https://" + host + "/" + container + "/" + b.getName();
    return new FileEntry(
        new FileKey(AzureBackend.SCHEME, urn),
        b.getProperties().getContentLength() == null ? 0 : b.getProperties().getContentLength(),
        b.getProperties().getLastModified() == null
            ? Instant.EPOCH
            : b.getProperties().getLastModified().toInstant(),
        urn);
  }

  private static String filename(String name) {
    int i = name.lastIndexOf('/');
    return i < 0 ? name : name.substring(i + 1);
  }
}
```

- [ ] **Step 4: Run listing test**

```bash
cd /Users/dan/fleak/zephflow-core && ./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.backend.azblob.AzureBackendIntegrationTest.listsMatchingBlobs" -Dgroups=integration 2>&1 | tail -30
```

Expected: `1 test completed, 0 failed`

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/azblob/AzureLister.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/azblob/AzureBackendIntegrationTest.java
git commit -m "feat(fssource): add AzureLister with native https:// URN support"
```

---

### Task 5: Create `AzureReader`

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/azblob/AzureReader.java`

- [ ] **Step 1: Create `AzureReader`**

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
package io.fleak.zephflow.lib.commands.fssource.backend.azblob;

import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobRange;
import io.fleak.zephflow.lib.commands.fssource.api.FileKey;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import java.io.InputStream;
import java.net.URI;

public final class AzureReader implements FileReader {

  private final BlobServiceClient serviceClient;

  public AzureReader(BlobServiceClient serviceClient) {
    this.serviceClient = serviceClient;
  }

  @Override
  public InputStream open(FileKey key, long offset) {
    URI uri = URI.create(key.urn());
    String path = uri.getPath().substring(1); // strip leading /
    int slash = path.indexOf('/');
    String container = path.substring(0, slash);
    String blobName = path.substring(slash + 1);

    BlobRange range = offset > 0 ? new BlobRange(offset) : new BlobRange(0);
    return serviceClient
        .getBlobContainerClient(container)
        .getBlobClient(blobName)
        .downloadWithResponse(null, range, null, false, null, null, null)
        .getValue();
  }
}
```

- [ ] **Step 2: Run read + offset tests**

```bash
cd /Users/dan/fleak/zephflow-core && ./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.backend.azblob.AzureBackendIntegrationTest.listsAndReadsBlob" --tests "io.fleak.zephflow.lib.commands.fssource.backend.azblob.AzureBackendIntegrationTest.readsWithOffset" -Dgroups=integration 2>&1 | tail -30
```

Expected: `2 tests completed, 0 failed`

- [ ] **Step 3: Run all integration tests in the class**

```bash
cd /Users/dan/fleak/zephflow-core && ./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.backend.azblob.AzureBackendIntegrationTest" -Dgroups=integration 2>&1 | tail -30
```

Expected: `4 tests completed, 0 failed`

- [ ] **Step 4: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/azblob/AzureReader.java
git commit -m "feat(fssource): add AzureReader with BlobRange offset support"
```

---

### Task 6: Add `"azblob"` cases to `PostActions`

**Files:**
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/PostActions.java`

`PostActions` already contains a `switch (file.key().backend())` for delete and moveTo. Because `FileKey.backend` is `"azblob"`, new cases must be added. Move uses server-side copy + delete (same pattern as S3/GCS).

- [ ] **Step 1: Add import and `"azblob"` case to `delete()`**

In the existing `delete()` method, after the `"gs"` case, add:

```java
case "azblob" -> {
  AzureBackendConfig ac = (AzureBackendConfig) cfg;
  java.net.URI blobUri = java.net.URI.create(file.key().urn());
  String blobPath = blobUri.getPath().substring(1);
  int blobSlash = blobPath.indexOf('/');
  String delContainer = blobPath.substring(0, blobSlash);
  String delBlobName = blobPath.substring(blobSlash + 1);
  AzureBackend.client(ac)
      .getBlobContainerClient(delContainer)
      .getBlobClient(delBlobName)
      .delete();
}
```

- [ ] **Step 2: Add `"azblob"` case to `moveTo()`**

In the existing `moveTo()` method, after the `"gs"` case, add:

```java
case "azblob" -> {
  AzureBackendConfig ac = (AzureBackendConfig) cfg;
  com.azure.storage.blob.BlobServiceClient bsc = AzureBackend.client(ac);

  java.net.URI srcUri = java.net.URI.create(file.key().urn());
  String srcPath = srcUri.getPath().substring(1);
  int srcSlash = srcPath.indexOf('/');
  String srcContainer = srcPath.substring(0, srcSlash);
  String srcBlob = srcPath.substring(srcSlash + 1);

  java.net.URI dstUri = java.net.URI.create(destinationPrefix);
  String dstPath = dstUri.getPath().substring(1);
  int dstSlash = dstPath.indexOf('/');
  String dstContainer = dstPath.substring(0, dstSlash);
  String dstPrefix = dstPath.substring(dstSlash + 1);
  String filename =
      srcBlob.contains("/") ? srcBlob.substring(srcBlob.lastIndexOf('/') + 1) : srcBlob;
  String dstBlob =
      dstPrefix.endsWith("/") ? dstPrefix + filename : dstPrefix + "/" + filename;

  com.azure.storage.blob.BlobClient dstClient =
      bsc.getBlobContainerClient(dstContainer).getBlobClient(dstBlob);
  dstClient.beginCopy(file.key().urn(), null).waitForCompletion();
  bsc.getBlobContainerClient(srcContainer).getBlobClient(srcBlob).delete();
}
```

You also need to add the import at the top of `PostActions.java`:

```java
import io.fleak.zephflow.lib.commands.fssource.backend.azblob.AzureBackend;
import io.fleak.zephflow.lib.commands.fssource.backend.azblob.AzureBackendConfig;
```

- [ ] **Step 3: Verify compilation**

```bash
cd /Users/dan/fleak/zephflow-core && ./gradlew :lib:compileJava 2>&1 | tail -20
```

Expected: `BUILD SUCCESSFUL`

- [ ] **Step 4: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/api/PostActions.java
git commit -m "feat(fssource): add azblob delete/move cases to PostActions"
```

---

### Task 7: Wire up `FsSourceCommandFactory` and `FsSourceCommand`

**Files:**
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommandFactory.java`
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommand.java`

`FsSourceCommand.buildBackendConfig()` and `buildBackendConfigForCheckpoint()` are currently private static methods that do NOT receive `JobContext`. To resolve a `credentialId` for Azure, `JobContext` must be threaded in. The changes:

1. `buildBackendConfig(FsSourceDto.Config c)` → `buildBackendConfig(FsSourceDto.Config c, JobContext jobContext)`
2. `buildBackendConfigForCheckpoint(...)` → same, add `JobContext jobContext` parameter
3. Both call sites (both in `createExecutionContext` and `buildCheckpointStore`) pass `jobContext`

- [ ] **Step 1: Register `AzureBackend` in `FsSourceCommandFactory`**

Edit `FsSourceCommandFactory.java`. The static block becomes:

```java
static {
  FsBackendRegistry.register(new LocalFsBackend());
  FsBackendRegistry.register(new S3Backend());
  FsBackendRegistry.register(new GcsBackend());
  FsBackendRegistry.register(new AzureBackend());
}
```

Add import:
```java
import io.fleak.zephflow.lib.commands.fssource.backend.azblob.AzureBackend;
```

- [ ] **Step 2: Update `FsSourceCommand`**

The complete updated private section of `FsSourceCommand.java` (replace the existing `buildBackendConfig`, `buildCheckpointStore`, and `buildBackendConfigForCheckpoint` methods):

```java
@Override
protected ExecutionContext createExecutionContext(
    MetricClientProvider mp, JobContext jc, CommandConfig cfg, String nodeId) {
  FsSourceDto.Config c = (FsSourceDto.Config) cfg;
  FsSourceExecutionContext ec = new FsSourceExecutionContext();
  ec.backend = FsBackendRegistry.get(c.getBackend());
  FsBackendConfig bc = buildBackendConfig(c, jc);
  ec.backendConfig = bc;
  ec.lister = ec.backend.createLister(bc);
  ec.reader = ec.backend.createReader(bc);
  ec.checkpointStore = buildCheckpointStore(c, ec.backend, bc, jc);
  return ec;
}

private static FsBackendConfig buildBackendConfig(FsSourceDto.Config c, JobContext jobContext) {
  return switch (c.getBackend()) {
    case "file" -> new LocalFsBackendConfig(c.getRoot());
    case "s3" -> s3BackendConfig(c.getBackendConfig());
    case "gs" -> gcsBackendConfig(c.getBackendConfig());
    case "azblob" -> azureBackendConfig(c.getBackendConfig(), jobContext);
    default -> throw new IllegalArgumentException("Unsupported backend: " + c.getBackend());
  };
}

private static S3BackendConfig s3BackendConfig(java.util.Map<String, Object> map) {
  if (map == null) map = java.util.Map.of();
  String region = (String) map.getOrDefault("region", "us-east-1");
  String credentialId = (String) map.get("credentialId");
  String endpoint = (String) map.get("s3EndpointOverride");
  return new S3BackendConfig(region, credentialId, endpoint);
}

private static GcsBackendConfig gcsBackendConfig(java.util.Map<String, Object> map) {
  if (map == null) map = java.util.Map.of();
  String serviceAccountJson = (String) map.get("serviceAccountJson");
  return new GcsBackendConfig(serviceAccountJson);
}

private static AzureBackendConfig azureBackendConfig(
    java.util.Map<String, Object> map, JobContext jobContext) {
  if (map == null) map = java.util.Map.of();
  String connectionString = (String) map.get("connectionString");
  if (connectionString != null && !connectionString.isBlank()) {
    return new AzureBackendConfig(connectionString, null, null);
  }
  String credentialId = (String) map.get("credentialId");
  if (credentialId != null && !credentialId.isBlank()) {
    io.fleak.zephflow.lib.credentials.UsernamePasswordCredential cred =
        io.fleak.zephflow.lib.utils.MiscUtils.lookupUsernamePasswordCredential(jobContext, credentialId);
    return new AzureBackendConfig(null, cred.getUsername(), cred.getPassword());
  }
  throw new IllegalArgumentException(
      "azblob backend requires either 'connectionString' or 'credentialId' in backendConfig");
}

private static CheckpointStore buildCheckpointStore(
    FsSourceDto.Config c, FsBackend sourceBackend, FsBackendConfig sourceBackendCfg, JobContext jobContext) {
  FsBackend cpBackend;
  FsBackendConfig cpCfg;
  String prefixRoot;
  if (c.getCheckpoint() != null) {
    cpBackend = FsBackendRegistry.get(c.getCheckpoint().getBackend());
    cpCfg =
        buildBackendConfigForCheckpoint(
            c.getCheckpoint().getBackend(), c.getCheckpoint().getRoot(), c.getBackendConfig(), jobContext);
    prefixRoot = c.getCheckpoint().getRoot();
  } else {
    cpBackend = sourceBackend;
    cpCfg = sourceBackendCfg;
    prefixRoot = c.getRoot();
  }
  String prefix =
      (prefixRoot.endsWith("/") ? prefixRoot : prefixRoot + "/") + "_zephflow_checkpoints/";
  return new ObjectStoreCheckpointStore(cpBackend, cpCfg, prefix);
}

private static FsBackendConfig buildBackendConfigForCheckpoint(
    String backend, String root, java.util.Map<String, Object> sourceBackendConfig, JobContext jobContext) {
  return switch (backend) {
    case "file" -> new LocalFsBackendConfig(root);
    case "s3" -> s3BackendConfig(sourceBackendConfig);
    case "gs" -> gcsBackendConfig(sourceBackendConfig);
    case "azblob" -> azureBackendConfig(sourceBackendConfig, jobContext);
    default -> throw new IllegalArgumentException("Unsupported checkpoint backend: " + backend);
  };
}
```

Add these imports to `FsSourceCommand.java`:
```java
import io.fleak.zephflow.lib.commands.fssource.backend.azblob.AzureBackendConfig;
```

- [ ] **Step 3: Verify compilation**

```bash
cd /Users/dan/fleak/zephflow-core && ./gradlew :lib:compileJava 2>&1 | tail -20
```

Expected: `BUILD SUCCESSFUL`

- [ ] **Step 4: Run the full fssource test suite (non-integration)**

```bash
cd /Users/dan/fleak/zephflow-core && ./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.*" 2>&1 | tail -20
```

Expected: all pass, 0 failures.

- [ ] **Step 5: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommandFactory.java \
        lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommand.java
git commit -m "feat(fssource): register AzureBackend and wire azblob config + credential resolution"
```

---

### Task 8: Remove legacy `azureblobsource` from registry and constants

**Files:**
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java`
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java`

- [ ] **Step 1: Remove from `OperatorCommandRegistry`**

In `OperatorCommandRegistry.java`:
- Remove import: `import io.fleak.zephflow.lib.commands.azureblobsource.AzureBlobSourceCommandFactory;`
- Remove line: `.put(COMMAND_NAME_AZURE_BLOB_SOURCE, new AzureBlobSourceCommandFactory())`

- [ ] **Step 2: Remove constant from `MiscUtils`**

In `MiscUtils.java`, remove the line:
```java
String COMMAND_NAME_AZURE_BLOB_SOURCE = "azureblobsource";
```

- [ ] **Step 3: Verify compilation**

```bash
cd /Users/dan/fleak/zephflow-core && ./gradlew :lib:compileJava 2>&1 | tail -20
```

Expected: `BUILD SUCCESSFUL` — at this point the `azureblobsource` package still exists but is no longer referenced from registries.

- [ ] **Step 4: Commit**

```bash
git add lib/src/main/java/io/fleak/zephflow/lib/commands/OperatorCommandRegistry.java \
        lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java
git commit -m "chore: deregister legacy azureblobsource command"
```

---

### Task 9: Delete `azureblobsource` package

**Files:**
- Delete: `lib/src/main/java/io/fleak/zephflow/lib/commands/azureblobsource/` (all 8 files)

The 8 files to delete:
- `AzureBlobData.java`
- `AzureBlobRawDataConverter.java`
- `AzureBlobRawDataEncoder.java`
- `AzureBlobSourceCommand.java`
- `AzureBlobSourceCommandFactory.java`
- `AzureBlobSourceConfigValidator.java`
- `AzureBlobSourceDto.java`
- `AzureBlobSourceFetcher.java`

- [ ] **Step 1: Delete the package directory**

```bash
rm -rf /Users/dan/fleak/zephflow-core/lib/src/main/java/io/fleak/zephflow/lib/commands/azureblobsource
```

- [ ] **Step 2: Verify compilation**

```bash
cd /Users/dan/fleak/zephflow-core && ./gradlew :lib:compileJava 2>&1 | tail -20
```

Expected: `BUILD SUCCESSFUL`

- [ ] **Step 3: Check for dangling references**

```bash
grep -r "azureblobsource\|AzureBlobSource\|COMMAND_NAME_AZURE_BLOB_SOURCE" \
  /Users/dan/fleak/zephflow-core/lib/src/main/java --include="*.java"
```

Expected: no output.

- [ ] **Step 4: Run full lib test suite**

```bash
cd /Users/dan/fleak/zephflow-core && ./gradlew :lib:test 2>&1 | tail -30
```

Expected: all non-integration tests pass.

- [ ] **Step 5: Commit**

```bash
git add -A lib/src/main/java/io/fleak/zephflow/lib/commands/azureblobsource/
git commit -m "chore: delete legacy azureblobsource package"
```

---

### Task 10: Run all integration tests and final verification

**Files:** none (read-only verification)

- [ ] **Step 1: Run integration tests for the new Azure backend**

```bash
cd /Users/dan/fleak/zephflow-core && ./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.backend.azblob.AzureBackendIntegrationTest" -Dgroups=integration 2>&1 | tail -30
```

Expected: `4 tests completed, 0 failed`

- [ ] **Step 2: Run fssource integration tests broadly**

```bash
cd /Users/dan/fleak/zephflow-core && ./gradlew :lib:test -Dgroups=integration --tests "io.fleak.zephflow.lib.commands.fssource.*" 2>&1 | tail -30
```

Expected: all pass.

- [ ] **Step 3: Run full lib build**

```bash
cd /Users/dan/fleak/zephflow-core && ./gradlew :lib:build 2>&1 | tail -20
```

Expected: `BUILD SUCCESSFUL`

- [ ] **Step 4: Final commit — update plan with completed status**

No code changes needed. The branch `feature/azure-blob-abstract-fs` is ready for PR.
