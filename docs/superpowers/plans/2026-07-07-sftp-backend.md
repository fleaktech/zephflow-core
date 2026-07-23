# SFTP fssource Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a read-only SFTP backend to the fssource abstract file system, alongside local, S3, GCS, and Azure Blob.

**Architecture:** New `backend/sftp/` package mirroring the Azure backend: `SftpBackendConfig` (addressing + credentials), `SftpConnection` (one lazily-connected sshj `SSHClient` per lister/reader, reconnect-once), `SftpLister` (recursive walk), `SftpReader` (offset reads), `SftpBackend` (registry entry). Config built by a static `SftpBackendConfig.from(...)` called from a new `case "sftp"` in `FsSourceCommand.buildBackendConfig()`.

**Tech Stack:** Java 17, sshj 0.40.0 (`com.hierynomus:sshj`), JUnit 5, TestContainers with `atmoz/sftp:alpine`.

**Spec:** `docs/superpowers/specs/2026-07-07-sftp-backend-design.md`

## Global Constraints

- All work on branch `sftp-backend` (created in Task 1). Never commit to `main`.
- sshj version: exactly `0.40.0`. Added to `gradle/libs.versions.toml`, referenced as `libs.sshj`.
- URN format: `sftp://host:port/absolute/path`. Default port `22` when root omits it. Scheme constant: `SftpBackendConfig.SCHEME = "sftp"`.
- Read-only: no delete/move/write of remote files anywhere in this feature.
- Every new `.java` file starts with this exact license header (copy verbatim; spotless enforces it):

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

- Before every commit: run `./gradlew :lib:spotlessApply` and include any reformatted files.
- Integration tests are tagged `@Tag("integration")` and require Docker. Run them with:
  `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.backend.sftp.SftpBackendIntegrationTest"`
- All sshj API signatures below were verified against sshj 0.40.0 sources. `net.schmizz.sshj.sftp.SFTPClient.ls()` already filters out `.` and `..`. SFTP `READDIR` returns lstat-like attributes, so symlinks report type `SYMLINK` — `isDirectory()` and `isRegularFile()` are both false for them, which gives us the spec's "skip symlinks" behavior for free.

---

### Task 1: sshj dependency + RSA private-key credential lookup

**Files:**
- Modify: `gradle/libs.versions.toml`
- Modify: `lib/build.gradle`
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/utils/MiscUtilsTest.java`

**Interfaces:**
- Consumes: existing `MiscUtils.lookupFromMapOrThrow(Map, String, Class)`; existing `io.fleak.zephflow.lib.credentials.RSAPrivateKeyCredential` (fields: `String key` (PKCS8), `String user`).
- Produces: `static RSAPrivateKeyCredential MiscUtils.lookupRSAPrivateKeyCredential(JobContext jobContext, String credentialId)` — throws `RuntimeException` when the id is missing/unresolvable. Gradle accessor `libs.sshj`.

- [ ] **Step 1: Create the branch**

```bash
git checkout -b sftp-backend
```

- [ ] **Step 2: Add sshj to the version catalog**

In `gradle/libs.versions.toml`, add to the `[versions]` section (after the `azureCoreHttpOkHttp` line):

```toml
sshj = "0.40.0"
```

Add to the `[libraries]` section (after the `azure-core-http-okhttp` line):

```toml
sshj = { module = "com.hierynomus:sshj", version.ref = "sshj" }
```

- [ ] **Step 3: Add the dependency to lib**

In `lib/build.gradle`, in the `dependencies` block, after the `implementation libs.azure.core.http.okhttp` line, add:

```groovy
  implementation libs.sshj
```

(sshj brings `bcprov-jdk18on`/`bcpkix-jdk18on` 1.80 and `slf4j-api` transitively at runtime — no extra entries needed.)

- [ ] **Step 4: Verify dependency resolves**

Run: `./gradlew :lib:dependencies --configuration runtimeClasspath | grep -A2 sshj`
Expected: `com.hierynomus:sshj:0.40.0` with bouncycastle children, no errors.

- [ ] **Step 5: Write the failing tests for the credential lookup**

Add to `lib/src/test/java/io/fleak/zephflow/lib/utils/MiscUtilsTest.java`. The test class already imports `java.util.Map` and lives in the same package as `MiscUtils`; you must add these imports:

```java
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.credentials.RSAPrivateKeyCredential;
```

Then add the tests:

```java
@Test
void lookupRSAPrivateKeyCredentialReturnsCredential() {
  RSAPrivateKeyCredential credential = new RSAPrivateKeyCredential("pkcs8-key-content", "demo");
  JobContext jobContext =
      JobContext.builder().otherProperties(Map.of("my_key_cred", credential)).build();

  RSAPrivateKeyCredential loaded =
      MiscUtils.lookupRSAPrivateKeyCredential(jobContext, "my_key_cred");

  assertEquals("pkcs8-key-content", loaded.getKey());
  assertEquals("demo", loaded.getUser());
}

@Test
void lookupRSAPrivateKeyCredentialMissingIdThrows() {
  JobContext jobContext = JobContext.builder().otherProperties(Map.of()).build();
  assertThrows(
      RuntimeException.class,
      () -> MiscUtils.lookupRSAPrivateKeyCredential(jobContext, "nope"));
}
```

- [ ] **Step 6: Run tests to verify they fail**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.utils.MiscUtilsTest"`
Expected: COMPILE FAILURE — `cannot find symbol: method lookupRSAPrivateKeyCredential`

- [ ] **Step 7: Implement the lookup helper**

In `lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java`, add import `io.fleak.zephflow.lib.credentials.RSAPrivateKeyCredential;` and add this method directly after `lookupApiKeyCredential` (`MiscUtils` is a public interface, so `static` methods are implicitly public — match the existing style exactly):

```java
/** Helper method to look up RSAPrivateKeyCredential from JobContext */
static RSAPrivateKeyCredential lookupRSAPrivateKeyCredential(
    JobContext jobContext, String credentialId) {
  Preconditions.checkNotNull(credentialId, "credentialId not provided");
  try {
    return lookupFromMapOrThrow(
        jobContext.getOtherProperties(), credentialId, RSAPrivateKeyCredential.class);
  } catch (Exception e) {
    throw new RuntimeException(
        "failed to load RSA private key credential for credentialId: " + credentialId, e);
  }
}
```

- [ ] **Step 8: Run tests to verify they pass**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.utils.MiscUtilsTest"`
Expected: PASS (all tests in the class, including pre-existing ones)

- [ ] **Step 9: Commit**

```bash
./gradlew :lib:spotlessApply
git add gradle/libs.versions.toml lib/build.gradle \
  lib/src/main/java/io/fleak/zephflow/lib/utils/MiscUtils.java \
  lib/src/test/java/io/fleak/zephflow/lib/utils/MiscUtilsTest.java
git commit -m "feat: add sshj dependency and RSA private key credential lookup"
```

---

### Task 2: SftpBackendConfig — URN parsing and credential resolution

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpBackendConfig.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpBackendConfigTest.java`

**Interfaces:**
- Consumes: `MiscUtils.lookupUsernamePasswordCredential(JobContext, String)` (existing), `MiscUtils.lookupRSAPrivateKeyCredential(JobContext, String)` (Task 1), `FsBackendConfig` marker interface, `JobContext`.
- Produces:
  - `record SftpBackendConfig(String host, int port, String username, String password, String privateKeyPkcs8, String hostKeyFingerprint) implements FsBackendConfig`
  - `public static final String SCHEME = "sftp"` (single source for the scheme string; `SftpBackend.scheme()` and lister URNs use it)
  - `static SftpBackendConfig from(String root, Map<String, Object> backendConfigMap, JobContext jobContext)` — `backendConfig` map keys: `credentialId` (password auth via `UsernamePasswordCredential`), `privateKeyCredentialId` (key auth via `RSAPrivateKeyCredential`; its `user` field is the username), `hostKeyFingerprint` (optional). Exactly one credential key required.

- [ ] **Step 1: Write the failing tests**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpBackendConfigTest.java` (license header per Global Constraints):

```java
package io.fleak.zephflow.lib.commands.fssource.backend.sftp;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.credentials.RSAPrivateKeyCredential;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import java.util.Map;
import org.junit.jupiter.api.Test;

class SftpBackendConfigTest {

  private static JobContext contextWithPasswordCred() {
    return JobContext.builder()
        .otherProperties(Map.of("cred1", new UsernamePasswordCredential("demo", "secret")))
        .build();
  }

  private static JobContext contextWithKeyCred() {
    return JobContext.builder()
        .otherProperties(Map.of("key1", new RSAPrivateKeyCredential("pkcs8-pem", "keyuser")))
        .build();
  }

  @Test
  void parsesHostAndDefaultPort() {
    SftpBackendConfig cfg =
        SftpBackendConfig.from(
            "sftp://example.com/upload", Map.of("credentialId", "cred1"), contextWithPasswordCred());
    assertEquals("example.com", cfg.host());
    assertEquals(22, cfg.port());
  }

  @Test
  void parsesExplicitPort() {
    SftpBackendConfig cfg =
        SftpBackendConfig.from(
            "sftp://example.com:2222/upload",
            Map.of("credentialId", "cred1"),
            contextWithPasswordCred());
    assertEquals(2222, cfg.port());
  }

  @Test
  void passwordCredentialPopulatesUsernameAndPassword() {
    SftpBackendConfig cfg =
        SftpBackendConfig.from(
            "sftp://example.com/upload", Map.of("credentialId", "cred1"), contextWithPasswordCred());
    assertEquals("demo", cfg.username());
    assertEquals("secret", cfg.password());
    assertNull(cfg.privateKeyPkcs8());
  }

  @Test
  void privateKeyCredentialPopulatesUsernameAndKey() {
    SftpBackendConfig cfg =
        SftpBackendConfig.from(
            "sftp://example.com/upload",
            Map.of("privateKeyCredentialId", "key1"),
            contextWithKeyCred());
    assertEquals("keyuser", cfg.username());
    assertEquals("pkcs8-pem", cfg.privateKeyPkcs8());
    assertNull(cfg.password());
  }

  @Test
  void hostKeyFingerprintIsCarriedThrough() {
    SftpBackendConfig cfg =
        SftpBackendConfig.from(
            "sftp://example.com/upload",
            Map.of("credentialId", "cred1", "hostKeyFingerprint", "SHA256:abc"),
            contextWithPasswordCred());
    assertEquals("SHA256:abc", cfg.hostKeyFingerprint());
  }

  @Test
  void bothCredentialKeysRejected() {
    IllegalArgumentException e =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                SftpBackendConfig.from(
                    "sftp://example.com/upload",
                    Map.of("credentialId", "cred1", "privateKeyCredentialId", "key1"),
                    contextWithPasswordCred()));
    assertTrue(e.getMessage().contains("exactly one"));
  }

  @Test
  void neitherCredentialKeyRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SftpBackendConfig.from(
                "sftp://example.com/upload", Map.of(), contextWithPasswordCred()));
  }

  @Test
  void nullBackendConfigMapRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () -> SftpBackendConfig.from("sftp://example.com/upload", null, contextWithPasswordCred()));
  }

  @Test
  void invalidRootRejected() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            SftpBackendConfig.from(
                "/no/scheme/here", Map.of("credentialId", "cred1"), contextWithPasswordCred()));
  }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.backend.sftp.SftpBackendConfigTest"`
Expected: COMPILE FAILURE — `package io.fleak.zephflow.lib.commands.fssource.backend.sftp does not exist` / `cannot find symbol: SftpBackendConfig`

- [ ] **Step 3: Implement SftpBackendConfig**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpBackendConfig.java` (license header per Global Constraints):

```java
package io.fleak.zephflow.lib.commands.fssource.backend.sftp;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendConfig;
import io.fleak.zephflow.lib.credentials.RSAPrivateKeyCredential;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.utils.MiscUtils;
import java.net.URI;
import java.util.Map;

public record SftpBackendConfig(
    String host,
    int port,
    String username,
    String password,
    String privateKeyPkcs8,
    String hostKeyFingerprint)
    implements FsBackendConfig {

  public static final String SCHEME = "sftp";
  public static final int DEFAULT_PORT = 22;

  public static SftpBackendConfig from(
      String root, Map<String, Object> backendConfigMap, JobContext jobContext) {
    URI uri = URI.create(root);
    if (!SCHEME.equals(uri.getScheme()) || uri.getHost() == null) {
      throw new IllegalArgumentException(
          "sftp root must be of the form sftp://host[:port]/path, got: " + root);
    }
    String host = uri.getHost();
    int port = uri.getPort() < 0 ? DEFAULT_PORT : uri.getPort();
    if (backendConfigMap == null) backendConfigMap = Map.of();
    String credentialId = (String) backendConfigMap.get("credentialId");
    String privateKeyCredentialId = (String) backendConfigMap.get("privateKeyCredentialId");
    String hostKeyFingerprint = (String) backendConfigMap.get("hostKeyFingerprint");
    boolean hasPassword = credentialId != null && !credentialId.isBlank();
    boolean hasKey = privateKeyCredentialId != null && !privateKeyCredentialId.isBlank();
    if (hasPassword == hasKey) {
      throw new IllegalArgumentException(
          "sftp backend requires exactly one of 'credentialId' or 'privateKeyCredentialId' in backendConfig");
    }
    if (hasPassword) {
      UsernamePasswordCredential credential =
          MiscUtils.lookupUsernamePasswordCredential(jobContext, credentialId);
      return new SftpBackendConfig(
          host, port, credential.getUsername(), credential.getPassword(), null, hostKeyFingerprint);
    }
    RSAPrivateKeyCredential credential =
        MiscUtils.lookupRSAPrivateKeyCredential(jobContext, privateKeyCredentialId);
    return new SftpBackendConfig(
        host, port, credential.getUser(), null, credential.getKey(), hostKeyFingerprint);
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.backend.sftp.SftpBackendConfigTest"`
Expected: PASS (9 tests)

- [ ] **Step 5: Commit**

```bash
./gradlew :lib:spotlessApply
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/ \
  lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/
git commit -m "feat: add SftpBackendConfig with URN parsing and credential resolution"
```

---

### Task 3: SftpConnection + integration test scaffold (password auth)

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpConnection.java`
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpBackendIntegrationTest.java`

**Interfaces:**
- Consumes: `SftpBackendConfig` (Task 2); sshj: `net.schmizz.sshj.SSHClient` (`connect(String, int)`, `isConnected()`, `authPassword(String, String)`, `addHostKeyVerifier(HostKeyVerifier)`, `newSFTPClient()`, `close()`), `net.schmizz.sshj.sftp.SFTPClient`, `net.schmizz.sshj.transport.verification.PromiscuousVerifier`, `net.schmizz.sshj.transport.verification.FingerprintVerifier.getInstance(String)`.
- Produces: package-private `final class SftpConnection implements AutoCloseable` with:
  - `SftpConnection(SftpBackendConfig cfg)` — no I/O in constructor
  - `synchronized SFTPClient sftp()` — lazily connects; reconnects if the SSH transport dropped; throws `UncheckedIOException` on connect/auth failure
  - `void close()` — idempotent, quiet
  - Password auth only in this task; key auth is added in Task 7 (the `authenticate()` method is the extension point).

**Docker required for the integration test.** The `atmoz/sftp` container chroots the user to `/home/demo`, so paths visible over SFTP drop that prefix: container path `/home/demo/upload/data/evt_1.log` is SFTP path `/upload/data/evt_1.log`.

- [ ] **Step 1: Write the failing integration test**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpBackendIntegrationTest.java` (license header per Global Constraints):

```java
package io.fleak.zephflow.lib.commands.fssource.backend.sftp;

import static org.junit.jupiter.api.Assertions.*;

import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.*;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Tag("integration")
@Testcontainers
class SftpBackendIntegrationTest {

  private static final int SFTP_PORT = 22;
  private static final String USER = "demo";
  private static final String PASSWORD = "secret";

  @Container
  static GenericContainer<?> SFTP =
      new GenericContainer<>(DockerImageName.parse("atmoz/sftp:alpine"))
          .withExposedPorts(SFTP_PORT)
          .withCommand(USER + ":" + PASSWORD + ":1001::upload");

  @BeforeAll
  static void seedFiles() throws Exception {
    exec("mkdir -p /home/demo/upload/data/nested");
    copy("hello", "/home/demo/upload/data/evt_1.log");
    copy("world", "/home/demo/upload/data/evt_2.log");
    copy("nope", "/home/demo/upload/data/skip.txt");
    copy("deep", "/home/demo/upload/data/nested/evt_3.log");
  }

  private static void exec(String cmd) throws Exception {
    var result = SFTP.execInContainer("sh", "-c", cmd);
    assertEquals(0, result.getExitCode(), result.getStderr());
  }

  private static void copy(String content, String containerPath) {
    SFTP.copyFileToContainer(
        Transferable.of(content.getBytes(StandardCharsets.UTF_8)), containerPath);
  }

  private static SftpBackendConfig passwordConfig() {
    return new SftpBackendConfig(
        SFTP.getHost(), SFTP.getMappedPort(SFTP_PORT), USER, PASSWORD, null, null);
  }

  private static String rootUrn() {
    return "sftp://" + SFTP.getHost() + ":" + SFTP.getMappedPort(SFTP_PORT) + "/upload/data";
  }

  @Test
  void connectsAndStatsWithPasswordAuth() throws Exception {
    try (SftpConnection connection = new SftpConnection(passwordConfig())) {
      assertNotNull(connection.sftp().stat("/upload/data/evt_1.log"));
    }
  }

  @Test
  void failsWithWrongPassword() {
    SftpBackendConfig bad =
        new SftpBackendConfig(
            SFTP.getHost(), SFTP.getMappedPort(SFTP_PORT), USER, "wrong-password", null, null);
    try (SftpConnection connection = new SftpConnection(bad)) {
      assertThrows(UncheckedIOException.class, connection::sftp);
    }
  }

  @Test
  void closeIsIdempotent() {
    SftpConnection connection = new SftpConnection(passwordConfig());
    connection.sftp();
    connection.close();
    connection.close();
  }
}
```

(`rootUrn()` is unused until Task 4 — that is expected; it keeps the container plumbing in one place.)

- [ ] **Step 2: Run test to verify it fails**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.backend.sftp.SftpBackendIntegrationTest"`
Expected: COMPILE FAILURE — `cannot find symbol: SftpConnection`

- [ ] **Step 3: Implement SftpConnection**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpConnection.java` (license header per Global Constraints):

```java
package io.fleak.zephflow.lib.commands.fssource.backend.sftp;

import java.io.IOException;
import java.io.UncheckedIOException;
import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.sftp.SFTPClient;
import net.schmizz.sshj.transport.verification.FingerprintVerifier;
import net.schmizz.sshj.transport.verification.PromiscuousVerifier;

/**
 * One lazily-established SFTP session. Reconnects once per call if the underlying SSH transport
 * has dropped. Not thread-safe beyond the synchronized accessor; each lister/reader owns its own
 * instance.
 */
final class SftpConnection implements AutoCloseable {

  private final SftpBackendConfig cfg;
  private SSHClient ssh;
  private SFTPClient sftp;

  SftpConnection(SftpBackendConfig cfg) {
    this.cfg = cfg;
  }

  synchronized SFTPClient sftp() {
    if (ssh != null && ssh.isConnected()) {
      return sftp;
    }
    closeQuietly();
    try {
      ssh = new SSHClient();
      if (cfg.hostKeyFingerprint() != null && !cfg.hostKeyFingerprint().isBlank()) {
        ssh.addHostKeyVerifier(FingerprintVerifier.getInstance(cfg.hostKeyFingerprint()));
      } else {
        ssh.addHostKeyVerifier(new PromiscuousVerifier());
      }
      ssh.connect(cfg.host(), cfg.port());
      authenticate();
      sftp = ssh.newSFTPClient();
      return sftp;
    } catch (IOException e) {
      closeQuietly();
      throw new UncheckedIOException(
          "failed to establish SFTP session to " + cfg.host() + ":" + cfg.port(), e);
    }
  }

  private void authenticate() throws IOException {
    ssh.authPassword(cfg.username(), cfg.password());
  }

  @Override
  public synchronized void close() {
    closeQuietly();
  }

  private void closeQuietly() {
    if (sftp != null) {
      try {
        sftp.close();
      } catch (IOException ignored) {
      }
      sftp = null;
    }
    if (ssh != null) {
      try {
        ssh.close();
      } catch (IOException ignored) {
      }
      ssh = null;
    }
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.backend.sftp.SftpBackendIntegrationTest"`
Expected: PASS (3 tests; needs Docker)

- [ ] **Step 5: Commit**

```bash
./gradlew :lib:spotlessApply
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpConnection.java \
  lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpBackendIntegrationTest.java
git commit -m "feat: add SftpConnection with lazy connect and password auth"
```

---

### Task 4: SftpLister — recursive listing and stat

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpLister.java`
- Modify (add tests): `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpBackendIntegrationTest.java`

**Interfaces:**
- Consumes: `SftpConnection` (Task 3), `SftpBackendConfig` (Task 2), `FileLister`/`FileEntry`/`FileKey`/`ListRequest` from `...fssource.api`; sshj: `SFTPClient.ls(String)` → `List<RemoteResourceInfo>` (`getPath()`, `getName()`, `getAttributes()`, `isDirectory()`, `isRegularFile()`), `SFTPClient.stat(String)` → `FileAttributes` (`getSize()`, `getMtime()` — epoch seconds).
- Produces: `public final class SftpLister implements FileLister` with constructor `SftpLister(SftpBackendConfig cfg)` (owns its own `SftpConnection`). URNs built as `"sftp://" + cfg.host() + ":" + cfg.port() + remotePath`. `close()` closes the connection.

- [ ] **Step 1: Write the failing tests**

Add to `SftpBackendIntegrationTest` (add imports: `io.fleak.zephflow.lib.commands.fssource.api.*`, `java.time.Instant`, `java.util.List`, `java.util.regex.Pattern`):

```java
@Test
void listsRecursivelyWithRegexFilter() {
  try (SftpLister lister = new SftpLister(passwordConfig())) {
    List<FileEntry> entries =
        lister.list(new ListRequest(rootUrn(), Pattern.compile("evt_\\d+\\.log"))).toList();

    assertEquals(3, entries.size()); // evt_1, evt_2, nested/evt_3; skip.txt filtered out
    assertTrue(entries.stream().allMatch(e -> e.key().backend().equals("sftp")));
    assertTrue(
        entries.stream()
            .anyMatch(e -> e.key().urn().endsWith("/upload/data/nested/evt_3.log")));
    assertTrue(entries.stream().allMatch(e -> e.key().urn().startsWith("sftp://")));
    assertTrue(entries.stream().allMatch(e -> e.size() > 0));
    assertTrue(entries.stream().allMatch(e -> e.lastModified().isAfter(Instant.EPOCH)));
  }
}

@Test
void listsEverythingWithoutRegex() {
  try (SftpLister lister = new SftpLister(passwordConfig())) {
    List<FileEntry> entries = lister.list(new ListRequest(rootUrn(), null)).toList();
    assertEquals(4, entries.size()); // includes skip.txt
  }
}

@Test
void statReturnsEntry() {
  try (SftpLister lister = new SftpLister(passwordConfig())) {
    String urn = rootUrn() + "/evt_1.log";
    FileEntry entry = lister.stat(FileKey.of(urn));
    assertEquals(5, entry.size()); // "hello"
    assertEquals(urn, entry.key().urn());
  }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.backend.sftp.SftpBackendIntegrationTest"`
Expected: COMPILE FAILURE — `cannot find symbol: SftpLister`

- [ ] **Step 3: Implement SftpLister**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpLister.java` (license header per Global Constraints):

```java
package io.fleak.zephflow.lib.commands.fssource.backend.sftp;

import io.fleak.zephflow.lib.commands.fssource.api.FileEntry;
import io.fleak.zephflow.lib.commands.fssource.api.FileKey;
import io.fleak.zephflow.lib.commands.fssource.api.FileLister;
import io.fleak.zephflow.lib.commands.fssource.api.ListRequest;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.stream.Stream;
import net.schmizz.sshj.sftp.FileAttributes;
import net.schmizz.sshj.sftp.RemoteResourceInfo;
import net.schmizz.sshj.sftp.SFTPClient;

public final class SftpLister implements FileLister {

  private final SftpBackendConfig cfg;
  private final SftpConnection connection;

  public SftpLister(SftpBackendConfig cfg) {
    this.cfg = cfg;
    this.connection = new SftpConnection(cfg);
  }

  @Override
  public Stream<FileEntry> list(ListRequest req) {
    String rootPath = URI.create(req.root()).getPath();
    if (rootPath == null || rootPath.isEmpty()) {
      rootPath = "/";
    }
    SFTPClient sftp = connection.sftp();
    // Eager collection: FsSourceCommand drains the stream immediately, and eager
    // traversal keeps connection state simple.
    List<FileEntry> entries = new ArrayList<>();
    Deque<String> dirs = new ArrayDeque<>();
    dirs.push(rootPath);
    try {
      while (!dirs.isEmpty()) {
        String dir = dirs.pop();
        for (RemoteResourceInfo info : sftp.ls(dir)) {
          // Symlinks report type SYMLINK: neither isDirectory() nor isRegularFile()
          // is true, so they are skipped (no cycle risk).
          if (info.isDirectory()) {
            dirs.push(info.getPath());
          } else if (info.isRegularFile()
              && (req.fileNameRegex() == null
                  || req.fileNameRegex().matcher(info.getName()).matches())) {
            entries.add(toEntry(info));
          }
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException("failed to list " + req.root(), e);
    }
    return entries.stream();
  }

  @Override
  public FileEntry stat(FileKey key) {
    String path = URI.create(key.urn()).getPath();
    try {
      FileAttributes attrs = connection.sftp().stat(path);
      return new FileEntry(
          key, attrs.getSize(), Instant.ofEpochSecond(attrs.getMtime()), key.urn());
    } catch (IOException e) {
      throw new UncheckedIOException("failed to stat " + key.urn(), e);
    }
  }

  @Override
  public void close() {
    connection.close();
  }

  private FileEntry toEntry(RemoteResourceInfo info) {
    String urn = "sftp://" + cfg.host() + ":" + cfg.port() + info.getPath();
    return new FileEntry(
        new FileKey(SftpBackendConfig.SCHEME, urn),
        info.getAttributes().getSize(),
        Instant.ofEpochSecond(info.getAttributes().getMtime()),
        urn);
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.backend.sftp.SftpBackendIntegrationTest"`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
./gradlew :lib:spotlessApply
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpLister.java \
  lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpBackendIntegrationTest.java
git commit -m "feat: add SftpLister with recursive listing and stat"
```

---

### Task 5: SftpReader — offset reads

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpReader.java`
- Modify (add tests): `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpBackendIntegrationTest.java`

**Interfaces:**
- Consumes: `SftpConnection` (Task 3), `SftpBackendConfig` (Task 2), `FileReader`/`FileKey` from `...fssource.api`; sshj: `SFTPClient.open(String)` → `RemoteFile`, inner class instantiation `remoteFile.new RemoteFileInputStream(long fileOffset)`, `RemoteFile.close()`.
- Produces: `public final class SftpReader implements FileReader` with constructor `SftpReader(SftpBackendConfig cfg)`. `open(key, offset)` returns an `InputStream` whose `close()` also closes the underlying `RemoteFile` handle. `close()` closes the connection.

- [ ] **Step 1: Write the failing tests**

Add to `SftpBackendIntegrationTest` (add import `java.io.InputStream`):

```java
@Test
void readsFullFile() throws Exception {
  try (SftpReader reader = new SftpReader(passwordConfig())) {
    try (InputStream in = reader.open(FileKey.of(rootUrn() + "/evt_1.log"), 0)) {
      assertEquals("hello", new String(in.readAllBytes(), StandardCharsets.UTF_8));
    }
  }
}

@Test
void readsFromOffset() throws Exception {
  try (SftpReader reader = new SftpReader(passwordConfig())) {
    try (InputStream in = reader.open(FileKey.of(rootUrn() + "/evt_1.log"), 2)) {
      assertEquals("llo", new String(in.readAllBytes(), StandardCharsets.UTF_8));
    }
  }
}

@Test
void openingMissingFileThrows() {
  try (SftpReader reader = new SftpReader(passwordConfig())) {
    assertThrows(
        UncheckedIOException.class,
        () -> reader.open(FileKey.of(rootUrn() + "/does_not_exist.log"), 0));
  }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.backend.sftp.SftpBackendIntegrationTest"`
Expected: COMPILE FAILURE — `cannot find symbol: SftpReader`

- [ ] **Step 3: Implement SftpReader**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpReader.java` (license header per Global Constraints):

```java
package io.fleak.zephflow.lib.commands.fssource.backend.sftp;

import io.fleak.zephflow.lib.commands.fssource.api.FileKey;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import net.schmizz.sshj.sftp.RemoteFile;

public final class SftpReader implements FileReader {

  private final SftpConnection connection;

  public SftpReader(SftpBackendConfig cfg) {
    this.connection = new SftpConnection(cfg);
  }

  @Override
  public InputStream open(FileKey key, long offset) {
    String path = URI.create(key.urn()).getPath();
    try {
      RemoteFile remoteFile = connection.sftp().open(path);
      InputStream in = remoteFile.new RemoteFileInputStream(offset);
      // Closing the returned stream must also release the remote file handle.
      return new FilterInputStream(in) {
        @Override
        public void close() throws IOException {
          try {
            super.close();
          } finally {
            remoteFile.close();
          }
        }
      };
    } catch (IOException e) {
      throw new UncheckedIOException("failed to open " + key.urn(), e);
    }
  }

  @Override
  public void close() {
    connection.close();
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.backend.sftp.SftpBackendIntegrationTest"`
Expected: PASS (9 tests)

- [ ] **Step 5: Commit**

```bash
./gradlew :lib:spotlessApply
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpReader.java \
  lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpBackendIntegrationTest.java
git commit -m "feat: add SftpReader with offset reads"
```

---

### Task 6: SftpBackend, registration, and FsSourceCommand wiring

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpBackend.java`
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommandFactory.java` (static registration block)
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommand.java` (`buildBackendConfig` switch, around line 148)
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpBackendTest.java`
- Modify (add test): `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/FsSourceRegistrationTest.java`

**Interfaces:**
- Consumes: `SftpLister` (Task 4), `SftpReader` (Task 5), `SftpBackendConfig.SCHEME` and `.from(...)` (Task 2), `FsBackend`/`FsBackendRegistry`.
- Produces: `public final class SftpBackend implements FsBackend` — `scheme()` returns `"sftp"`, `capabilities()` returns `{DELETE, MOVE, RANGE_READ}`, `createLister`/`createReader` cast the config. Registered in `FsSourceCommandFactory`. `case "sftp"` in `buildBackendConfig`.

- [ ] **Step 1: Write the failing unit test**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpBackendTest.java` (license header per Global Constraints):

```java
package io.fleak.zephflow.lib.commands.fssource.backend.sftp;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.commands.fssource.api.FsBackend;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SftpBackendTest {

  @Test
  void schemeAndCapabilities() {
    SftpBackend backend = new SftpBackend();
    assertEquals("sftp", backend.scheme());
    assertEquals(
        Set.of(
            FsBackend.Capability.DELETE,
            FsBackend.Capability.MOVE,
            FsBackend.Capability.RANGE_READ),
        backend.capabilities());
  }

  @Test
  void createsListerAndReader() {
    SftpBackend backend = new SftpBackend();
    SftpBackendConfig cfg = new SftpBackendConfig("example.com", 22, "u", "p", null, null);
    // No I/O happens until first use, so creation must succeed without a server.
    assertNotNull(backend.createLister(cfg));
    assertNotNull(backend.createReader(cfg));
  }
}
```

Add to `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/FsSourceRegistrationTest.java` (mirror the existing `localFsBackendIsRegistered` pattern — the `@BeforeEach` already forces command-registry initialization which runs the factory's static block; add a fallback registration like the existing one):

```java
@Test
void sftpBackendIsRegistered() {
  try {
    FsBackendRegistry.get("sftp");
  } catch (IllegalArgumentException ignored) {
    FsBackendRegistry.register(
        new io.fleak.zephflow.lib.commands.fssource.backend.sftp.SftpBackend());
  }
  assertNotNull(FsBackendRegistry.get("sftp"));
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.backend.sftp.SftpBackendTest" --tests "io.fleak.zephflow.lib.commands.fssource.FsSourceRegistrationTest"`
Expected: COMPILE FAILURE — `cannot find symbol: SftpBackend`

- [ ] **Step 3: Implement SftpBackend and wire it up**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpBackend.java` (license header per Global Constraints):

```java
package io.fleak.zephflow.lib.commands.fssource.backend.sftp;

import io.fleak.zephflow.lib.commands.fssource.api.FileLister;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackend;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendConfig;
import java.util.Set;

public final class SftpBackend implements FsBackend {

  @Override
  public String scheme() {
    return SftpBackendConfig.SCHEME;
  }

  @Override
  public FileLister createLister(FsBackendConfig cfg) {
    return new SftpLister((SftpBackendConfig) cfg);
  }

  @Override
  public FileReader createReader(FsBackendConfig cfg) {
    return new SftpReader((SftpBackendConfig) cfg);
  }

  @Override
  public Set<Capability> capabilities() {
    return Set.of(Capability.DELETE, Capability.MOVE, Capability.RANGE_READ);
  }
}
```

In `FsSourceCommandFactory.java`, add the import `io.fleak.zephflow.lib.commands.fssource.backend.sftp.SftpBackend;` and add to the static block after the `AzureBackend` registration:

```java
    FsBackendRegistry.register(new SftpBackend());
```

In `FsSourceCommand.java`, add the import `io.fleak.zephflow.lib.commands.fssource.backend.sftp.SftpBackendConfig;` and add a case to the `buildBackendConfig` switch after the `"azblob"` case:

```java
      case "sftp" -> SftpBackendConfig.from(
          config.getRoot(), config.getBackendConfig(), jobContext);
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.backend.sftp.SftpBackendTest" --tests "io.fleak.zephflow.lib.commands.fssource.FsSourceRegistrationTest"`
Expected: PASS

- [ ] **Step 5: Run the full fssource test suite to check for regressions**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.*"`
Expected: PASS (all existing fssource tests plus new ones)

- [ ] **Step 6: Commit**

```bash
./gradlew :lib:spotlessApply
git add lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/ \
  lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/
git commit -m "feat: register sftp backend and wire config into FsSourceCommand"
```

---

### Task 7: Key auth and host-key pinning

**Files:**
- Create: `lib/src/test/resources/sftp/test_key_pkcs8.pem` (generated, see Step 1)
- Create: `lib/src/test/resources/sftp/test_key.pub` (generated, see Step 1)
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpConnection.java` (`authenticate()` method)
- Modify (add tests): `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpBackendIntegrationTest.java`

**Interfaces:**
- Consumes: sshj `SSHClient.loadKeys(String privateKey, String publicKey, PasswordFinder)` (pass `null, null` for the last two — format auto-detected, PKCS8 supported via Bouncy Castle), `SSHClient.authPublickey(String, KeyProvider...)`, `net.schmizz.sshj.common.SecurityUtils.getFingerprint(PublicKey)` (returns MD5 colon-hex, a format `FingerprintVerifier.getInstance` accepts), `net.schmizz.sshj.transport.verification.HostKeyVerifier`.
- Produces: `SftpConnection.authenticate()` supporting both auth modes: password when `cfg.password() != null`, otherwise public key from `cfg.privateKeyPkcs8()`. Committed test keypair used ONLY for tests.

- [ ] **Step 1: Generate the test keypair (one-time, committed as test resources)**

```bash
mkdir -p lib/src/test/resources/sftp
ssh-keygen -t rsa -b 2048 -m PEM -f /tmp/zephflow_test_key -N "" -C "zephflow-sftp-test"
openssl pkcs8 -topk8 -inform PEM -outform PEM -nocrypt \
  -in /tmp/zephflow_test_key -out lib/src/test/resources/sftp/test_key_pkcs8.pem
cp /tmp/zephflow_test_key.pub lib/src/test/resources/sftp/test_key.pub
rm /tmp/zephflow_test_key /tmp/zephflow_test_key.pub
```

Verify: `head -1 lib/src/test/resources/sftp/test_key_pkcs8.pem`
Expected: `-----BEGIN PRIVATE KEY-----` (PKCS8, matching `RSAPrivateKeyCredential`'s documented format)

- [ ] **Step 2: Write the failing tests**

In `SftpBackendIntegrationTest`, first mount the public key into the container by adding this line to the `SFTP` container definition (before `.withCommand(...)`; add import `org.testcontainers.utility.MountableFile`). The atmoz/sftp entrypoint appends every file in `/home/<user>/.ssh/keys/` to `authorized_keys`, so the same `demo` user accepts both password and key auth:

```java
          .withCopyFileToContainer(
              MountableFile.forClasspathResource("sftp/test_key.pub"),
              "/home/" + USER + "/.ssh/keys/test_key.pub")
```

Then add these tests and helpers (add imports: `java.io.IOException`, `java.io.InputStream`, `java.security.PublicKey`, `java.util.List`, `java.util.Objects`, `java.util.concurrent.atomic.AtomicReference`, `net.schmizz.sshj.SSHClient`, `net.schmizz.sshj.common.SecurityUtils`, `net.schmizz.sshj.transport.verification.HostKeyVerifier`):

```java
private static String testKeyPkcs8() throws IOException {
  try (InputStream in =
      SftpBackendIntegrationTest.class.getResourceAsStream("/sftp/test_key_pkcs8.pem")) {
    return new String(Objects.requireNonNull(in).readAllBytes(), StandardCharsets.UTF_8);
  }
}

/** Connects once with an accept-all verifier that records the server's key fingerprint. */
private static String captureHostKeyFingerprint() throws IOException {
  AtomicReference<String> fingerprint = new AtomicReference<>();
  try (SSHClient probe = new SSHClient()) {
    probe.addHostKeyVerifier(
        new HostKeyVerifier() {
          @Override
          public boolean verify(String hostname, int port, PublicKey key) {
            fingerprint.set(SecurityUtils.getFingerprint(key));
            return true;
          }

          @Override
          public List<String> findExistingAlgorithms(String hostname, int port) {
            return List.of();
          }
        });
    probe.connect(SFTP.getHost(), SFTP.getMappedPort(SFTP_PORT));
  }
  return fingerprint.get();
}

@Test
void connectsWithPrivateKeyAuth() throws Exception {
  SftpBackendConfig cfg =
      new SftpBackendConfig(
          SFTP.getHost(), SFTP.getMappedPort(SFTP_PORT), USER, null, testKeyPkcs8(), null);
  try (SftpConnection connection = new SftpConnection(cfg)) {
    assertNotNull(connection.sftp().stat("/upload/data/evt_1.log"));
  }
}

@Test
void correctHostKeyFingerprintConnects() throws Exception {
  String fingerprint = captureHostKeyFingerprint();
  SftpBackendConfig cfg =
      new SftpBackendConfig(
          SFTP.getHost(), SFTP.getMappedPort(SFTP_PORT), USER, PASSWORD, null, fingerprint);
  try (SftpConnection connection = new SftpConnection(cfg)) {
    assertNotNull(connection.sftp().stat("/upload/data/evt_1.log"));
  }
}

@Test
void wrongHostKeyFingerprintFails() {
  // Valid MD5 fingerprint format, wrong value.
  String wrong = "00:11:22:33:44:55:66:77:88:99:aa:bb:cc:dd:ee:ff";
  SftpBackendConfig cfg =
      new SftpBackendConfig(
          SFTP.getHost(), SFTP.getMappedPort(SFTP_PORT), USER, PASSWORD, null, wrong);
  try (SftpConnection connection = new SftpConnection(cfg)) {
    assertThrows(UncheckedIOException.class, connection::sftp);
  }
}
```

- [ ] **Step 3: Run tests to verify key auth fails and pinning passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.backend.sftp.SftpBackendIntegrationTest"`
Expected: `connectsWithPrivateKeyAuth` FAILS (`authenticate()` calls `authPassword` with null password → auth error). The two fingerprint tests PASS already (verifier wiring was built in Task 3) — that is expected; the red step here is key auth.

- [ ] **Step 4: Implement key auth**

In `SftpConnection.java`, add import `net.schmizz.sshj.userauth.keyprovider.KeyProvider;` and replace the `authenticate()` method:

```java
  private void authenticate() throws IOException {
    if (cfg.password() != null) {
      ssh.authPassword(cfg.username(), cfg.password());
      return;
    }
    KeyProvider keyProvider = ssh.loadKeys(cfg.privateKeyPkcs8(), null, null);
    ssh.authPublickey(cfg.username(), keyProvider);
  }
```

- [ ] **Step 5: Run the full integration test class**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.backend.sftp.SftpBackendIntegrationTest"`
Expected: PASS (12 tests)

- [ ] **Step 6: Run the whole fssource suite one last time**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.*" --tests "io.fleak.zephflow.lib.utils.MiscUtilsTest"`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
./gradlew :lib:spotlessApply
git add lib/src/test/resources/sftp/ \
  lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpConnection.java \
  lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/backend/sftp/SftpBackendIntegrationTest.java
git commit -m "feat: add sftp private key auth and host key pinning"
```

---

## Verification checklist (post-implementation)

- `./gradlew :lib:test` — full lib suite green (Docker running for integration tests)
- `./gradlew :lib:spotlessCheck` — clean
- Spec cross-check: every item under "Testing" in `docs/superpowers/specs/2026-07-07-sftp-backend-design.md` has a corresponding passing test
