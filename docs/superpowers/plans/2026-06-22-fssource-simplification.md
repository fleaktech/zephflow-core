# fssource Simplification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce `fssource` to a single bounded, encoding-driven, resume-capable scan: list once, read+decode each file via the shared `EncodingType` pipeline with automatic gzip detection, emit bare records, checkpoint progress over a configurable HTTP endpoint, terminate.

**Architecture:** `FsSourceCommand` becomes a `SourceType.BATCH` source that does one listing pass. Per file it reads the whole object into bytes, auto-gunzips when gzip magic bytes are present, deserializes via `DeserializerFactory` into bare `RecordFleakData`, and saves a `FsCheckpoint` (watermark + completed-set) through a ported `CheckpointClient` (in-memory or HTTP). All unbounded/streaming, partitioning, stability-probe, post-action, and object-store-checkpoint machinery is deleted.

**Tech Stack:** Java 21, Gradle, JUnit 5, Lombok, Jackson (`JsonUtils.OBJECT_MAPPER`), `java.net.http.HttpClient`, `com.sun.net.httpserver` (tests).

## Global Constraints

- License header (the standard Apache 2.0 block present at the top of every existing `.java` file) must head every new file. Copy it verbatim from any sibling file in the same package.
- `spotlessApply` must be clean before each commit; run `./gradlew :lib:spotlessApply` (and `:api:spotlessApply` for the `JobContext` change).
- Build/test gate per task: `./gradlew :lib:compileJava :lib:compileTestJava` must pass; the task's tests must pass.
- The FS source consumes data exactly like other sources: `DeserializerFactory.createDeserializerFactory(encodingType).createDeserializer().deserialize(new SerializedEvent(null, bytes, null))`. Emit those records unchanged — no `file`/`line`/`content` fields.
- Checkpoint payload is the existing `FsCheckpoint` record serialized via `JsonUtils` (has `JavaTimeModule`, so `Instant` round-trips). Checkpoint `id` is `SourceIdHasher.compute(backend, root, fileNameRegex)`.
- HTTP checkpoint target is `{configuredUrl}/{id}` — no hardcoded `/api/v1/state` path.

---

### Task 1: Port `CheckpointClient` into core + add `JobContext.JOB_MASTER_URL`

**Files:**
- Create: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/checkpoint/CheckpointClient.java`
- Modify: `api/src/main/java/io/fleak/zephflow/api/JobContext.java` (add constant near line 35)
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/checkpoint/CheckpointClientTest.java`

**Interfaces:**
- Produces:
  - `interface CheckpointClient { void checkpoint(String id, String data); Optional<CheckpointData> loadCheckpoint(String id); record CheckpointData(long ts, String data) {} }`
  - `class CheckpointClient.InMemCheckpointClient implements CheckpointClient`
  - `class CheckpointClient.HttpCheckpointClient implements CheckpointClient` — constructor `HttpCheckpointClient(String baseUrl)`; targets `baseUrl + "/" + id`.
  - `class CheckpointClient.CheckpointException extends RuntimeException`
  - `JobContext.JOB_MASTER_URL` = `"JOB_MASTER_URL"` (String constant)

- [ ] **Step 1: Add the `JOB_MASTER_URL` constant to `JobContext`**

In `api/src/main/java/io/fleak/zephflow/api/JobContext.java`, after the existing `DATA_KEY_PREFIX` line (line 35), add:

```java
  public static final String JOB_MASTER_URL = "JOB_MASTER_URL";
```

- [ ] **Step 2: Write the failing test**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/checkpoint/CheckpointClientTest.java`:

```java
package io.fleak.zephflow.lib.commands.fssource.checkpoint;

import static org.junit.jupiter.api.Assertions.*;

import com.sun.net.httpserver.HttpServer;
import io.fleak.zephflow.lib.commands.fssource.checkpoint.CheckpointClient.CheckpointData;
import io.fleak.zephflow.lib.commands.fssource.checkpoint.CheckpointClient.HttpCheckpointClient;
import io.fleak.zephflow.lib.commands.fssource.checkpoint.CheckpointClient.InMemCheckpointClient;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;

class CheckpointClientTest {

  @Test
  void inMem_savesAndLoadsLatest() {
    InMemCheckpointClient client = new InMemCheckpointClient();
    assertTrue(client.loadCheckpoint("src-1").isEmpty());

    client.checkpoint("src-1", "{\"a\":1}");
    client.checkpoint("src-1", "{\"a\":2}");

    Optional<CheckpointData> loaded = client.loadCheckpoint("src-1");
    assertTrue(loaded.isPresent());
    assertEquals("{\"a\":2}", loaded.get().data());
  }

  @Test
  void http_postsToBaseUrlSlashIdAndGetsItBack() throws Exception {
    Map<String, String> store = new ConcurrentHashMap<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/state",
        ex -> {
          String id = ex.getRequestURI().getPath().substring("/state/".length());
          if ("POST".equals(ex.getRequestMethod())) {
            try (InputStream in = ex.getRequestBody()) {
              store.put(id, new String(in.readAllBytes(), StandardCharsets.UTF_8));
            }
            ex.sendResponseHeaders(200, -1);
          } else {
            byte[] body = store.getOrDefault(id, "").getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, body.length);
            ex.getResponseBody().write(body);
          }
          ex.close();
        });
    server.start();
    try {
      String base = "http://127.0.0.1:" + server.getAddress().getPort() + "/state";
      HttpCheckpointClient client = new HttpCheckpointClient(base);

      assertTrue(client.loadCheckpoint("src-9").isEmpty());

      client.checkpoint("src-9", "{\"watermark\":\"x\"}");
      Optional<CheckpointData> loaded = client.loadCheckpoint("src-9");
      assertTrue(loaded.isPresent());
      assertEquals("{\"watermark\":\"x\"}", loaded.get().data());
    } finally {
      server.stop(0);
    }
  }
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `./gradlew :lib:compileTestJava`
Expected: FAIL — `CheckpointClient` does not exist.

- [ ] **Step 4: Create `CheckpointClient`**

Create `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/checkpoint/CheckpointClient.java` (prefix with the standard Apache license header copied from `FsCheckpoint.java`):

```java
package io.fleak.zephflow.lib.commands.fssource.checkpoint;

import io.fleak.zephflow.lib.utils.JsonUtils;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/** HTTP-backed (or in-memory) checkpoint state store. Ported from zephflow-plus. */
public interface CheckpointClient {
  void checkpoint(String id, String data);

  Optional<CheckpointData> loadCheckpoint(String id);

  record CheckpointData(long ts, String data) {}

  class InMemCheckpointClient implements CheckpointClient {
    private final Map<String, LinkedList<CheckpointData>> store = new HashMap<>();

    @Override
    public synchronized void checkpoint(String id, String data) {
      store.computeIfAbsent(id, k -> new LinkedList<>())
          .add(new CheckpointData(System.currentTimeMillis(), data));
    }

    @Override
    public synchronized Optional<CheckpointData> loadCheckpoint(String id) {
      var checkpoints = store.get(id);
      if (CollectionUtils.isEmpty(checkpoints)) {
        return Optional.empty();
      }
      return Optional.of(checkpoints.getLast());
    }
  }

  class HttpCheckpointClient implements CheckpointClient {
    private final HttpClient client = HttpClient.newHttpClient();
    private final String baseUrl;

    public HttpCheckpointClient(String baseUrl) {
      baseUrl = Objects.requireNonNull(baseUrl).trim();
      if (baseUrl.endsWith("/")) {
        baseUrl = baseUrl.substring(0, baseUrl.length() - 1);
      }
      this.baseUrl = baseUrl;
    }

    @Override
    public synchronized void checkpoint(String id, String data) {
      try {
        String json = JsonUtils.toJsonString(new CheckpointData(System.currentTimeMillis(), data));
        HttpRequest request =
            HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/" + id))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() > 299) {
          throw new CheckpointException("Failed to checkpoint state for id=" + id);
        }
      } catch (RuntimeException rte) {
        throw rte;
      } catch (Exception e) {
        throw new CheckpointException("Failed to checkpoint state for id=" + id, e);
      }
    }

    @Override
    public synchronized Optional<CheckpointData> loadCheckpoint(String id) {
      try {
        HttpRequest request =
            HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/" + id))
                .header("Accept", "application/json")
                .GET()
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() < 200 || response.statusCode() > 299) {
          throw new CheckpointException(
              "Failed to load checkpoint for id=" + id + "; response=" + response.body());
        }
        String body = response.body();
        if (StringUtils.isBlank(body)) {
          return Optional.empty();
        }
        CheckpointData checkpoint = JsonUtils.fromJsonString(body, CheckpointData.class);
        if (checkpoint == null || checkpoint.data() == null) {
          return Optional.empty();
        }
        return Optional.of(checkpoint);
      } catch (RuntimeException rte) {
        throw rte;
      } catch (Exception e) {
        throw new CheckpointException("Failed to load checkpoint for id=" + id, e);
      }
    }
  }

  class CheckpointException extends RuntimeException {
    public CheckpointException(String message) {
      super(message);
    }

    public CheckpointException(String message, Throwable cause) {
      super(message, cause);
    }
  }
}
```

> Note: `JsonUtils` has `FAIL_ON_TRAILING_TOKENS` enabled. The test stub returns the exact stored body (a single JSON object), so parsing is clean. If a real server pads the body, that is its contract to keep clean.

- [ ] **Step 5: Run the tests and make sure they pass**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.checkpoint.CheckpointClientTest"`
Expected: PASS (both tests).

- [ ] **Step 6: Commit**

```bash
./gradlew :api:spotlessApply :lib:spotlessApply
git add api/src/main/java/io/fleak/zephflow/api/JobContext.java \
        lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/checkpoint/CheckpointClient.java \
        lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/checkpoint/CheckpointClientTest.java
git commit -m "feat(fssource): add CheckpointClient (in-mem + http) and JOB_MASTER_URL"
```

---

### Task 2: Rewrite the command to a single bounded encoding-driven scan; delete obsolete code

This is one atomic task — the DTO, validator, execution context, and command are coupled by compilation, and removing them breaks the old tests, so all of it lands together and ends with a green build.

**Files:**
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceDto.java`
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceConfigValidator.java`
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceExecutionContext.java`
- Modify: `lib/src/main/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommand.java`
- Modify: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommandBoundedTest.java`
- Delete (prod): `emission/LineEmissionStrategy.java`, `emission/WholeFileEmissionStrategy.java`, `emission/FileReferenceEmissionStrategy.java`, `api/EmissionStrategy.java`, `api/StabilityProbe.java`, `api/SizeStableProbe.java`, `api/PostAction.java`, `api/PostActions.java`, `util/Partitioner.java`, `checkpoint/CheckpointStore.java`, `checkpoint/ObjectStoreCheckpointStore.java`, `checkpoint/InMemoryCheckpointStore.java`, `checkpoint/GenerationMigrator.java` (all under `lib/.../commands/fssource/`)
- Delete (test): `FsSourceCommandUnboundedTest.java`, `BoundedMemoryStreamingTest.java`, `CrossJobDeterminismTest.java`, `FsSourceCommandMigrationTest.java` (all under `lib/.../commands/fssource/`)

**Interfaces:**
- Consumes: `CheckpointClient`, `JobContext.JOB_MASTER_URL` (Task 1); `SourceIdHasher.compute(String,String,String)`; `FsCheckpoint.empty()/.withEmitted(String,Instant)/.isCompleted(String)/.watermark()`; `DeserializerFactory.createDeserializerFactory(EncodingType)`; `FleakDeserializer.deserialize(SerializedEvent)`; `CompressionUtils.gunzip(byte[])`; `FileLister.list(ListRequest)`; `FileReader.open(FileKey,long)`.
- Produces:
  - `FsSourceDto.Config` with getters `getBackend()/getRoot()/getFileNameRegex()/getEncodingType()/getBackendConfig()`.
  - `FsSourceCommand.sourceType()` → `SourceType.BATCH`.
  - `static byte[] FsSourceCommand.maybeGunzip(byte[])` (package-visible for direct unit assertions if desired).
  - `FsSourceExecutionContext.checkpointClient` field.

- [ ] **Step 1: Confirm no external references to the classes being deleted**

Run:
```bash
grep -rln "Partitioner\|EmissionStrategy\|StabilityProbe\|SizeStableProbe\|PostActions\|ObjectStoreCheckpointStore\|GenerationMigrator\|InMemoryCheckpointStore\|FsSourceDto.Mode\|EmissionType\|PartitionConfig" \
  /Users/dan/fleak/zephflow-core /Users/dan/fleak/zephflow-plus \
  | grep -v "/commands/fssource/"
```
Expected: no output. If anything outside `fssource` references these, STOP and surface it before deleting.

- [ ] **Step 2: Delete the obsolete test files**

```bash
cd /Users/dan/fleak/zephflow-core/lib/src/test/java/io/fleak/zephflow/lib/commands/fssource
git rm FsSourceCommandUnboundedTest.java BoundedMemoryStreamingTest.java \
       CrossJobDeterminismTest.java FsSourceCommandMigrationTest.java
```

- [ ] **Step 3: Rewrite `FsSourceDto`**

Replace the body of `FsSourceDto.java` (keep the license header + package) with:

```java
package io.fleak.zephflow.lib.commands.fssource;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.fleak.zephflow.api.CommandConfig;
import io.fleak.zephflow.lib.serdes.EncodingType;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

public interface FsSourceDto {

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  class Config implements CommandConfig {
    private String backend;
    private String root;
    private String fileNameRegex;
    private EncodingType encodingType;
    private Map<String, Object> backendConfig;
  }
}
```

- [ ] **Step 4: Rewrite `FsSourceConfigValidator`**

Replace the `validateConfig` body (keep header/package/imports, add `import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;`):

```java
  @Override
  public void validateConfig(CommandConfig config, String nodeId, JobContext ctx) {
    if (!(config instanceof FsSourceDto.Config c)) {
      throw new IllegalArgumentException("expected FsSourceDto.Config, got " + config.getClass());
    }
    if (c.getBackend() == null || c.getBackend().isBlank()) {
      throw new IllegalArgumentException("backend is required");
    }
    if (c.getRoot() == null || c.getRoot().isBlank()) {
      throw new IllegalArgumentException("root is required");
    }
    if (c.getFileNameRegex() != null && !c.getFileNameRegex().isBlank()) {
      try {
        Pattern.compile(c.getFileNameRegex());
      } catch (PatternSyntaxException e) {
        throw new IllegalArgumentException("invalid fileNameRegex: " + e.getMessage(), e);
      }
    }
    if (c.getEncodingType() == null) {
      throw new IllegalArgumentException("encodingType is required");
    }
    DeserializerFactory.validateEncodingType(c.getEncodingType());
  }
```

- [ ] **Step 5: Rewrite `FsSourceExecutionContext`**

Replace the `checkpointStore` field and its import with the checkpoint client. Full file body (after header/package):

```java
package io.fleak.zephflow.lib.commands.fssource;

import io.fleak.zephflow.api.ExecutionContext;
import io.fleak.zephflow.lib.commands.fssource.api.FileLister;
import io.fleak.zephflow.lib.commands.fssource.api.FileReader;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackend;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendConfig;
import io.fleak.zephflow.lib.commands.fssource.checkpoint.CheckpointClient;
import java.io.IOException;

public final class FsSourceExecutionContext implements ExecutionContext {

  FsBackend backend;
  FsBackendConfig backendConfig;
  FileLister lister;
  FileReader reader;
  CheckpointClient checkpointClient;

  @Override
  public void close() throws IOException {
    if (lister != null)
      try {
        lister.close();
      } catch (Exception ignored) {
      }
    if (reader != null)
      try {
        reader.close();
      } catch (Exception ignored) {
      }
  }
}
```

- [ ] **Step 6: Rewrite `FsSourceCommand`**

Replace `sourceType()`, `createExecutionContext`, `execute`, and the private helpers. Keep `commandName()`, `buildBackendConfig` + the `s3BackendConfig`/`gcsBackendConfig`/`azureBackendConfig` helpers, `tsFromName`, `terminate()`, and the `Pending` record. Remove `buildCheckpointStore`, `buildEmission`, `buildPostAction`, `resolveParallelism`, `resolveJobIndex`, and the `checkpoint`/`checkpointKey` fields.

Replace the import block + relevant methods so the class reads:

```java
package io.fleak.zephflow.lib.commands.fssource;

import static io.fleak.zephflow.lib.utils.MiscUtils.lookupUsernamePasswordCredential;
import static io.fleak.zephflow.lib.utils.MiscUtils.lookupUsernamePasswordCredentialOpt;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.*;
import io.fleak.zephflow.lib.commands.fssource.backend.azblob.AzureBackendConfig;
import io.fleak.zephflow.lib.commands.fssource.backend.gcs.GcsBackendConfig;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackendConfig;
import io.fleak.zephflow.lib.commands.fssource.backend.s3.S3BackendConfig;
import io.fleak.zephflow.lib.commands.fssource.checkpoint.CheckpointClient;
import io.fleak.zephflow.lib.commands.fssource.checkpoint.FsCheckpoint;
import io.fleak.zephflow.lib.commands.fssource.util.SourceIdHasher;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import io.fleak.zephflow.lib.utils.CompressionUtils;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class FsSourceCommand extends SourceCommand {

  private volatile boolean terminated = false;

  public FsSourceCommand(String nodeId, JobContext jobContext) {
    super(nodeId, jobContext, new FsSourceConfigParser(), new FsSourceConfigValidator());
  }

  @Override
  public String commandName() {
    return "fssource";
  }

  @Override
  public SourceType sourceType() {
    return SourceType.BATCH;
  }

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
    ec.checkpointClient = buildCheckpointClient(jc);
    return ec;
  }

  private static CheckpointClient buildCheckpointClient(JobContext jc) {
    Object url = jc.getOtherProperties().get(JobContext.JOB_MASTER_URL);
    String s = url == null ? null : url.toString().trim();
    if (s == null || s.isEmpty()) {
      return new CheckpointClient.InMemCheckpointClient();
    }
    return new CheckpointClient.HttpCheckpointClient(s);
  }

  // buildBackendConfig + s3BackendConfig + gcsBackendConfig + azureBackendConfig: UNCHANGED

  @Override
  public void execute(String user, SourceEventAcceptor out) throws Exception {
    FsSourceExecutionContext ec = (FsSourceExecutionContext) getExecutionContext();
    FsSourceDto.Config c = (FsSourceDto.Config) commandConfig;

    String sourceId =
        SourceIdHasher.compute(c.getBackend(), c.getRoot(), c.getFileNameRegex());
    FsCheckpoint checkpoint = loadCheckpoint(ec.checkpointClient, sourceId);
    log.info("fs_source open: sourceId={} watermark={}", sourceId, checkpoint.watermark());

    Pattern regex = c.getFileNameRegex() == null ? null : Pattern.compile(c.getFileNameRegex());
    FleakDeserializer<?> deserializer =
        DeserializerFactory.createDeserializerFactory(c.getEncodingType()).createDeserializer();

    ListRequest req = new ListRequest(c.getRoot(), regex);
    List<Pending> todo = new ArrayList<>();
    try (var stream = ec.lister.list(req)) {
      stream
          .map(f -> new Pending(f, tsFromName(f, regex)))
          .filter(p -> p.ts().compareTo(checkpoint.watermark()) >= 0)
          .filter(p -> !checkpoint.isCompleted(p.entry().key().urn()))
          .sorted(Comparator.comparing(Pending::ts).thenComparing(p -> p.entry().key().urn()))
          .forEach(todo::add);
    }

    for (Pending p : todo) {
      if (terminated) break;
      FileEntry f = p.entry();
      byte[] bytes;
      try (InputStream in = ec.reader.open(f.key(), 0)) {
        bytes = maybeGunzip(in.readAllBytes());
      }
      List<RecordFleakData> records =
          deserializer.deserialize(new SerializedEvent(null, bytes, null));
      out.accept(records);
      checkpoint = checkpoint.withEmitted(f.key().urn(), p.ts());
      saveCheckpoint(ec.checkpointClient, sourceId, checkpoint);
    }
    out.terminate();
  }

  private static FsCheckpoint loadCheckpoint(CheckpointClient client, String sourceId) {
    return client
        .loadCheckpoint(sourceId)
        .map(d -> JsonUtils.fromJsonString(d.data(), FsCheckpoint.class))
        .orElse(FsCheckpoint.empty());
  }

  private static void saveCheckpoint(CheckpointClient client, String sourceId, FsCheckpoint cp) {
    client.checkpoint(sourceId, JsonUtils.toJsonString(cp));
  }

  /** Auto-detect gzip by magic bytes (0x1f 0x8b) and decompress; otherwise pass through. */
  static byte[] maybeGunzip(byte[] data) {
    if (data.length >= 2 && (data[0] & 0xff) == 0x1f && (data[1] & 0xff) == 0x8b) {
      return CompressionUtils.gunzip(data);
    }
    return data;
  }

  private record Pending(FileEntry entry, Instant ts) {}

  @Override
  public void terminate() throws java.io.IOException {
    terminated = true;
    super.terminate();
  }

  // tsFromName(FileEntry, Pattern): UNCHANGED
}
```

> Keep the existing `buildBackendConfig`, `s3BackendConfig`, `gcsBackendConfig`, `azureBackendConfig`, and `tsFromName` method bodies verbatim from the current file — they are unaffected. The `Map` import is retained because those helpers use `java.util.Map`.

- [ ] **Step 7: Delete the obsolete production classes**

```bash
cd /Users/dan/fleak/zephflow-core/lib/src/main/java/io/fleak/zephflow/lib/commands/fssource
git rm emission/LineEmissionStrategy.java emission/WholeFileEmissionStrategy.java \
       emission/FileReferenceEmissionStrategy.java api/EmissionStrategy.java \
       api/StabilityProbe.java api/SizeStableProbe.java api/PostAction.java api/PostActions.java \
       util/Partitioner.java checkpoint/CheckpointStore.java \
       checkpoint/ObjectStoreCheckpointStore.java checkpoint/InMemoryCheckpointStore.java \
       checkpoint/GenerationMigrator.java
```

- [ ] **Step 8: Rewrite the primary bounded test (JSON_OBJECT_LINE)**

Replace `FsSourceCommandBoundedTest.java` body (keep header/package) with:

```java
package io.fleak.zephflow.lib.commands.fssource;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceCommand.SourceType;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendRegistry;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackend;
import io.fleak.zephflow.lib.commands.fssource.backend.s3.S3Backend;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FsSourceCommandBoundedTest {

  @BeforeEach
  void registerBackend() {
    FsBackendRegistry.unregister("file");
    FsBackendRegistry.register(new LocalFsBackend());
    FsBackendRegistry.unregister("s3");
    FsBackendRegistry.register(new S3Backend());
  }

  @AfterEach
  void cleanup() {
    FsBackendRegistry.unregister("file");
    FsBackendRegistry.unregister("s3");
  }

  static List<RecordFleakData> run(Map<String, Object> rawCfg, JobContext jc) throws Exception {
    List<RecordFleakData> emitted = new ArrayList<>();
    boolean[] terminated = {false};
    SourceEventAcceptor out =
        new SourceEventAcceptor() {
          @Override
          public void accept(List<RecordFleakData> r) {
            emitted.addAll(r);
          }

          @Override
          public void terminate() {
            terminated[0] = true;
          }
        };
    FsSourceCommand cmd = new FsSourceCommand("node-1", jc);
    cmd.parseAndValidateArg(rawCfg);
    cmd.initialize(new MetricClientProvider.NoopMetricClientProvider());
    cmd.execute("test-user", out);
    assertTrue(terminated[0], "execute must call out.terminate()");
    return emitted;
  }

  static Map<String, Object> cfg(Path tmp, String encodingType) {
    return Map.of(
        "backend", "file",
        "root", tmp.toUri().toString(),
        "fileNameRegex", "evt_(?<ts>\\d+)\\.log",
        "encodingType", encodingType);
  }

  @Test
  void sourceTypeIsBatch() {
    FsSourceCommand cmd = new FsSourceCommand("n", JobContext.builder().build());
    assertEquals(SourceType.BATCH, cmd.sourceType());
  }

  @Test
  void jsonObjectLine_emitsBareRecordsInTimestampOrder(@TempDir Path tmp) throws Exception {
    Files.writeString(tmp.resolve("evt_3.log"), "{\"v\":\"c\"}\n{\"v\":\"d\"}");
    Files.writeString(tmp.resolve("evt_1.log"), "{\"v\":\"a\"}");
    Files.writeString(tmp.resolve("evt_2.log"), "{\"v\":\"b\"}");
    Files.writeString(tmp.resolve("ignored.txt"), "{\"v\":\"x\"}");

    List<RecordFleakData> emitted = run(cfg(tmp, "JSON_OBJECT_LINE"), JobContext.builder().build());

    List<Object> vs = emitted.stream().map(r -> r.unwrap().get("v")).toList();
    assertEquals(List.of("a", "b", "c", "d"), vs);
    // bare records: no provenance fields added
    assertFalse(emitted.get(0).unwrap().containsKey("file"));
    assertFalse(emitted.get(0).unwrap().containsKey("line"));
  }
}
```

- [ ] **Step 9: Build and run the FS source + checkpoint tests**

Run: `./gradlew :lib:compileJava :lib:compileTestJava :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.*"`
Expected: PASS. Build compiles with all obsolete classes removed.

- [ ] **Step 10: Commit**

```bash
./gradlew :lib:spotlessApply
git add -A lib/src/main/java/io/fleak/zephflow/lib/commands/fssource \
          lib/src/test/java/io/fleak/zephflow/lib/commands/fssource
git commit -m "refactor(fssource): single bounded encoding-driven scan; drop streaming/parallelism/emission/post-actions"
```

---

### Task 3: Encoding coverage + automatic gzip decompression tests

**Files:**
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommandEncodingTest.java`

**Interfaces:**
- Consumes: `FsSourceCommandBoundedTest.run(...)` pattern (re-implemented locally to keep the test self-contained); `FsSourceCommand.maybeGunzip(byte[])`.

- [ ] **Step 1: Write the failing test**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommandEncodingTest.java`:

```java
package io.fleak.zephflow.lib.commands.fssource;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendRegistry;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackend;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.zip.GZIPOutputStream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FsSourceCommandEncodingTest {

  @BeforeEach
  void reg() {
    FsBackendRegistry.unregister("file");
    FsBackendRegistry.register(new LocalFsBackend());
  }

  @AfterEach
  void cleanup() {
    FsBackendRegistry.unregister("file");
  }

  private List<RecordFleakData> run(Path tmp, String encodingType) throws Exception {
    Map<String, Object> rawCfg =
        Map.of(
            "backend", "file",
            "root", tmp.toUri().toString(),
            "fileNameRegex", "evt_(?<ts>\\d+)\\..*",
            "encodingType", encodingType);
    List<RecordFleakData> emitted = new ArrayList<>();
    SourceEventAcceptor out =
        new SourceEventAcceptor() {
          @Override
          public void accept(List<RecordFleakData> r) {
            emitted.addAll(r);
          }

          @Override
          public void terminate() {}
        };
    FsSourceCommand cmd = new FsSourceCommand("n", JobContext.builder().build());
    cmd.parseAndValidateArg(rawCfg);
    cmd.initialize(new MetricClientProvider.NoopMetricClientProvider());
    cmd.execute("u", out);
    return emitted;
  }

  @Test
  void jsonObject_singleRecord(@TempDir Path tmp) throws Exception {
    Files.writeString(tmp.resolve("evt_1.json"), "{\"k\":\"v\"}");
    List<RecordFleakData> out = run(tmp, "JSON_OBJECT");
    assertEquals(1, out.size());
    assertEquals("v", out.get(0).unwrap().get("k"));
  }

  @Test
  void jsonArray_fansOut(@TempDir Path tmp) throws Exception {
    Files.writeString(tmp.resolve("evt_1.json"), "[{\"k\":1},{\"k\":2}]");
    List<RecordFleakData> out = run(tmp, "JSON_ARRAY");
    assertEquals(2, out.size());
  }

  @Test
  void text_emitsRecordPerFile(@TempDir Path tmp) throws Exception {
    Files.writeString(tmp.resolve("evt_1.txt"), "hello\nworld");
    List<RecordFleakData> out = run(tmp, "STRING_LINE");
    assertFalse(out.isEmpty());
  }

  @Test
  void csv_parses(@TempDir Path tmp) throws Exception {
    Files.writeString(tmp.resolve("evt_1.csv"), "a,b\n1,2\n3,4");
    List<RecordFleakData> out = run(tmp, "CSV");
    assertEquals(2, out.size());
    assertEquals("1", String.valueOf(out.get(0).unwrap().get("a")));
  }

  @Test
  void gzip_isAutoDetectedAndDecompressed(@TempDir Path tmp) throws Exception {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (GZIPOutputStream gz = new GZIPOutputStream(bos)) {
      gz.write("{\"v\":\"a\"}\n{\"v\":\"b\"}".getBytes(StandardCharsets.UTF_8));
    }
    Files.write(tmp.resolve("evt_1.jsonl.gz"), bos.toByteArray());
    List<RecordFleakData> out = run(tmp, "JSON_OBJECT_LINE");
    assertEquals(List.of("a", "b"), out.stream().map(r -> r.unwrap().get("v")).toList());
  }

  @Test
  void nonGzipBytesPassThrough() {
    byte[] plain = "{\"v\":1}".getBytes(StandardCharsets.UTF_8);
    assertArrayEquals(plain, FsSourceCommand.maybeGunzip(plain));
  }
}
```

> If a specific deserializer's record shape differs from the assertion above (e.g. `CSV` header handling or `STRING_LINE` field name), adjust the assertion to match that deserializer's documented output — run the single test, read the actual `unwrap()` map, and assert against it. Do **not** change production code to fit a guessed shape.

- [ ] **Step 2: Run to verify (and calibrate) the tests**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.FsSourceCommandEncodingTest"`
Expected: PASS. If a CSV/STRING_LINE assertion fails on shape, inspect the emitted map and fix the assertion (not the source), then re-run.

- [ ] **Step 3: Commit**

```bash
./gradlew :lib:spotlessApply
git add lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommandEncodingTest.java
git commit -m "test(fssource): per-encoding coverage and gzip auto-detection"
```

---

### Task 4: Resume via HTTP checkpoint (command-level integration)

**Files:**
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommandResumeTest.java`

**Interfaces:**
- Consumes: `JobContext.JOB_MASTER_URL`; a stateful in-process `com.sun.net.httpserver.HttpServer` acting as the checkpoint store across two command runs.

- [ ] **Step 1: Write the failing test**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommandResumeTest.java`:

```java
package io.fleak.zephflow.lib.commands.fssource;

import static org.junit.jupiter.api.Assertions.*;

import com.sun.net.httpserver.HttpServer;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendRegistry;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackend;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

class FsSourceCommandResumeTest {

  private HttpServer server;
  private final Map<String, String> store = new ConcurrentHashMap<>();
  private String baseUrl;

  @BeforeEach
  void setUp() throws Exception {
    FsBackendRegistry.unregister("file");
    FsBackendRegistry.register(new LocalFsBackend());
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/state",
        ex -> {
          String id = ex.getRequestURI().getPath().substring("/state/".length());
          if ("POST".equals(ex.getRequestMethod())) {
            try (InputStream in = ex.getRequestBody()) {
              store.put(id, new String(in.readAllBytes(), StandardCharsets.UTF_8));
            }
            ex.sendResponseHeaders(200, -1);
          } else {
            byte[] body = store.getOrDefault(id, "").getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, body.length);
            ex.getResponseBody().write(body);
          }
          ex.close();
        });
    server.start();
    baseUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/state";
  }

  @AfterEach
  void tearDown() {
    FsBackendRegistry.unregister("file");
    server.stop(0);
  }

  private List<RecordFleakData> runOnce(Path tmp) throws Exception {
    Map<String, Object> rawCfg =
        Map.of(
            "backend", "file",
            "root", tmp.toUri().toString(),
            "fileNameRegex", "evt_(?<ts>\\d+)\\.log",
            "encodingType", "JSON_OBJECT_LINE");
    JobContext jc =
        JobContext.builder()
            .otherProperties(new HashMap<>(Map.of(JobContext.JOB_MASTER_URL, baseUrl)))
            .build();
    List<RecordFleakData> emitted = new ArrayList<>();
    SourceEventAcceptor out =
        new SourceEventAcceptor() {
          @Override
          public void accept(List<RecordFleakData> r) {
            emitted.addAll(r);
          }

          @Override
          public void terminate() {}
        };
    FsSourceCommand cmd = new FsSourceCommand("n", jc);
    cmd.parseAndValidateArg(rawCfg);
    cmd.initialize(new MetricClientProvider.NoopMetricClientProvider());
    cmd.execute("u", out);
    return emitted;
  }

  @Test
  void secondRunSkipsAlreadyCheckpointedFiles(@TempDir Path tmp) throws Exception {
    Files.writeString(tmp.resolve("evt_1.log"), "{\"v\":\"a\"}");
    Files.writeString(tmp.resolve("evt_2.log"), "{\"v\":\"b\"}");

    List<RecordFleakData> first = runOnce(tmp);
    assertEquals(List.of("a", "b"), first.stream().map(r -> r.unwrap().get("v")).toList());
    assertFalse(store.isEmpty(), "checkpoint should have been POSTed");

    // No new files; resume from the same HTTP-backed checkpoint store.
    List<RecordFleakData> second = runOnce(tmp);
    assertTrue(second.isEmpty(), "all files already checkpointed -> nothing re-emitted");
  }
}
```

- [ ] **Step 2: Run to verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.FsSourceCommandResumeTest"`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
./gradlew :lib:spotlessApply
git add lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/FsSourceCommandResumeTest.java
git commit -m "test(fssource): resume skips checkpointed files via http checkpoint client"
```

---

### Task 5: Validator tests

**Files:**
- Test: `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/FsSourceConfigValidatorTest.java` (create if absent; otherwise extend)

**Interfaces:**
- Consumes: `FsSourceConfigValidator.validateConfig(CommandConfig, String, JobContext)`; `FsSourceDto.Config` builder.

- [ ] **Step 1: Write the failing test**

Create `lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/FsSourceConfigValidatorTest.java`:

```java
package io.fleak.zephflow.lib.commands.fssource;

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.lib.serdes.EncodingType;
import org.junit.jupiter.api.Test;

class FsSourceConfigValidatorTest {

  private final FsSourceConfigValidator validator = new FsSourceConfigValidator();
  private final JobContext jc = JobContext.builder().build();

  private FsSourceDto.Config.ConfigBuilder valid() {
    return FsSourceDto.Config.builder()
        .backend("file")
        .root("file:///data/")
        .encodingType(EncodingType.JSON_OBJECT_LINE);
  }

  @Test
  void acceptsValidConfig() {
    assertDoesNotThrow(() -> validator.validateConfig(valid().build(), "n", jc));
  }

  @Test
  void rejectsBlankBackend() {
    var c = valid().backend("").build();
    var ex =
        assertThrows(IllegalArgumentException.class, () -> validator.validateConfig(c, "n", jc));
    assertTrue(ex.getMessage().contains("backend"));
  }

  @Test
  void rejectsBlankRoot() {
    var c = valid().root("").build();
    var ex =
        assertThrows(IllegalArgumentException.class, () -> validator.validateConfig(c, "n", jc));
    assertTrue(ex.getMessage().contains("root"));
  }

  @Test
  void rejectsNullEncodingType() {
    var c = valid().encodingType(null).build();
    var ex =
        assertThrows(IllegalArgumentException.class, () -> validator.validateConfig(c, "n", jc));
    assertTrue(ex.getMessage().contains("encodingType"));
  }

  @Test
  void rejectsParquetEncodingType() {
    var c = valid().encodingType(EncodingType.PARQUET).build();
    assertThrows(IllegalArgumentException.class, () -> validator.validateConfig(c, "n", jc));
  }

  @Test
  void rejectsInvalidRegex() {
    var c = valid().fileNameRegex("evt_(?<ts>\\d+").build();
    var ex =
        assertThrows(IllegalArgumentException.class, () -> validator.validateConfig(c, "n", jc));
    assertTrue(ex.getMessage().contains("fileNameRegex"));
  }
}
```

- [ ] **Step 2: Run to verify it passes**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.FsSourceConfigValidatorTest"`
Expected: PASS.

- [ ] **Step 3: Full module test + commit**

Run: `./gradlew :lib:test --tests "io.fleak.zephflow.lib.commands.fssource.*"`
Expected: PASS (all fssource tests).

```bash
./gradlew :lib:spotlessApply
git add lib/src/test/java/io/fleak/zephflow/lib/commands/fssource/FsSourceConfigValidatorTest.java
git commit -m "test(fssource): validator rejects bad backend/root/encodingType/regex"
```

---

## Self-Review

**Spec coverage:**
- Single bounded mode / `SourceType.BATCH` → Task 2 (`sourceType()`, single-pass `execute`) + test in Task 2 Step 8.
- Encoding via `EncodingType`/bare records → Task 2 (`execute`) + Task 3 coverage.
- Auto gzip detection → Task 2 (`maybeGunzip`) + Task 3 gzip tests.
- Parallelism/stability/post-action removal → Task 2 Step 7 deletions + Step 1 reference check.
- Resume-capable HTTP checkpoint, `{url}/{id}`, `JOB_MASTER_URL`, in-mem fallback → Task 1 (client) + Task 2 (`buildCheckpointClient`, load/save) + Task 4 (resume integration).
- Validator (required `encodingType`, reject `PARQUET`) → Task 5.
- Removed tests → Task 2 Step 2.

**Placeholder scan:** No TBD/TODO. The only conditional guidance is Task 3 Step 1's note to calibrate CSV/STRING_LINE assertions against the real deserializer output — this is explicit (inspect emitted map, adjust assertion, never change source) and the calibration happens at Step 2.

**Type consistency:** `CheckpointClient`/`InMemCheckpointClient`/`HttpCheckpointClient`/`CheckpointData` names match across Tasks 1, 2, 4. `FsSourceExecutionContext.checkpointClient` defined in Task 2 Step 5, consumed in Task 2 Step 6. `maybeGunzip` defined in Task 2 Step 6, asserted in Task 3. `JobContext.JOB_MASTER_URL` defined in Task 1 Step 1, consumed in Tasks 2/4. Config getters (`getEncodingType`, etc.) match the DTO in Task 2 Step 3.
