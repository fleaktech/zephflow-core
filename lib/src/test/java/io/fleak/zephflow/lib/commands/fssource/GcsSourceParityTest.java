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
package io.fleak.zephflow.lib.commands.fssource;

import static org.junit.jupiter.api.Assertions.*;

import com.google.cloud.NoCredentials;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.JsonConfigParser;
import io.fleak.zephflow.lib.commands.fssource.api.*;
import io.fleak.zephflow.lib.commands.fssource.backend.gcs.GcsLister;
import io.fleak.zephflow.lib.commands.fssource.backend.gcs.GcsReader;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackend;
import io.fleak.zephflow.lib.commands.gcssource.GcsSourceCommand;
import io.fleak.zephflow.lib.commands.gcssource.GcsSourceConfigValidator;
import io.fleak.zephflow.lib.commands.gcssource.GcsSourceDto;
import io.fleak.zephflow.lib.gcp.GcsClientFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Parity test: the legacy {@code gcs_source} command and the new {@code fs_source} (backend=gs,
 * emission=WHOLE_FILE) must both process the same files from the same fake-gcs-server fixture and
 * produce the same number of events with the same per-file content.
 *
 * <p><strong>Output shape differences (by design):</strong>
 *
 * <ul>
 *   <li>Legacy {@code gcs_source} with {@code encodingType=TEXT}: emits one record per file with
 *       key {@code "__raw__"} containing the raw file bytes decoded as a string.
 *   <li>New {@code fs_source} with {@code emission=WHOLE_FILE}: emits one record per file with keys
 *       {@code "file"} (the GCS URN) and {@code "content"} (the decoded string).
 * </ul>
 *
 * <p>Full key-name parity is not achievable between the two commands (legacy uses {@code
 * "__raw__"}, new uses {@code "content"}), but <em>content parity</em> is: both must decode the
 * same bytes from the same objects. This test asserts:
 *
 * <ol>
 *   <li>Both commands emit exactly one event per file (3 files → 3 events each).
 *   <li>The set of decoded content strings is identical across both commands.
 * </ol>
 */
@Tag("integration")
@Testcontainers
class GcsSourceParityTest {

  private static final String BUCKET = "parity-bkt";

  @Container
  static GenericContainer<?> FAKE_GCS =
      new GenericContainer<>(DockerImageName.parse("fsouza/fake-gcs-server:1.49"))
          .withExposedPorts(4443)
          .withCommand("-scheme", "http");

  @TempDir Path checkpointDir;

  // ──────────────────────────────────────────────────────────────────────────────
  // Helper: build a Storage client pointing at the fake-gcs-server
  // ──────────────────────────────────────────────────────────────────────────────

  private static Storage fakeStorage() {
    return StorageOptions.newBuilder()
        .setHost("http://" + FAKE_GCS.getHost() + ":" + FAKE_GCS.getMappedPort(4443))
        .setProjectId("test-proj")
        .setCredentials(NoCredentials.getInstance())
        .build()
        .getService();
  }

  // ──────────────────────────────────────────────────────────────────────────────
  // Fixture
  // ──────────────────────────────────────────────────────────────────────────────

  /** Content strings for the three fixture files (single-line, no newlines). */
  static final List<String> FILE_CONTENTS =
      List.of("alpha content here", "beta content here", "gamma content here");

  @BeforeEach
  void populateBucket() {
    Storage s = fakeStorage();
    try {
      s.create(BucketInfo.of(BUCKET));
    } catch (Exception e) {
      // Bucket may already exist from a previous test method; that's fine.
    }
    s.create(
        BlobInfo.newBuilder(BUCKET, "data/file_1.txt").build(),
        FILE_CONTENTS.get(0).getBytes(StandardCharsets.UTF_8));
    s.create(
        BlobInfo.newBuilder(BUCKET, "data/file_2.txt").build(),
        FILE_CONTENTS.get(1).getBytes(StandardCharsets.UTF_8));
    s.create(
        BlobInfo.newBuilder(BUCKET, "data/file_3.txt").build(),
        FILE_CONTENTS.get(2).getBytes(StandardCharsets.UTF_8));
  }

  @BeforeEach
  void registerFileBackend() {
    // LocalFsBackend is needed for the checkpoint override in runFsSource().
    FsBackendRegistry.unregister("file");
    FsBackendRegistry.register(new LocalFsBackend());
  }

  @AfterEach
  void deregisterFileBackend() {
    FsBackendRegistry.unregister("file");
    FsBackendRegistry.unregister("gs");
  }

  // ──────────────────────────────────────────────────────────────────────────────
  // Test
  // ──────────────────────────────────────────────────────────────────────────────

  @Test
  void wholeFileEmissionMatchesGcsSourceOutput() throws Exception {

    // ── 1. Run legacy gcs_source ──────────────────────────────────────────────
    List<RecordFleakData> legacyEvents = runLegacyGcsSource();

    // ── 2. Run new fs_source ──────────────────────────────────────────────────
    List<RecordFleakData> newEvents = runFsSource();

    // ── 3. Assertions ─────────────────────────────────────────────────────────

    // Both commands must see all three files (one event per file).
    assertEquals(3, legacyEvents.size(), "legacy gcs_source must emit 3 events (one per file)");
    assertEquals(3, newEvents.size(), "fs_source must emit 3 events (one per file)");

    // Extract decoded content strings.
    // Legacy uses key "__raw__"; fs_source uses key "content".
    // Key names differ by design — we assert content value parity, not structural parity.
    Set<String> legacyContents = new HashSet<>();
    for (RecordFleakData r : legacyEvents) {
      Object raw = r.unwrap().get("__raw__");
      assertNotNull(
          raw, "legacy event must have '__raw__' key; actual keys: " + r.unwrap().keySet());
      legacyContents.add(raw.toString());
    }

    Set<String> newContents = new HashSet<>();
    for (RecordFleakData r : newEvents) {
      Object content = r.unwrap().get("content");
      assertNotNull(
          content, "fs_source event must have 'content' key; actual keys: " + r.unwrap().keySet());
      newContents.add(content.toString());
    }

    // Both sides must decode exactly the same content strings.
    assertEquals(
        legacyContents,
        newContents,
        "legacy gcs_source and fs_source must produce the same file contents");

    // Sanity: each fixture content string must be present in both.
    Set<String> expected = new HashSet<>(FILE_CONTENTS);
    assertEquals(expected, legacyContents, "legacy contents must match fixture");
    assertEquals(expected, newContents, "fs_source contents must match fixture");
  }

  // ──────────────────────────────────────────────────────────────────────────────
  // Helpers
  // ──────────────────────────────────────────────────────────────────────────────

  /**
   * Runs the legacy {@code GcsSourceCommand} with TEXT encoding against the fake-gcs-server bucket.
   * TEXT encoding → one event per file, with key {@code "__raw__"} holding the decoded bytes.
   *
   * <p>A custom {@link GcsClientFactory} subclass is used to inject the fake {@link Storage} client
   * so ADC is never consulted.
   */
  private List<RecordFleakData> runLegacyGcsSource() throws Exception {
    Storage fakeStore = fakeStorage();

    GcsClientFactory fakeFactory =
        new GcsClientFactory() {
          @Override
          public Storage createStorageClient() {
            return fakeStore;
          }
        };

    // GcsSourceCommand.createExecutionContext() calls basicCommandMetricTags which requires
    // "service" and "env" tags to be present in the JobContext.
    JobContext legacyCtx =
        JobContext.builder().metricTags(Map.of("service", "parity-test", "env", "test")).build();

    GcsSourceCommand cmd =
        new GcsSourceCommand(
            "legacy-node",
            legacyCtx,
            new JsonConfigParser<>(GcsSourceDto.Config.class),
            new GcsSourceConfigValidator(),
            fakeFactory);

    Map<String, Object> cfg = new LinkedHashMap<>();
    cfg.put("bucketName", BUCKET);
    cfg.put("objectPrefix", "data/");
    cfg.put("encodingType", "TEXT");

    cmd.parseAndValidateArg(cfg);
    cmd.initialize(new MetricClientProvider.NoopMetricClientProvider());

    List<RecordFleakData> events = new ArrayList<>();
    cmd.execute("test-user", collector(events));
    return events;
  }

  /**
   * Runs the new {@code FsSourceCommand} with backend=gs and emission=WHOLE_FILE against the same
   * fake-gcs-server bucket.
   *
   * <p>A test-only {@link FsBackend} is registered under scheme "gs" that injects the fake {@link
   * Storage} directly, bypassing ADC. Checkpoints are redirected to a local temp dir so the
   * checkpoint write path does not attempt real GCS calls (the {@link
   * io.fleak.zephflow.lib.commands.fssource.checkpoint.ObjectStoreCheckpointStore} bypasses the
   * registered backend for GCS writes by calling {@link
   * io.fleak.zephflow.lib.commands.fssource.backend.gcs.GcsBackend#client} directly).
   */
  private List<RecordFleakData> runFsSource() throws Exception {
    Storage fakeStore = fakeStorage();

    FsBackend fakeGcsBackend =
        new FsBackend() {
          @Override
          public String scheme() {
            return "gs";
          }

          @Override
          public FileLister createLister(FsBackendConfig cfg) {
            return new GcsLister(fakeStore);
          }

          @Override
          public FileReader createReader(FsBackendConfig cfg) {
            return new GcsReader(fakeStore);
          }

          @Override
          public Set<FsBackend.Capability> capabilities() {
            return Set.of();
          }
        };

    FsBackendRegistry.unregister("gs");
    FsBackendRegistry.register(fakeGcsBackend);

    // Redirect checkpoints to local FS to avoid real GCS calls during checkpoint writes.
    Path cpDir = Files.createTempDirectory(checkpointDir, "fs-cp-");

    Map<String, Object> cfg = new LinkedHashMap<>();
    cfg.put("backend", "gs");
    cfg.put("root", "gs://" + BUCKET + "/data/");
    cfg.put("emission", Map.of("type", "WHOLE_FILE", "encoding", "utf-8"));
    cfg.put("mode", "BOUNDED");
    cfg.put("partition", Map.of("index", 0, "parallelism", 1));
    cfg.put("checkpoint", Map.of("backend", "file", "root", cpDir.toUri().toString()));

    FsSourceCommand cmd = new FsSourceCommand("fs-node", JobContext.builder().build());
    cmd.parseAndValidateArg(cfg);
    cmd.initialize(new MetricClientProvider.NoopMetricClientProvider());

    List<RecordFleakData> events = new ArrayList<>();
    cmd.execute("test-user", collector(events));
    return events;
  }

  private static SourceEventAcceptor collector(List<RecordFleakData> sink) {
    return new SourceEventAcceptor() {
      @Override
      public void accept(List<RecordFleakData> r) {
        sink.addAll(r);
      }

      @Override
      public void terminate() {}
    };
  }
}
