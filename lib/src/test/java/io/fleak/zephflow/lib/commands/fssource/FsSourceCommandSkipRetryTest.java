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

import com.sun.net.httpserver.HttpServer;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.fssource.api.FsBackendRegistry;
import io.fleak.zephflow.lib.commands.fssource.backend.local.LocalFsBackend;
import io.fleak.zephflow.lib.commands.fssource.checkpoint.CheckpointClient;
import io.fleak.zephflow.lib.commands.fssource.checkpoint.FsCheckpoint;
import io.fleak.zephflow.lib.commands.fssource.util.Partitioner;
import io.fleak.zephflow.lib.utils.JsonUtils;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FsSourceCommandSkipRetryTest {

  private HttpServer server;
  private final Map<String, String> store = new ConcurrentHashMap<>();
  private final Map<String, Long> versions = new ConcurrentHashMap<>();
  private final AtomicInteger postCount = new AtomicInteger();
  private String baseUrl;
  private int conflictOnPostNumber = Integer.MAX_VALUE;
  private volatile String expectedVersionHeaderOnConflictingPost;

  @BeforeEach
  void setUp() throws Exception {
    store.clear();
    versions.clear();
    postCount.set(0);
    conflictOnPostNumber = Integer.MAX_VALUE;
    expectedVersionHeaderOnConflictingPost = null;
    FsBackendRegistry.unregister("file");
    FsBackendRegistry.register(new LocalFsBackend());
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/state",
        exchange -> {
          String id = exchange.getRequestURI().getPath().substring("/state/".length());
          if ("POST".equals(exchange.getRequestMethod())) {
            int postNumber = postCount.incrementAndGet();
            String body;
            try (InputStream inputStream = exchange.getRequestBody()) {
              body = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
            }
            if (postNumber == conflictOnPostNumber) {
              // The real jobmaster's 409 carries no body and no headers at all.
              expectedVersionHeaderOnConflictingPost =
                  exchange.getRequestHeaders().getFirst("X-State-Expected-Version");
              exchange.sendResponseHeaders(409, -1);
              exchange.close();
              return;
            }
            store.put(id, body);
            long next = versions.getOrDefault(id, 0L) + 1;
            versions.put(id, next);
            exchange.getResponseHeaders().add("X-State-Version", String.valueOf(next));
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
          } else {
            byte[] body = store.getOrDefault(id, "").getBytes(StandardCharsets.UTF_8);
            exchange
                .getResponseHeaders()
                .add("X-State-Version", String.valueOf(versions.getOrDefault(id, 0L)));
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream outputStream = exchange.getResponseBody()) {
              outputStream.write(body);
            }
            exchange.close();
          }
        });
    server.start();
    baseUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/state";
  }

  @AfterEach
  void tearDown() {
    FsBackendRegistry.unregister("file");
    server.stop(0);
  }

  private List<Object> run(Path dir) throws Exception {
    return run(dir, "JSON_OBJECT_LINE", Map.of());
  }

  private List<Object> run(Path dir, String encodingType, Map<String, Object> extraConfig)
      throws Exception {
    JobContext jobContext =
        JobContext.builder()
            .otherProperties(new HashMap<>(Map.of(JobContext.CHECKPOINT_URL, baseUrl)))
            .build();
    Map<String, Object> rawConfig = new HashMap<>();
    rawConfig.put("backend", "file");
    rawConfig.put("root", dir.toUri().toString());
    rawConfig.put("fileNameRegex", "evt_(?<ts>\\d+)\\.(log|json)");
    rawConfig.put("encodingType", encodingType);
    rawConfig.putAll(extraConfig);
    List<RecordFleakData> emitted = new ArrayList<>();
    SourceEventAcceptor acceptor =
        new SourceEventAcceptor() {
          @Override
          public void accept(List<RecordFleakData> records) {
            emitted.addAll(records);
          }

          @Override
          public void terminate() {}
        };
    FsSourceCommand command = new FsSourceCommand("n", jobContext);
    command.parseAndValidateArg(rawConfig);
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
    command.execute("u", acceptor);
    return emitted.stream().map(record -> record.unwrap().get("v")).toList();
  }

  /** Bytes that claim to be gzip (1f 8b) but are not, so the read throws. */
  private static final byte[] CORRUPT_GZIP = {0x1f, (byte) 0x8b, 0x08, 0x00, 1, 2, 3, 4};

  @Test
  void aFileSkippedByAReadErrorIsStillRetriedAfterLaterFilesSucceed(@TempDir Path dir)
      throws Exception {
    Files.write(dir.resolve("evt_1.log"), CORRUPT_GZIP);
    Files.writeString(dir.resolve("evt_2.log"), "{\"v\":\"b\"}");

    assertEquals(List.of("b"), run(dir), "run 1: evt_1 is skipped, evt_2 is emitted");

    Files.writeString(dir.resolve("evt_1.log"), "{\"v\":\"a\"}");

    assertEquals(
        List.of("a"), run(dir), "run 2 must retry the repaired file and not re-emit evt_2");
  }

  @Test
  void aStillBrokenFileDoesNotBlockNewerFilesFromBeingEmitted(@TempDir Path dir) throws Exception {
    Files.write(dir.resolve("evt_1.log"), CORRUPT_GZIP);
    Files.writeString(dir.resolve("evt_2.log"), "{\"v\":\"b\"}");

    assertEquals(List.of("b"), run(dir));

    Files.writeString(dir.resolve("evt_3.log"), "{\"v\":\"c\"}");

    assertEquals(
        List.of("c"),
        run(dir),
        "the held watermark re-lists evt_2 but it is remembered as completed");
  }

  @Test
  void aRunInWhichEveryFileIsSkippedFails(@TempDir Path dir) throws Exception {
    Files.write(dir.resolve("evt_1.log"), CORRUPT_GZIP);
    Files.write(dir.resolve("evt_2.log"), CORRUPT_GZIP);

    IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> run(dir));
    assertTrue(
        thrown.getMessage().contains("2"),
        "the failure must say how many files were skipped: " + thrown.getMessage());
  }

  @Test
  void anOversizedFileIsNotRetriedOnLaterRuns(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("evt_1.json"), oversizedJsonObject("a"));
    Files.writeString(dir.resolve("evt_2.json"), "{\"v\":\"b\"}");
    Map<String, Object> smallCap = Map.of("maxFileBytes", 64L);

    assertEquals(List.of("b"), run(dir, "JSON_OBJECT", smallCap), "run 1: evt_1 is too large");

    // Shrinking the file shows whether run 2 looks at it again: a retry would now emit it.
    Files.writeString(dir.resolve("evt_1.json"), "{\"v\":\"a\"}");
    Files.writeString(dir.resolve("evt_3.json"), "{\"v\":\"c\"}");

    assertEquals(
        List.of("c"),
        run(dir, "JSON_OBJECT", smallCap),
        "an oversized file cannot succeed on retry, so it is completed rather than retried");
  }

  @Test
  void anOversizedFileDoesNotHoldTheWatermark(@TempDir Path dir) throws Exception {
    // The watermark is per bucket, so the newer file must share the oversized file's bucket.
    Files.writeString(dir.resolve("evt_1.log"), oversizedJsonObject("a"));
    String newerFileName = sameBucketFileName(dir, "evt_1.log");
    Files.writeString(dir.resolve(newerFileName), "{\"v\":\"b\"}");

    run(dir, "JSON_OBJECT", Map.of("maxFileBytes", 64L));

    String newerUrn = dir.resolve(newerFileName).toUri().toString();
    FsCheckpoint checkpoint =
        store.values().stream()
            .map(body -> JsonUtils.fromJsonString(body, CheckpointClient.CheckpointData.class))
            .map(envelope -> JsonUtils.fromJsonString(envelope.data(), FsCheckpoint.class))
            .filter(candidate -> candidate.isCompleted(newerUrn))
            .findFirst()
            .orElseThrow();
    assertEquals(
        checkpoint.completedSinceWatermark().get(newerUrn),
        checkpoint.watermark(),
        "the watermark must advance past the oversized evt_1 to the newer file");
  }

  @Test
  void aRunWhoseOnlyFilesAreOversizedSucceeds(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("evt_1.json"), oversizedJsonObject("a"));

    assertEquals(
        List.of(),
        run(dir, "JSON_OBJECT", Map.of("maxFileBytes", 64L)),
        "the oversized file is resolved, so there is nothing left for a retry to do");
  }

  /**
   * A JSON object comfortably over a 64-byte cap. JSON_OBJECT is a single document, so unlike the
   * streamed formats it is still read whole and capped.
   */
  private static String oversizedJsonObject(String value) {
    return "{\"v\":\"" + value + "\",\"padding\":\"" + "x".repeat(100) + "\"}";
  }

  @Test
  void aCheckpointConflictAbortsTheRun(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("evt_1.log"), "{\"v\":\"a\"}");
    // Checkpoints are now per virtual bucket, so the second write only lands on the same id (and
    // therefore only conflicts against the version left by the first write) when it hashes into
    // the same bucket. Search for a second filename that does.
    String secondFileName = sameBucketFileName(dir, "evt_1.log");
    Files.writeString(dir.resolve(secondFileName), "{\"v\":\"b\"}");
    conflictOnPostNumber = 2;

    assertThrows(
        CheckpointClient.CheckpointConflictException.class,
        () -> run(dir),
        "losing the checkpoint race means another attempt owns this source; stop, do not continue");
    assertEquals(
        "1",
        expectedVersionHeaderOnConflictingPost,
        "the rejected write must have been a genuine conditional update, not a bare POST");
  }

  /** Finds a second file name whose urn hashes into the same virtual bucket as {@code first}. */
  private static String sameBucketFileName(Path dir, String first) {
    int targetBucket = Partitioner.virtualBucket(dir.resolve(first).toUri().toString());
    for (int candidate = 2; candidate < 10_000; candidate++) {
      String name = "evt_" + candidate + ".log";
      if (Partitioner.virtualBucket(dir.resolve(name).toUri().toString()) == targetBucket) {
        return name;
      }
    }
    throw new IllegalStateException("no same-bucket filename found");
  }
}
