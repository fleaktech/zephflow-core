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
    store.clear();
    FsBackendRegistry.unregister("file");
    FsBackendRegistry.register(new LocalFsBackend());
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/state",
        exchange -> {
          String id = exchange.getRequestURI().getPath().substring("/state/".length());
          if ("POST".equals(exchange.getRequestMethod())) {
            try (InputStream inputStream = exchange.getRequestBody()) {
              store.put(id, new String(inputStream.readAllBytes(), StandardCharsets.UTF_8));
            }
            exchange.sendResponseHeaders(200, -1);
          } else {
            byte[] body = store.getOrDefault(id, "").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (java.io.OutputStream outputStream = exchange.getResponseBody()) {
              outputStream.write(body);
            }
          }
          exchange.close();
        });
    server.start();
    baseUrl = "http://127.0.0.1:" + server.getAddress().getPort() + "/state";
  }

  @AfterEach
  void tearDown() {
    FsBackendRegistry.unregister("file");
    server.stop(0);
  }

  private List<RecordFleakData> runOnce(Path tempDir) throws Exception {
    JobContext jobContext =
        JobContext.builder()
            .otherProperties(new HashMap<>(Map.of(JobContext.CHECKPOINT_URL, baseUrl)))
            .build();
    return run(tempDir, jobContext);
  }

  private List<RecordFleakData> run(Path tempDir, JobContext jobContext) throws Exception {
    Map<String, Object> rawConfig =
        Map.of(
            "backend", "file",
            "root", tempDir.toUri().toString(),
            "fileNameRegex", "evt_(?<ts>\\d+)\\.log",
            "encodingType", "JSON_OBJECT_LINE");
    List<RecordFleakData> emitted = new ArrayList<>();
    SourceEventAcceptor out =
        new SourceEventAcceptor() {
          @Override
          public void accept(List<RecordFleakData> record) {
            emitted.addAll(record);
          }

          @Override
          public void terminate() {}
        };
    FsSourceCommand command = new FsSourceCommand("n", jobContext);
    command.parseAndValidateArg(rawConfig);
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());
    command.execute("u", out);
    return emitted;
  }

  @Test
  void secondRunSkipsAlreadyCheckpointedFiles(@TempDir Path tempDir) throws Exception {
    Files.writeString(tempDir.resolve("evt_1.log"), "{\"v\":\"a\"}");
    Files.writeString(tempDir.resolve("evt_2.log"), "{\"v\":\"b\"}");

    List<RecordFleakData> first = runOnce(tempDir);
    assertEquals(
        List.of("a", "b"), first.stream().map(record -> record.unwrap().get("v")).toList());
    assertEquals(1, store.size(), "exactly one checkpoint entry should have been POSTed");

    // No new files; resume from the same HTTP-backed checkpoint store.
    List<RecordFleakData> second = runOnce(tempDir);
    assertTrue(second.isEmpty(), "all files already checkpointed -> nothing re-emitted");
  }

  @Test
  void withoutCheckpointUrl_emitsAllFilesAndDoesNotCheckpoint(@TempDir Path tempDir)
      throws Exception {
    Files.writeString(tempDir.resolve("evt_1.log"), "{\"v\":\"a\"}");
    Files.writeString(tempDir.resolve("evt_2.log"), "{\"v\":\"b\"}");

    // checkpoint_url is not provided: the source must work and emit every file.
    JobContext withoutCheckpointUrl = JobContext.builder().build();
    List<RecordFleakData> first = run(tempDir, withoutCheckpointUrl);
    assertEquals(
        List.of("a", "b"), first.stream().map(record -> record.unwrap().get("v")).toList());

    // No checkpoint state is sent anywhere when checkpoint_url is absent.
    assertTrue(store.isEmpty(), "no checkpoint_url -> nothing POSTed to the checkpoint server");

    // Without durable checkpointing, a second run re-emits the same files.
    List<RecordFleakData> second = run(tempDir, JobContext.builder().build());
    assertEquals(
        List.of("a", "b"), second.stream().map(record -> record.unwrap().get("v")).toList());
  }
}
