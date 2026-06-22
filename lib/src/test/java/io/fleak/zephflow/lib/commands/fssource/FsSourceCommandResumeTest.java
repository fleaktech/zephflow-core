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
