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

class FsSourceCommandRescaleTest {

  private HttpServer server;
  private final Map<String, String> store = new ConcurrentHashMap<>();
  private final Map<String, Long> versions = new ConcurrentHashMap<>();
  private final AtomicInteger postCount = new AtomicInteger();
  private String baseUrl;

  @BeforeEach
  void setUp() throws Exception {
    store.clear();
    versions.clear();
    postCount.set(0);
    FsBackendRegistry.unregister("file");
    FsBackendRegistry.register(new LocalFsBackend());
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/state",
        exchange -> {
          String id = exchange.getRequestURI().getPath().substring("/state/".length());
          if ("POST".equals(exchange.getRequestMethod())) {
            postCount.incrementAndGet();
            String body;
            try (InputStream inputStream = exchange.getRequestBody()) {
              body = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
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

  private List<Object> run(Path dir, int replicaIndex, int replicaCount) throws Exception {
    JobContext jobContext =
        JobContext.builder()
            .otherProperties(
                new HashMap<>(
                    Map.of(
                        JobContext.CHECKPOINT_URL,
                        baseUrl,
                        JobContext.REPLICA_INDEX,
                        String.valueOf(replicaIndex),
                        JobContext.REPLICA_COUNT,
                        String.valueOf(replicaCount))))
            .build();
    Map<String, Object> rawConfig =
        Map.of(
            "backend", "file",
            "root", dir.toUri().toString(),
            "fileNameRegex", "evt_(?<ts>\\d+)\\.log",
            "encodingType", "JSON_OBJECT_LINE");
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

  @Test
  void progressSurvivesAChangeInTheReplicaCount(@TempDir Path dir) throws Exception {
    for (int fileNumber = 1; fileNumber <= 40; fileNumber++) {
      Files.writeString(dir.resolve("evt_" + fileNumber + ".log"), "{\"v\":" + fileNumber + "}");
    }

    List<Object> firstPass = new ArrayList<>();
    for (int replicaIndex = 0; replicaIndex < 3; replicaIndex++) {
      firstPass.addAll(run(dir, replicaIndex, 3));
    }
    assertEquals(40, firstPass.size(), "3 replicas between them read every file exactly once");
    int postCountAfterFirstPass = postCount.get();

    // Rescale 3 -> 4. Buckets move between replicas; no file may be read a second time.
    List<Object> afterRescale = new ArrayList<>();
    for (int replicaIndex = 0; replicaIndex < 4; replicaIndex++) {
      afterRescale.addAll(run(dir, replicaIndex, 4));
    }
    assertEquals(List.of(), afterRescale, "a rescale must not re-read files already checkpointed");
    assertEquals(
        postCountAfterFirstPass,
        postCount.get(),
        "no file was re-read after the rescale, so no new checkpoint POST should have happened"
            + " either");
  }

  @Test
  void progressSurvivesScalingDownToASingleReplica(@TempDir Path dir) throws Exception {
    for (int fileNumber = 1; fileNumber <= 40; fileNumber++) {
      Files.writeString(dir.resolve("evt_" + fileNumber + ".log"), "{\"v\":" + fileNumber + "}");
    }

    for (int replicaIndex = 0; replicaIndex < 2; replicaIndex++) {
      run(dir, replicaIndex, 2);
    }

    // replicaCount <= 1 means grid injects nothing at all, so this is the (0, 1) default.
    assertEquals(List.of(), run(dir, 0, 1), "scaling to one replica must not re-read everything");
  }
}
