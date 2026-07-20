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
            exchange.getResponseBody().write(body);
          }
          exchange.close();
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
