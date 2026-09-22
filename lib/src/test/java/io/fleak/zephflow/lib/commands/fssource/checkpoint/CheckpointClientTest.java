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
import java.util.List;
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

  @Test
  void http_sendsTheVersionItReadAndAcceptsTheBump() throws Exception {
    VersionedStubServer server = new VersionedStubServer();
    try {
      HttpCheckpointClient client = new HttpCheckpointClient(server.baseUrl());

      assertTrue(client.loadCheckpoint("src-cas").isEmpty());
      client.checkpoint("src-cas", "{\"n\":1}");
      client.checkpoint("src-cas", "{\"n\":2}");

      assertEquals(2, server.versionOf("src-cas"), "each write bumps the version exactly once");
      assertEquals("{\"n\":2}", client.loadCheckpoint("src-cas").orElseThrow().data());
    } finally {
      server.stop();
    }
  }

  @Test
  void http_throwsWhenAnotherWriterAdvancedTheState() throws Exception {
    VersionedStubServer server = new VersionedStubServer();
    try {
      HttpCheckpointClient mine = new HttpCheckpointClient(server.baseUrl());
      HttpCheckpointClient theirs = new HttpCheckpointClient(server.baseUrl());

      mine.loadCheckpoint("src-race");
      theirs.loadCheckpoint("src-race");

      mine.checkpoint("src-race", "{\"n\":1}");

      assertThrows(
          CheckpointClient.CheckpointConflictException.class,
          () -> theirs.checkpoint("src-race", "{\"n\":99}"),
          "a writer holding a stale version must not be able to clobber");
      assertEquals("{\"n\":1}", mine.loadCheckpoint("src-race").orElseThrow().data());
    } finally {
      server.stop();
    }
  }

  @Test
  void http_fallsBackToUnconditionalWritesAgainstAJobmasterWithoutVersions() throws Exception {
    // No X-State-Version header anywhere: an older jobmaster.
    VersionedStubServer server = new VersionedStubServer(false);
    try {
      HttpCheckpointClient client = new HttpCheckpointClient(server.baseUrl());

      client.loadCheckpoint("src-old");
      client.checkpoint("src-old", "{\"n\":1}");

      assertEquals("{\"n\":1}", client.loadCheckpoint("src-old").orElseThrow().data());
      assertTrue(
          server.expectedVersionHeadersSeen().isEmpty(),
          "legacy mode must not send a version the server cannot honour");
    } finally {
      server.stop();
    }
  }

  @Test
  void http_throwsWhenVersionHeaderIsUnparseable() throws Exception {
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext(
        "/state",
        exchange -> {
          exchange.getResponseHeaders().add("X-State-Version", "not-a-number");
          exchange.sendResponseHeaders(200, 0);
          exchange.close();
        });
    server.start();
    try {
      String base = "http://127.0.0.1:" + server.getAddress().getPort() + "/state";
      HttpCheckpointClient client = new HttpCheckpointClient(base);

      CheckpointClient.CheckpointException thrown =
          assertThrows(
              CheckpointClient.CheckpointException.class,
              () -> client.loadCheckpoint("src-bad-version"),
              "a malformed version header is a protocol violation, not a legacy server");
      assertFalse(
          thrown instanceof CheckpointClient.CheckpointConflictException,
          "this is not a version conflict");
      assertTrue(
          thrown.getMessage().contains("not-a-number"),
          "the error must name the offending value: " + thrown.getMessage());
    } finally {
      server.stop(0);
    }
  }

  /** Minimal stand-in for the jobmaster state API, with or without version support. */
  private static final class VersionedStubServer {
    private final HttpServer server;
    private final Map<String, String> bodies = new ConcurrentHashMap<>();
    private final Map<String, Long> versions = new ConcurrentHashMap<>();
    private final List<String> expectedVersionHeadersSeen =
        java.util.Collections.synchronizedList(new java.util.ArrayList<>());
    private final boolean versioned;

    VersionedStubServer() throws Exception {
      this(true);
    }

    VersionedStubServer(boolean versioned) throws Exception {
      this.versioned = versioned;
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.createContext("/state", this::handle);
      server.start();
    }

    private void handle(com.sun.net.httpserver.HttpExchange exchange) throws java.io.IOException {
      String id = exchange.getRequestURI().getPath().substring("/state/".length());
      if ("POST".equals(exchange.getRequestMethod())) {
        String expected = exchange.getRequestHeaders().getFirst("X-State-Expected-Version");
        if (expected != null) {
          expectedVersionHeadersSeen.add(expected);
        }
        String body;
        try (InputStream inputStream = exchange.getRequestBody()) {
          body = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }
        long current = versions.getOrDefault(id, 0L);
        if (versioned && expected != null && Long.parseLong(expected) != current) {
          exchange.sendResponseHeaders(409, -1);
          exchange.close();
          return;
        }
        bodies.put(id, body);
        long next = current + 1;
        versions.put(id, next);
        if (versioned) {
          exchange.getResponseHeaders().add("X-State-Version", String.valueOf(next));
        }
        exchange.sendResponseHeaders(200, -1);
        exchange.close();
        return;
      }
      byte[] response = bodies.getOrDefault(id, "").getBytes(StandardCharsets.UTF_8);
      if (versioned) {
        exchange
            .getResponseHeaders()
            .add("X-State-Version", String.valueOf(versions.getOrDefault(id, 0L)));
      }
      exchange.sendResponseHeaders(200, response.length);
      try (java.io.OutputStream outputStream = exchange.getResponseBody()) {
        outputStream.write(response);
      }
      exchange.close();
    }

    String baseUrl() {
      return "http://127.0.0.1:" + server.getAddress().getPort() + "/state";
    }

    long versionOf(String id) {
      return versions.getOrDefault(id, 0L);
    }

    List<String> expectedVersionHeadersSeen() {
      return expectedVersionHeadersSeen;
    }

    void stop() {
      server.stop(0);
    }
  }
}
