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
package io.fleak.zephflow.lib.commands.elasticsearchsource;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

/** Answers each POST with the next queued response and every DELETE with 200; records requests. */
final class ElasticsearchStub implements AutoCloseable {

  record Request(String method, String path, String query, String body, String authorization) {}

  record Response(int status, String body) {}

  private final HttpServer server;
  private final Queue<Response> responses;
  private final List<Request> requests = new CopyOnWriteArrayList<>();

  ElasticsearchStub(Response... responses) throws IOException {
    this.responses = new ConcurrentLinkedQueue<>(List.of(responses));
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", this::handle);
    server.start();
  }

  String url() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  List<Request> requests() {
    return List.copyOf(requests);
  }

  static String json(String singleQuoted) {
    return singleQuoted.replace('\'', '"');
  }

  private void handle(HttpExchange exchange) throws IOException {
    String method = exchange.getRequestMethod();
    requests.add(
        new Request(
            method,
            exchange.getRequestURI().getPath(),
            exchange.getRequestURI().getQuery(),
            new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8),
            exchange.getRequestHeaders().getFirst("Authorization")));
    Response response = method.equals("DELETE") ? new Response(200, "{}") : responses.poll();
    if (response == null) {
      response = new Response(500, "no response queued");
    }
    byte[] bytes = response.body().getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json; charset=UTF-8");
    exchange.sendResponseHeaders(response.status(), bytes.length == 0 ? -1 : bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  @Override
  public void close() {
    server.stop(0);
  }
}
