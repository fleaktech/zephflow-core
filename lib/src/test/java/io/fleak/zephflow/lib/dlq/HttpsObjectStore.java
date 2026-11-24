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
package io.fleak.zephflow.lib.dlq;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.net.ssl.SSLContext;

/**
 * The part of S3 that stores an object, over HTTPS on 127.0.0.1 with the certificate of a test's
 * authority. Objects are kept by path, so the client addresses the bucket in the path.
 */
final class HttpsObjectStore implements AutoCloseable {

  private final HttpsServer server;
  private final Map<String, byte[]> objects = new ConcurrentHashMap<>();

  HttpsObjectStore(SSLContext serverContext) throws IOException {
    server = HttpsServer.create(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), 0);
    server.setHttpsConfigurator(new HttpsConfigurator(serverContext));
    server.createContext("/", this::handle);
    server.start();
  }

  String endpoint() {
    return "https://127.0.0.1:" + server.getAddress().getPort();
  }

  /** The stored objects by path, {@code /<bucket>/<key>}. */
  Map<String, byte[]> objects() {
    return Map.copyOf(objects);
  }

  private void handle(HttpExchange exchange) throws IOException {
    try (exchange) {
      if (!"PUT".equals(exchange.getRequestMethod())) {
        exchange.sendResponseHeaders(405, -1);
        return;
      }
      objects.put(exchange.getRequestURI().getPath(), exchange.getRequestBody().readAllBytes());
      exchange.getResponseHeaders().add("ETag", "\"stored\"");
      exchange.sendResponseHeaders(200, -1);
    }
  }

  @Override
  public void close() {
    server.stop(0);
  }
}
