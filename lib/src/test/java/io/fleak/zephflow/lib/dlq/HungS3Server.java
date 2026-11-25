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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/** A store that accepts connections and reads every request byte, and never answers. */
final class HungS3Server implements AutoCloseable {
  private final ServerSocketChannel server;
  private final List<SocketChannel> connections = new CopyOnWriteArrayList<>();

  HungS3Server() throws IOException {
    server = ServerSocketChannel.open().bind(new InetSocketAddress("127.0.0.1", 0));
    Thread acceptor = new Thread(this::acceptForever, "hung-s3-acceptor");
    acceptor.setDaemon(true);
    acceptor.start();
  }

  String endpoint() throws IOException {
    return "http://127.0.0.1:" + ((InetSocketAddress) server.getLocalAddress()).getPort();
  }

  /** An endpoint where nothing listens, so every connection is refused at once. */
  static String refusingEndpoint() throws IOException {
    try (ServerSocketChannel closed =
        ServerSocketChannel.open().bind(new InetSocketAddress("127.0.0.1", 0))) {
      return "http://127.0.0.1:" + ((InetSocketAddress) closed.getLocalAddress()).getPort();
    }
  }

  private void acceptForever() {
    while (server.isOpen()) {
      try {
        SocketChannel connection = server.accept();
        connections.add(connection);
        Thread reader = new Thread(() -> readForever(connection), "hung-s3-reader");
        reader.setDaemon(true);
        reader.start();
      } catch (IOException closed) {
        return;
      }
    }
  }

  private static void readForever(SocketChannel connection) {
    ByteBuffer buffer = ByteBuffer.allocate(64 * 1024);
    try {
      while (connection.read(buffer) >= 0) {
        buffer.clear();
      }
    } catch (IOException closed) {
      // the client gave up and closed the connection
    }
  }

  @Override
  public void close() throws IOException {
    server.close();
    for (SocketChannel connection : connections) {
      connection.close();
    }
  }
}
