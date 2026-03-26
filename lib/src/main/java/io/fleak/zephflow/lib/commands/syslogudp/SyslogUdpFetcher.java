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
package io.fleak.zephflow.lib.commands.syslogudp;

import io.fleak.zephflow.lib.commands.source.CommitStrategy;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.commands.source.NoCommitStrategy;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SyslogUdpFetcher implements Fetcher<SerializedEvent> {

  private static final int POLL_TIMEOUT_MS = 100;
  private static final int MAX_BATCH_SIZE = 500;

  private final SyslogUdpDto.Config config;
  private final LinkedBlockingQueue<SerializedEvent> queue;

  private DatagramSocket socket;
  private Thread receiverThread;
  private volatile boolean running;

  public SyslogUdpFetcher(SyslogUdpDto.Config config) {
    this.config = config;
    this.queue = new LinkedBlockingQueue<>(config.getQueueCapacity());
  }

  public void start() throws Exception {
    socket = new DatagramSocket(config.getPort(), InetAddress.getByName(config.getHost()));
    running = true;
    receiverThread = new Thread(this::receiveLoop, "syslog-udp-receiver");
    receiverThread.setDaemon(true);
    receiverThread.start();
    log.info("SyslogUDP receiver started on {}:{}", config.getHost(), getLocalPort());
  }

  public int getLocalPort() {
    return socket != null ? socket.getLocalPort() : -1;
  }

  @Override
  public List<SerializedEvent> fetch() {
    List<SerializedEvent> batch = new ArrayList<>();
    try {
      SerializedEvent first = queue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      if (first != null) {
        batch.add(first);
        queue.drainTo(batch, MAX_BATCH_SIZE - 1);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    return batch;
  }

  @Override
  public boolean isExhausted() {
    return false;
  }

  @Override
  public CommitStrategy commitStrategy() {
    return NoCommitStrategy.INSTANCE;
  }

  @Override
  public void close() {
    running = false;
    if (socket != null && !socket.isClosed()) {
      socket.close();
    }
    if (receiverThread != null) {
      receiverThread.interrupt();
      try {
        receiverThread.join(5000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
    log.info("SyslogUDP receiver stopped");
  }

  private void receiveLoop() {
    byte[] buffer = new byte[config.getBufferSize()];
    while (running) {
      try {
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
        socket.receive(packet);
        long receivedAt = System.currentTimeMillis();

        byte[] data = new byte[packet.getLength()];
        System.arraycopy(packet.getData(), packet.getOffset(), data, 0, packet.getLength());

        String sourceAddress = packet.getAddress().getHostAddress();
        String sourcePort = String.valueOf(packet.getPort());

        Map<String, String> metadata =
            Map.of(
                SyslogUdpDto.METADATA_SOURCE_ADDRESS, sourceAddress,
                SyslogUdpDto.METADATA_SOURCE_PORT, sourcePort,
                SyslogUdpDto.METADATA_RECEIVED_AT, String.valueOf(receivedAt));

        SerializedEvent event = new SerializedEvent(null, data, metadata);

        if (!queue.offer(event)) {
          log.warn("SyslogUDP queue full, dropping datagram from {}:{}", sourceAddress, sourcePort);
        }
      } catch (SocketException e) {
        if (running) {
          log.error("Socket error in SyslogUDP receiver", e);
          sleepQuietly(100);
        }
      } catch (Exception e) {
        if (running) {
          log.error("Unexpected error in SyslogUDP receiver", e);
          sleepQuietly(100);
        }
      }
    }
  }

  private static void sleepQuietly(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }
}
