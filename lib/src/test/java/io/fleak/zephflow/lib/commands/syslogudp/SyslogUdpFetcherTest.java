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

import static org.junit.jupiter.api.Assertions.*;

import io.fleak.zephflow.lib.commands.source.NoCommitStrategy;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SyslogUdpFetcherTest {

  private SyslogUdpFetcher fetcher;
  private int testPort;

  @BeforeEach
  void setUp() throws Exception {
    SyslogUdpDto.Config config =
        SyslogUdpDto.Config.builder()
            .host("127.0.0.1")
            .port(0)
            .bufferSize(65535)
            .queueCapacity(100)
            .encoding("UTF-8")
            .build();
    fetcher = new SyslogUdpFetcher(config);
    fetcher.start();
    testPort = fetcher.getLocalPort();
  }

  @AfterEach
  void tearDown() throws Exception {
    if (fetcher != null) {
      fetcher.close();
    }
  }

  @Test
  void fetchReturnsSentDatagram() throws Exception {
    String message = "<14>Mar 25 12:00:00 firewall1 message body";
    sendUdp(message);

    Thread.sleep(200);

    List<SerializedEvent> events = fetcher.fetch();
    assertFalse(events.isEmpty());
    SerializedEvent event = events.getFirst();
    assertEquals(message, new String(event.value(), StandardCharsets.UTF_8));
    assertNotNull(event.metadata());
    assertEquals("127.0.0.1", event.metadata().get("source_address"));
    assertNotNull(event.metadata().get("source_port"));
    assertNotNull(event.metadata().get("received_at"));
  }

  @Test
  void fetchReturnsMultipleDatagrams() throws Exception {
    for (int i = 0; i < 5; i++) {
      sendUdp("message-" + i);
    }
    Thread.sleep(300);

    List<SerializedEvent> allEvents = new ArrayList<>();
    for (int attempt = 0; attempt < 10 && allEvents.size() < 5; attempt++) {
      allEvents.addAll(fetcher.fetch());
      if (allEvents.size() < 5) Thread.sleep(50);
    }
    assertEquals(5, allEvents.size());
  }

  @Test
  void fetchReturnsEmptyWhenNoData() {
    List<SerializedEvent> events = fetcher.fetch();
    assertTrue(events.isEmpty());
  }

  @Test
  void isExhaustedAlwaysFalse() {
    assertFalse(fetcher.isExhausted());
  }

  @Test
  void commitStrategyIsNone() {
    assertEquals(NoCommitStrategy.INSTANCE, fetcher.commitStrategy());
  }

  @Test
  void dropsMessagesWhenQueueFull() throws Exception {
    for (int i = 0; i < 120; i++) {
      sendUdp("msg-" + i);
    }
    Thread.sleep(500);

    List<SerializedEvent> allEvents = new ArrayList<>();
    for (int attempt = 0; attempt < 20; attempt++) {
      allEvents.addAll(fetcher.fetch());
      if (allEvents.size() >= 100) break;
      Thread.sleep(50);
    }
    assertTrue(allEvents.size() <= 100);
    assertTrue(allEvents.size() >= 90);
  }

  private void sendUdp(String message) throws Exception {
    try (DatagramSocket sender = new DatagramSocket()) {
      byte[] data = message.getBytes(StandardCharsets.UTF_8);
      DatagramPacket packet =
          new DatagramPacket(data, data.length, InetAddress.getByName("127.0.0.1"), testPort);
      sender.send(packet);
    }
  }
}
