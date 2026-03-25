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

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;
import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.api.SourceCommand;
import io.fleak.zephflow.api.SourceEventAcceptor;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.TestUtils;
import io.fleak.zephflow.lib.commands.OperatorCommandRegistry;
import io.fleak.zephflow.lib.utils.MiscUtils;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class SyslogUdpCommandTest {

  @Test
  void endToEndReceiveAndConvert() throws Exception {
    int port = findFreePort();

    var factory = OperatorCommandRegistry.OPERATOR_COMMANDS.get(MiscUtils.COMMAND_NAME_SYSLOG_UDP);
    SourceCommand command =
        (SourceCommand) factory.createCommand("test_node", TestUtils.JOB_CONTEXT);

    Map<String, Object> configMap =
        OBJECT_MAPPER.convertValue(
            SyslogUdpDto.Config.builder().host("127.0.0.1").port(port).queueCapacity(100).build(),
            new TypeReference<>() {});

    command.parseAndValidateArg(configMap);
    command.initialize(new MetricClientProvider.NoopMetricClientProvider());

    TestSourceEventAcceptor eventAcceptor = new TestSourceEventAcceptor();

    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(
        () -> {
          try {
            command.execute("test_user", eventAcceptor);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

    Thread.sleep(500);

    String[] messages = {
      "<14>Mar 25 12:00:00 firewall1 test message 1",
      "<14>Mar 25 12:00:01 firewall1 test message 2",
      "<14>Mar 25 12:00:02 firewall1 test message 3"
    };

    for (String msg : messages) {
      sendUdp(msg, port);
    }

    Thread.sleep(1000);

    command.terminate();
    executor.shutdown();
    assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

    List<RecordFleakData> received = eventAcceptor.getReceivedEvents();
    assertEquals(3, received.size());

    for (int i = 0; i < 3; i++) {
      RecordFleakData record = received.get(i);
      Map<String, Object> unwrapped = record.unwrap();
      assertEquals(messages[i], unwrapped.get("message"));
      assertEquals("127.0.0.1", unwrapped.get("source_address"));
      assertNotNull(unwrapped.get("source_port"));
      assertNotNull(unwrapped.get("received_at"));
    }
  }

  private static int findFreePort() throws Exception {
    try (DatagramSocket s = new DatagramSocket(0)) {
      return s.getLocalPort();
    }
  }

  private static void sendUdp(String message, int port) throws Exception {
    try (DatagramSocket sender = new DatagramSocket()) {
      byte[] data = message.getBytes(StandardCharsets.UTF_8);
      DatagramPacket packet =
          new DatagramPacket(data, data.length, InetAddress.getByName("127.0.0.1"), port);
      sender.send(packet);
    }
  }

  public static class TestSourceEventAcceptor implements SourceEventAcceptor {
    private final List<RecordFleakData> receivedEvents =
        Collections.synchronizedList(new ArrayList<>());

    @Override
    public void terminate() {}

    @Override
    public void accept(List<RecordFleakData> recordFleakData) {
      receivedEvents.addAll(recordFleakData);
    }

    public List<RecordFleakData> getReceivedEvents() {
      return receivedEvents;
    }
  }
}
