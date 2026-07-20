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
package io.fleak.zephflow.lib.commands.mqttsource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.lib.commands.source.CommitStrategy;
import io.fleak.zephflow.lib.commands.source.NoCommitStrategy;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.junit.jupiter.api.Test;

class MqttSourceFetcherTest {

  @Test
  void testFetch_withNoMessages() {
    MqttClient mockClient = mock(MqttClient.class);
    LinkedBlockingQueue<byte[]> queue = new LinkedBlockingQueue<>();

    MqttSourceFetcher fetcher = new MqttSourceFetcher(mockClient, queue, 100L, 500);

    List<SerializedEvent> result = fetcher.fetch();

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  void testFetch_withMessages() {
    MqttClient mockClient = mock(MqttClient.class);
    LinkedBlockingQueue<byte[]> queue = new LinkedBlockingQueue<>();
    byte[] payload = "{\"id\":1}".getBytes();
    queue.offer(payload);

    MqttSourceFetcher fetcher = new MqttSourceFetcher(mockClient, queue, 1000L, 500);

    List<SerializedEvent> result = fetcher.fetch();

    assertEquals(1, result.size());
    assertNull(result.getFirst().key());
    assertArrayEquals(payload, result.getFirst().value());
  }

  @Test
  void testFetch_stopsAtMaxBatchSize() {
    MqttClient mockClient = mock(MqttClient.class);
    LinkedBlockingQueue<byte[]> queue = new LinkedBlockingQueue<>();
    for (int i = 0; i < 10; i++) {
      queue.offer(("{\"id\":" + i + "}").getBytes());
    }

    int maxBatchSize = 3;
    MqttSourceFetcher fetcher = new MqttSourceFetcher(mockClient, queue, 1000L, maxBatchSize);

    List<SerializedEvent> result = fetcher.fetch();

    assertEquals(maxBatchSize, result.size());
  }

  @Test
  void testCommitStrategy_isNone() {
    MqttClient mockClient = mock(MqttClient.class);
    MqttSourceFetcher fetcher =
        new MqttSourceFetcher(mockClient, new LinkedBlockingQueue<>(), 1000L, 500);

    assertSame(NoCommitStrategy.INSTANCE, fetcher.commitStrategy());
    assertEquals(CommitStrategy.CommitMode.NONE, fetcher.commitStrategy().getCommitMode());
  }

  @Test
  void testClose_disconnectsAndClosesClient() throws Exception {
    MqttClient mockClient = mock(MqttClient.class);
    when(mockClient.isConnected()).thenReturn(true);

    MqttSourceFetcher fetcher =
        new MqttSourceFetcher(mockClient, new LinkedBlockingQueue<>(), 1000L, 500);

    fetcher.close();

    verify(mockClient).disconnect();
    verify(mockClient).close();
  }

  @Test
  void testClose_isQuietWhenClientThrows() throws Exception {
    MqttClient mockClient = mock(MqttClient.class);
    when(mockClient.isConnected()).thenReturn(true);
    doThrow(new RuntimeException("disconnect failed")).when(mockClient).disconnect();

    MqttSourceFetcher fetcher =
        new MqttSourceFetcher(mockClient, new LinkedBlockingQueue<>(), 1000L, 500);

    assertDoesNotThrow(fetcher::close);
    verify(mockClient).close();
  }
}
