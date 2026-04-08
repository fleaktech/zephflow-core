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
package io.fleak.zephflow.lib.commands.activemqsource;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.lib.commands.source.BatchCommitStrategy;
import io.fleak.zephflow.lib.commands.source.BytesRawDataConverter;
import io.fleak.zephflow.lib.commands.source.SourceExecutionContext;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import jakarta.jms.*;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ActiveMqSourceFetcherTest {

  @Test
  void testFetch_withNoMessages() throws Exception {
    MessageConsumer mockConsumer = mock(MessageConsumer.class);
    Session mockSession = mock(Session.class);
    Connection mockConnection = mock(Connection.class);

    when(mockConsumer.receive(1000L)).thenReturn(null);

    ActiveMqSourceFetcher fetcher =
        new ActiveMqSourceFetcher(
            mockConnection, mockSession, mockConsumer, 1000L, 500, BatchCommitStrategy.forKafka());

    var result = fetcher.fetch();

    assertNotNull(result);
    assertTrue(result.isEmpty());
  }

  @Test
  void testFetch_withTextMessage() throws Exception {
    MessageConsumer mockConsumer = mock(MessageConsumer.class);
    Session mockSession = mock(Session.class);
    Connection mockConnection = mock(Connection.class);

    TextMessage mockMessage = mock(TextMessage.class);
    when(mockMessage.getText()).thenReturn("{\"id\":1,\"value\":\"test\"}");

    when(mockConsumer.receive(1000L)).thenReturn(mockMessage);
    when(mockConsumer.receiveNoWait()).thenReturn(null);

    ActiveMqSourceFetcher fetcher =
        new ActiveMqSourceFetcher(
            mockConnection, mockSession, mockConsumer, 1000L, 500, BatchCommitStrategy.forKafka());

    var fetchedData = fetcher.fetch();

    assertEquals(1, fetchedData.size());
    assertNull(fetchedData.getFirst().key());
    assertNotNull(fetchedData.getFirst().value());

    FleakDeserializer<?> deserializer =
        DeserializerFactory.createDeserializerFactory(EncodingType.JSON_OBJECT)
            .createDeserializer();
    BytesRawDataConverter converter = new BytesRawDataConverter(deserializer);

    SourceExecutionContext<SerializedEvent> mockContext = mock();
    when(mockContext.dataSizeCounter()).thenReturn(mock());
    when(mockContext.inputEventCounter()).thenReturn(mock());
    when(mockContext.deserializeFailureCounter()).thenReturn(mock());

    var converted =
        fetchedData.stream()
            .flatMap(x -> converter.convert(x, mockContext).transformedData().stream())
            .toList();

    assertEquals(1, converted.size());
    Map<String, Object> expected = Map.of("id", 1L, "value", "test");
    assertEquals(FleakData.wrap(expected), converted.getFirst());
  }

  @Test
  void testFetch_withBytesMessage() throws Exception {
    MessageConsumer mockConsumer = mock(MessageConsumer.class);
    Session mockSession = mock(Session.class);
    Connection mockConnection = mock(Connection.class);

    byte[] payload = "{\"id\":2}".getBytes();
    BytesMessage mockMessage = mock(BytesMessage.class);
    when(mockMessage.getBodyLength()).thenReturn((long) payload.length);
    doAnswer(
            inv -> {
              byte[] buf = inv.getArgument(0);
              System.arraycopy(payload, 0, buf, 0, payload.length);
              return payload.length;
            })
        .when(mockMessage)
        .readBytes(any(byte[].class));

    when(mockConsumer.receive(1000L)).thenReturn(mockMessage);
    when(mockConsumer.receiveNoWait()).thenReturn(null);

    ActiveMqSourceFetcher fetcher =
        new ActiveMqSourceFetcher(
            mockConnection, mockSession, mockConsumer, 1000L, 500, BatchCommitStrategy.forKafka());

    var fetchedData = fetcher.fetch();

    assertEquals(1, fetchedData.size());
    assertArrayEquals(payload, fetchedData.getFirst().value());
  }

  @Test
  void testFetch_stopsAtMaxBatchSize() throws Exception {
    MessageConsumer mockConsumer = mock(MessageConsumer.class);
    Session mockSession = mock(Session.class);
    Connection mockConnection = mock(Connection.class);

    TextMessage mockMessage = mock(TextMessage.class);
    when(mockMessage.getText()).thenReturn("{\"id\":1}");

    when(mockConsumer.receive(1000L)).thenReturn(mockMessage);
    when(mockConsumer.receiveNoWait()).thenReturn(mockMessage);

    int maxBatch = 3;
    ActiveMqSourceFetcher fetcher =
        new ActiveMqSourceFetcher(
            mockConnection,
            mockSession,
            mockConsumer,
            1000L,
            maxBatch,
            BatchCommitStrategy.forKafka());

    var result = fetcher.fetch();

    assertEquals(maxBatch, result.size());
  }

  @Test
  void testFetch_unsupportedMessageType() throws Exception {
    MessageConsumer mockConsumer = mock(MessageConsumer.class);
    Session mockSession = mock(Session.class);
    Connection mockConnection = mock(Connection.class);

    MapMessage mockMessage = mock(MapMessage.class);
    when(mockConsumer.receive(1000L)).thenReturn(mockMessage);

    ActiveMqSourceFetcher fetcher =
        new ActiveMqSourceFetcher(
            mockConnection, mockSession, mockConsumer, 1000L, 500, BatchCommitStrategy.forKafka());

    var result = fetcher.fetch();

    // Unsupported message type is caught by the JMSException handler, returns empty
    assertTrue(result.isEmpty());
  }

  @Test
  void testFetch_exceptionMidBatch_returnsPartialResults() throws Exception {
    MessageConsumer mockConsumer = mock(MessageConsumer.class);
    Session mockSession = mock(Session.class);
    Connection mockConnection = mock(Connection.class);

    TextMessage goodMessage = mock(TextMessage.class);
    when(goodMessage.getText()).thenReturn("{\"id\":1}");

    when(mockConsumer.receive(1000L)).thenReturn(goodMessage);
    when(mockConsumer.receiveNoWait())
        .thenReturn(goodMessage)
        .thenThrow(new JMSException("connection lost"));

    ActiveMqSourceFetcher fetcher =
        new ActiveMqSourceFetcher(
            mockConnection, mockSession, mockConsumer, 1000L, 500, BatchCommitStrategy.forKafka());

    var result = fetcher.fetch();

    // Should have the 2 messages received before the exception
    assertEquals(2, result.size());
  }

  @Test
  void testClose_continuesClosingWhenOneResourceThrows() throws Exception {
    Session mockSession = mock(Session.class);
    Connection mockConnection = mock(Connection.class);
    MessageConsumer mockConsumer = mock(MessageConsumer.class);

    doThrow(new RuntimeException("consumer close failed")).when(mockConsumer).close();

    ActiveMqSourceFetcher fetcher =
        new ActiveMqSourceFetcher(
            mockConnection, mockSession, mockConsumer, 1000L, 500, BatchCommitStrategy.forKafka());

    fetcher.close();

    // All resources should still have close() called despite consumer throwing
    verify(mockConsumer).close();
    verify(mockSession).close();
    verify(mockConnection).close();
  }

  @Test
  void testCommitter_callsSessionCommit() throws Exception {
    Session mockSession = mock(Session.class);
    Connection mockConnection = mock(Connection.class);
    MessageConsumer mockConsumer = mock(MessageConsumer.class);

    ActiveMqSourceFetcher fetcher =
        new ActiveMqSourceFetcher(
            mockConnection, mockSession, mockConsumer, 1000L, 500, BatchCommitStrategy.forKafka());

    fetcher.committer().commit();

    verify(mockSession).commit();
  }

  @Test
  void testClose_closesAllResources() throws Exception {
    Session mockSession = mock(Session.class);
    Connection mockConnection = mock(Connection.class);
    MessageConsumer mockConsumer = mock(MessageConsumer.class);

    ActiveMqSourceFetcher fetcher =
        new ActiveMqSourceFetcher(
            mockConnection, mockSession, mockConsumer, 1000L, 500, BatchCommitStrategy.forKafka());

    fetcher.close();

    verify(mockConsumer).close();
    verify(mockSession).close();
    verify(mockConnection).close();
  }
}
