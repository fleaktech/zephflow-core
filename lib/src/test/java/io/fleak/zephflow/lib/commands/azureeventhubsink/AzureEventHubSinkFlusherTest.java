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
package io.fleak.zephflow.lib.commands.azureeventhubsink;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.azure.messaging.eventhubs.EventDataBatch;
import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.models.CreateBatchOptions;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.pathselect.PathExpression;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.ser.FleakSerializer;
import io.fleak.zephflow.lib.serdes.ser.SerializerFactory;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AzureEventHubSinkFlusherTest {

  private EventHubProducerClient producerClient;
  private FleakSerializer<?> serializer;

  @BeforeEach
  void setUp() {
    producerClient = mock(EventHubProducerClient.class);
    serializer =
        SerializerFactory.createSerializerFactory(EncodingType.JSON_OBJECT).createSerializer();
  }

  private static SimpleSinkCommand.PreparedInputEvents<RecordFleakData> prepared(
      RecordFleakData... events) {
    SimpleSinkCommand.PreparedInputEvents<RecordFleakData> prepared =
        new SimpleSinkCommand.PreparedInputEvents<>();
    for (RecordFleakData event : events) {
      prepared.add(event, event); // PassThrough preprocessor: prepared == raw
    }
    return prepared;
  }

  private static RecordFleakData record(Map<String, Object> fields) {
    return (RecordFleakData) FleakData.wrap(fields);
  }

  @Test
  void sendsAllEventsInOneBatchWhenTheyFit() throws Exception {
    EventDataBatch batch = mock(EventDataBatch.class);
    when(producerClient.createBatch()).thenReturn(batch);
    when(batch.tryAdd(any())).thenReturn(true);
    when(batch.getCount()).thenReturn(3);

    AzureEventHubSinkFlusher flusher =
        new AzureEventHubSinkFlusher(producerClient, serializer, null);

    SimpleSinkCommand.FlushResult result =
        flusher.flush(
            prepared(record(Map.of("id", 1)), record(Map.of("id", 2)), record(Map.of("id", 3))),
            Map.of());

    assertEquals(3, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    assertTrue(result.flushedDataSize() > 0);
    verify(producerClient, times(1)).send(batch);
  }

  @Test
  void rollsToANewBatchWhenTheCurrentBatchIsFull() throws Exception {
    EventDataBatch batch = mock(EventDataBatch.class);
    when(producerClient.createBatch()).thenReturn(batch);
    // First two events fit, the third does not (batch full), then it fits in the fresh batch.
    when(batch.tryAdd(any())).thenReturn(true, true, false, true);
    when(batch.getCount())
        .thenReturn(2); // non-empty so the full batch is sent, not treated as oversize

    AzureEventHubSinkFlusher flusher =
        new AzureEventHubSinkFlusher(producerClient, serializer, null);

    SimpleSinkCommand.FlushResult result =
        flusher.flush(
            prepared(record(Map.of("id", 1)), record(Map.of("id", 2)), record(Map.of("id", 3))),
            Map.of());

    assertEquals(3, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    verify(producerClient, times(2)).send(batch); // rolled: one full batch + the remainder
  }

  @Test
  void groupsEventsByPartitionKeyIntoSeparateBatches() throws Exception {
    EventDataBatch batch = mock(EventDataBatch.class);
    when(producerClient.createBatch(any(CreateBatchOptions.class))).thenReturn(batch);
    when(batch.tryAdd(any())).thenReturn(true);
    when(batch.getCount()).thenReturn(1);

    AzureEventHubSinkFlusher flusher =
        new AzureEventHubSinkFlusher(producerClient, serializer, PathExpression.fromString("$.k"));

    SimpleSinkCommand.FlushResult result =
        flusher.flush(
            prepared(
                record(Map.of("k", "a", "id", 1)),
                record(Map.of("k", "a", "id", 2)),
                record(Map.of("k", "b", "id", 3))),
            Map.of());

    assertEquals(3, result.successCount());
    // Two distinct partition keys -> two keyed batches -> two sends.
    verify(producerClient, times(2)).createBatch(any(CreateBatchOptions.class));
    verify(producerClient, times(2)).send(batch);
    verify(producerClient, never()).createBatch();
  }

  @Test
  void reportsOversizedEventAsErrorWithoutSending() throws Exception {
    EventDataBatch batch = mock(EventDataBatch.class);
    when(producerClient.createBatch()).thenReturn(batch);
    when(batch.tryAdd(any())).thenReturn(false); // never fits
    when(batch.getCount()).thenReturn(0); // empty -> event itself is too large

    AzureEventHubSinkFlusher flusher =
        new AzureEventHubSinkFlusher(producerClient, serializer, null);

    SimpleSinkCommand.FlushResult result =
        flusher.flush(prepared(record(Map.of("id", 1))), Map.of());

    assertEquals(0, result.successCount());
    assertEquals(1, result.errorOutputList().size());
    verify(producerClient, never()).send(any(EventDataBatch.class));
  }

  @Test
  void returnsEmptyResultForNoEvents() throws Exception {
    AzureEventHubSinkFlusher flusher =
        new AzureEventHubSinkFlusher(producerClient, serializer, null);

    SimpleSinkCommand.FlushResult result =
        flusher.flush(new SimpleSinkCommand.PreparedInputEvents<>(), Map.of());

    assertEquals(0, result.successCount());
    assertTrue(result.errorOutputList().isEmpty());
    verifyNoInteractions(producerClient);
  }

  @Test
  void throwsWhenFlushingAfterClose() {
    AzureEventHubSinkFlusher flusher =
        new AzureEventHubSinkFlusher(producerClient, serializer, null);
    flusher.close();
    assertThrows(
        IllegalStateException.class,
        () -> flusher.flush(prepared(record(Map.of("id", 1))), Map.of()));
    verify(producerClient).close();
  }
}
