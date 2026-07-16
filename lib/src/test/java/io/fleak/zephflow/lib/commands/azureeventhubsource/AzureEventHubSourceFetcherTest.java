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
package io.fleak.zephflow.lib.commands.azureeventhubsource;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.models.EventContext;
import com.azure.messaging.eventhubs.models.PartitionContext;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.commands.source.PerRecordCommitStrategy;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.jupiter.api.Test;

class AzureEventHubSourceFetcherTest {

  private static final Duration SHORT_POLL = Duration.ofMillis(50);

  private static EventContext eventContext(String partitionId, String body) {
    EventContext ctx = mock(EventContext.class);
    when(ctx.getPartitionContext())
        .thenReturn(new PartitionContext("ns.servicebus.windows.net", "eh", "cg", partitionId));
    when(ctx.getEventData()).thenReturn(new EventData(body.getBytes(UTF_8)));
    return ctx;
  }

  @Test
  void fetchDrainsQueueAndMapsBodyToValue() {
    BlockingQueue<EventContext> queue = new LinkedBlockingQueue<>();
    queue.add(eventContext("0", "hello"));
    queue.add(eventContext("0", "world"));

    AzureEventHubSourceFetcher fetcher =
        new AzureEventHubSourceFetcher(
            mock(EventProcessorClient.class),
            queue,
            500,
            SHORT_POLL,
            PerRecordCommitStrategy.INSTANCE);

    List<SerializedEvent> events = fetcher.fetch();

    assertEquals(2, events.size());
    assertEquals("hello", new String(events.get(0).value(), UTF_8));
    assertEquals("world", new String(events.get(1).value(), UTF_8));
    assertNull(events.get(0).key()); // no partition key set on the source events
  }

  @Test
  void fetchAttachesMetadataFromExtractor() {
    BlockingQueue<EventContext> queue = new LinkedBlockingQueue<>();
    queue.add(eventContext("0", "hello"));

    AzureEventHubSourceFetcher fetcher =
        new AzureEventHubSourceFetcher(
            mock(EventProcessorClient.class),
            queue,
            500,
            SHORT_POLL,
            PerRecordCommitStrategy.INSTANCE,
            ctx -> java.util.Map.of("deviceId", "sensor-01"));

    List<SerializedEvent> events = fetcher.fetch();

    assertEquals(1, events.size());
    assertEquals("sensor-01", events.get(0).metadata().get("deviceId"));
  }

  @Test
  void fiveArgConstructorLeavesMetadataNull() {
    BlockingQueue<EventContext> queue = new LinkedBlockingQueue<>();
    queue.add(eventContext("0", "hello"));

    AzureEventHubSourceFetcher fetcher =
        new AzureEventHubSourceFetcher(
            mock(EventProcessorClient.class),
            queue,
            500,
            SHORT_POLL,
            PerRecordCommitStrategy.INSTANCE);

    List<SerializedEvent> events = fetcher.fetch();

    assertEquals(1, events.size());
    assertNull(events.get(0).metadata());
  }

  @Test
  void fetchReturnsEmptyWhenQueueIsEmpty() {
    AzureEventHubSourceFetcher fetcher =
        new AzureEventHubSourceFetcher(
            mock(EventProcessorClient.class),
            new LinkedBlockingQueue<>(),
            500,
            SHORT_POLL,
            PerRecordCommitStrategy.INSTANCE);

    assertTrue(fetcher.fetch().isEmpty());
  }

  @Test
  void fetchIsCappedByMaxEventsPerFetch() {
    BlockingQueue<EventContext> queue = new LinkedBlockingQueue<>();
    for (int i = 0; i < 5; i++) {
      queue.add(eventContext("0", "e" + i));
    }

    AzureEventHubSourceFetcher fetcher =
        new AzureEventHubSourceFetcher(
            mock(EventProcessorClient.class),
            queue,
            2,
            SHORT_POLL,
            PerRecordCommitStrategy.INSTANCE);

    assertEquals(2, fetcher.fetch().size());
    assertEquals(3, queue.size()); // remainder stays buffered
  }

  @Test
  void commitCheckpointsOnlyTheLatestEventPerPartition() throws Exception {
    EventContext p0First = eventContext("0", "a");
    EventContext p0Last = eventContext("0", "b");
    EventContext p1Only = eventContext("1", "c");

    BlockingQueue<EventContext> queue = new LinkedBlockingQueue<>();
    queue.add(p0First);
    queue.add(p1Only);
    queue.add(p0Last);

    AzureEventHubSourceFetcher fetcher =
        new AzureEventHubSourceFetcher(
            mock(EventProcessorClient.class),
            queue,
            500,
            SHORT_POLL,
            PerRecordCommitStrategy.INSTANCE);

    fetcher.fetch();
    Fetcher.Committer committer = fetcher.committer();
    committer.commit();

    verify(p0Last).updateCheckpoint();
    verify(p1Only).updateCheckpoint();
    verify(p0First, never()).updateCheckpoint(); // superseded within the same partition

    // Second commit with no new fetch must not re-checkpoint (map cleared after commit).
    committer.commit();
    verify(p0Last, times(1)).updateCheckpoint();
    verify(p1Only, times(1)).updateCheckpoint();
  }

  @Test
  void closeStopsTheProcessorClient() {
    EventProcessorClient processorClient = mock(EventProcessorClient.class);
    AzureEventHubSourceFetcher fetcher =
        new AzureEventHubSourceFetcher(
            processorClient,
            new LinkedBlockingQueue<>(),
            500,
            SHORT_POLL,
            PerRecordCommitStrategy.INSTANCE);

    fetcher.close();

    verify(processorClient).stop(any(Duration.class));
  }
}
