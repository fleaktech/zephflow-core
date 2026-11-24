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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.*;

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.models.EventContext;
import com.azure.messaging.eventhubs.models.LastEnqueuedEventProperties;
import com.azure.messaging.eventhubs.models.PartitionContext;
import io.fleak.zephflow.lib.commands.source.PerRecordCommitStrategy;
import io.fleak.zephflow.lib.commands.source.SourceBacklog;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.jupiter.api.Test;

class AzureEventHubSourceFetcherBacklogTest {

  @Test
  void sumsLastEnqueuedMinusCheckpointOverOwnedPartitions() throws Exception {
    AzureEventHubSourceFetcher fetcher =
        fetcher(event("0", 10, 110L), event("1", 20, 70L), event("0", 11, 111L));

    fetcher.fetch();
    fetcher.committer().commit();

    assertEquals(SourceBacklog.records(150L, null), fetcher.backlog());
  }

  @Test
  void aPartitionWithoutACheckpointYetMakesTheBacklogUnknown() throws Exception {
    AzureEventHubSourceFetcher fetcher = fetcher(event("0", 10, 110L), event("1", 20, 70L));

    fetcher.fetch();
    fetcher.committer().commitThrough(1);

    assertEquals(SourceBacklog.UNKNOWN, fetcher.backlog());
  }

  @Test
  void eventsWithoutLastEnqueuedPropertiesLeaveTheBacklogUnknown() throws Exception {
    AzureEventHubSourceFetcher fetcher = fetcher(event("0", 10, null));

    fetcher.fetch();
    fetcher.committer().commit();

    assertEquals(SourceBacklog.UNKNOWN, fetcher.backlog());
  }

  @Test
  void pauseStopsTheProcessorAndForgetsWhatItNoLongerOwns() throws Exception {
    EventProcessorClient processor = mock();
    EventContext first = event("0", 10, 110L);
    EventContext second = event("0", 11, 111L);
    LinkedBlockingQueue<EventContext> queue = new LinkedBlockingQueue<>(List.of(first, second));
    AzureEventHubSourceFetcher fetcher = fetcher(processor, queue);
    fetcher.fetch();
    fetcher.committer().commitThrough(1);
    queue.add(event("0", 12, 112L));

    fetcher.pause();

    verify(processor).stop(any(Duration.class));
    assertEquals(0, queue.size());
    assertEquals(SourceBacklog.UNKNOWN, fetcher.backlog());

    fetcher.resume();
    verify(processor).start();
    EventContext afterResume = event("1", 5, 5L);
    queue.add(afterResume);
    fetcher.fetch();
    fetcher.committer().commit();
    verify(first).updateCheckpoint();
    verify(second, never()).updateCheckpoint();
    verify(afterResume).updateCheckpoint();
  }

  @Test
  void pauseLetsACallbackBlockedOnAFullQueueFinish() throws Exception {
    EventProcessorClient processor = mock();
    LinkedBlockingQueue<EventContext> queue = new LinkedBlockingQueue<>(1);
    queue.add(event("0", 1, 1L));
    Thread callback =
        new Thread(
            () -> {
              try {
                queue.put(event("0", 2, 2L));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
              }
            });
    callback.start();
    doAnswer(
            call -> {
              callback.join();
              return null;
            })
        .when(processor)
        .stop(any(Duration.class));

    fetcher(processor, queue).pause();

    assertFalse(callback.isAlive());
    assertEquals(0, queue.size());
  }

  private static AzureEventHubSourceFetcher fetcher(
      EventProcessorClient processor, LinkedBlockingQueue<EventContext> queue) {
    return new AzureEventHubSourceFetcher(
        processor, queue, 500, Duration.ofMillis(50), PerRecordCommitStrategy.INSTANCE);
  }

  private static AzureEventHubSourceFetcher fetcher(EventContext... events) {
    return new AzureEventHubSourceFetcher(
        mock(EventProcessorClient.class),
        new LinkedBlockingQueue<>(List.of(events)),
        500,
        Duration.ofMillis(50),
        PerRecordCommitStrategy.INSTANCE);
  }

  private static EventContext event(String partitionId, long sequenceNumber, Long lastEnqueued) {
    EventContext ctx = mock(EventContext.class);
    when(ctx.getPartitionContext())
        .thenReturn(new PartitionContext("ns.servicebus.windows.net", "eh", "cg", partitionId));
    EventData data = mock(EventData.class);
    when(data.getSequenceNumber()).thenReturn(sequenceNumber);
    when(data.getBody()).thenReturn(new byte[0]);
    when(ctx.getEventData()).thenReturn(data);
    when(ctx.getLastEnqueuedEventProperties())
        .thenReturn(
            lastEnqueued == null
                ? null
                : new LastEnqueuedEventProperties(lastEnqueued, 0L, null, null));
    return ctx;
  }
}
