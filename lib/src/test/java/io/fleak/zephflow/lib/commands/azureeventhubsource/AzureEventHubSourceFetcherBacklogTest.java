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
