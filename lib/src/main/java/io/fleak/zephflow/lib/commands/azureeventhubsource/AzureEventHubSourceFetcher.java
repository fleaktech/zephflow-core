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

import com.azure.messaging.eventhubs.EventData;
import com.azure.messaging.eventhubs.EventProcessorClient;
import com.azure.messaging.eventhubs.models.EventContext;
import io.fleak.zephflow.lib.commands.source.CommitStrategy;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

/**
 * Adapts Azure's <b>push-based</b> {@link EventProcessorClient} onto ZephFlow's <b>pull-based</b>
 * {@link Fetcher} contract.
 *
 * <p>The processor delivers events through a callback (one thread per owned partition) which
 * enqueue them into a bounded {@code queue}; a full queue blocks the callback and so applies
 * backpressure back to Event Hub. {@link #fetch()} drains that queue on the single source polling
 * thread.
 *
 * <p>Checkpointing mirrors the Kafka source's {@code commitAsync} semantics: {@link #committer()}
 * persists the offset of the <i>latest event drained per partition</i> since the previous commit.
 * Because the queue only ever hands the polling thread events it has actually dequeued, we never
 * checkpoint ahead of what was returned to the pipeline. The per-partition map is confined to the
 * polling thread (mutated only in {@link #fetch()}, read/cleared only in {@link #committer()}), so
 * it needs no synchronization; the {@code queue} is the sole cross-thread boundary.
 */
@Slf4j
public class AzureEventHubSourceFetcher implements Fetcher<SerializedEvent> {

  private static final Duration STOP_TIMEOUT = Duration.ofSeconds(30);

  private final EventProcessorClient processorClient;
  private final BlockingQueue<EventContext> queue;
  private final int maxEventsPerFetch;
  private final Duration pollTimeout;
  private final CommitStrategy commitStrategy;

  private final Map<String, EventContext> latestCheckpointByPartition = new LinkedHashMap<>();

  public AzureEventHubSourceFetcher(
      EventProcessorClient processorClient,
      BlockingQueue<EventContext> queue,
      int maxEventsPerFetch,
      Duration pollTimeout,
      CommitStrategy commitStrategy) {
    this.processorClient = processorClient;
    this.queue = queue;
    this.maxEventsPerFetch = maxEventsPerFetch;
    this.pollTimeout = pollTimeout;
    this.commitStrategy = commitStrategy;
  }

  @Override
  public List<SerializedEvent> fetch() {
    List<SerializedEvent> rawEvents = new ArrayList<>();
    try {
      // Block briefly for the first event so an idle source doesn't spin; the SimpleSourceCommand
      // loop adds its own backoff sleep when we return empty.
      EventContext first = queue.poll(pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
      if (first == null) {
        return rawEvents;
      }
      accept(first, rawEvents);
      // Drain whatever else is immediately available, up to the batch cap.
      while (rawEvents.size() < maxEventsPerFetch) {
        EventContext next = queue.poll();
        if (next == null) {
          break;
        }
        accept(next, rawEvents);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.debug("AzureEventHubSourceFetcher interrupted while polling", e);
    }
    return rawEvents;
  }

  private void accept(EventContext eventContext, List<SerializedEvent> rawEvents) {
    latestCheckpointByPartition.put(
        eventContext.getPartitionContext().getPartitionId(), eventContext);
    EventData eventData = eventContext.getEventData();
    byte[] key =
        eventData.getPartitionKey() == null
            ? null
            : eventData.getPartitionKey().getBytes(StandardCharsets.UTF_8);
    rawEvents.add(new SerializedEvent(key, eventData.getBody(), null));
  }

  @Override
  public Committer committer() {
    return () -> {
      for (EventContext eventContext : latestCheckpointByPartition.values()) {
        eventContext.updateCheckpoint();
      }
      latestCheckpointByPartition.clear();
    };
  }

  @Override
  public CommitStrategy commitStrategy() {
    return commitStrategy;
  }

  @Override
  public void close() {
    // stop() blocks until partition processing has wound down; bound it so a wedged connection
    // can't hang terminate() indefinitely.
    processorClient.stop(STOP_TIMEOUT);
  }
}
