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
import com.azure.messaging.eventhubs.models.LastEnqueuedEventProperties;
import io.fleak.zephflow.lib.commands.source.CommitStrategy;
import io.fleak.zephflow.lib.commands.source.FatalPauseException;
import io.fleak.zephflow.lib.commands.source.FetchProgress;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.commands.source.SourceBacklog;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
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
 * <p>{@link #committer()} checkpoints, per partition, the latest event the pipeline has handled:
 * the first {@code handledRecords} events of the latest fetch, and every event of earlier fetches.
 * The checkpoint state is confined to the polling thread (changed only in {@link #fetch()} and
 * {@link #committer()}), so it needs no synchronization; the {@code queue} is the sole cross-thread
 * boundary.
 */
@Slf4j
public class AzureEventHubSourceFetcher implements Fetcher<SerializedEvent> {

  private static final Duration STOP_TIMEOUT = Duration.ofSeconds(30);

  private final EventProcessorClient processorClient;
  private final BlockingQueue<EventContext> queue;
  private final int maxEventsPerFetch;
  private final Duration pollTimeout;
  private final CommitStrategy commitStrategy;
  private final Function<EventContext, Map<String, String>> metadataExtractor;

  private final Map<String, EventContext> latestCheckpointByPartition = new LinkedHashMap<>();
  private final FetchProgress<EventContext> progress = new FetchProgress<>();
  // Null values mean unknown; both maps are confined to the polling thread.
  private final Map<String, Long> lastEnqueuedByPartition = new HashMap<>();
  private final Map<String, Long> checkpointedByPartition = new HashMap<>();
  private volatile SourceBacklog backlog = SourceBacklog.UNKNOWN;

  public AzureEventHubSourceFetcher(
      EventProcessorClient processorClient,
      BlockingQueue<EventContext> queue,
      int maxEventsPerFetch,
      Duration pollTimeout,
      CommitStrategy commitStrategy) {
    this(processorClient, queue, maxEventsPerFetch, pollTimeout, commitStrategy, ctx -> null);
  }

  public AzureEventHubSourceFetcher(
      EventProcessorClient processorClient,
      BlockingQueue<EventContext> queue,
      int maxEventsPerFetch,
      Duration pollTimeout,
      CommitStrategy commitStrategy,
      Function<EventContext, Map<String, String>> metadataExtractor) {
    this.processorClient = processorClient;
    this.queue = queue;
    this.maxEventsPerFetch = maxEventsPerFetch;
    this.pollTimeout = pollTimeout;
    this.commitStrategy = commitStrategy;
    this.metadataExtractor = metadataExtractor;
  }

  @Override
  public List<SerializedEvent> fetch() {
    List<SerializedEvent> rawEvents = new ArrayList<>();
    List<EventContext> drained = new ArrayList<>();
    try {
      // Block briefly for the first event so an idle source doesn't spin; the SimpleSourceCommand
      // loop adds its own backoff sleep when we return empty.
      EventContext first = queue.poll(pollTimeout.toMillis(), TimeUnit.MILLISECONDS);
      if (first != null) {
        accept(first, drained, rawEvents);
        // Drain whatever else is immediately available, up to the batch cap.
        while (rawEvents.size() < maxEventsPerFetch) {
          EventContext next = queue.poll();
          if (next == null) {
            break;
          }
          accept(next, drained, rawEvents);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.debug("AzureEventHubSourceFetcher interrupted while polling", e);
    }
    markHandled(progress.next(drained));
    updateBacklog();
    return rawEvents;
  }

  @Override
  public SourceBacklog backlog() {
    return backlog;
  }

  private void updateBacklog() {
    if (lastEnqueuedByPartition.isEmpty()) {
      backlog = SourceBacklog.UNKNOWN;
      return;
    }
    long total = 0;
    for (Map.Entry<String, Long> entry : lastEnqueuedByPartition.entrySet()) {
      Long checkpointed = checkpointedByPartition.get(entry.getKey());
      if (entry.getValue() == null || checkpointed == null) {
        backlog = SourceBacklog.UNKNOWN;
        return;
      }
      total += Math.max(0, entry.getValue() - checkpointed);
    }
    backlog = SourceBacklog.records(total, null);
  }

  private void accept(
      EventContext eventContext, List<EventContext> drained, List<SerializedEvent> rawEvents) {
    drained.add(eventContext);
    LastEnqueuedEventProperties lastEnqueued = eventContext.getLastEnqueuedEventProperties();
    lastEnqueuedByPartition.put(
        eventContext.getPartitionContext().getPartitionId(),
        lastEnqueued == null ? null : lastEnqueued.getSequenceNumber());
    EventData eventData = eventContext.getEventData();
    byte[] key =
        eventData.getPartitionKey() == null
            ? null
            : eventData.getPartitionKey().getBytes(StandardCharsets.UTF_8);
    Map<String, String> metadata = metadataExtractor.apply(eventContext);
    rawEvents.add(new SerializedEvent(key, eventData.getBody(), metadata));
  }

  @Override
  public Committer committer() {
    return new Committer() {
      @Override
      public void commit() {
        markHandled(progress.rest());
        checkpointHandled();
      }

      @Override
      public void commitThrough(int handledRecords) {
        markHandled(progress.through(handledRecords));
        checkpointHandled();
      }
    };
  }

  private void markHandled(List<EventContext> handled) {
    for (EventContext eventContext : handled) {
      latestCheckpointByPartition.put(
          eventContext.getPartitionContext().getPartitionId(), eventContext);
    }
  }

  private void checkpointHandled() {
    for (EventContext eventContext : latestCheckpointByPartition.values()) {
      eventContext.updateCheckpoint();
      checkpointedByPartition.put(
          eventContext.getPartitionContext().getPartitionId(),
          eventContext.getEventData().getSequenceNumber());
    }
    latestCheckpointByPartition.clear();
    updateBacklog();
  }

  @Override
  public CommitStrategy commitStrategy() {
    return commitStrategy;
  }

  /**
   * Stops the processor so its partitions go to other processors in the consumer group. Queued
   * events were never handled and are left to the next owner; the queue is emptied while stopping
   * so that partition callbacks blocked on a full queue can finish.
   */
  @Override
  public void pause() {
    CompletableFuture<Void> stopped =
        CompletableFuture.runAsync(() -> processorClient.stop(STOP_TIMEOUT));
    while (true) {
      queue.clear();
      try {
        stopped.get(50, TimeUnit.MILLISECONDS);
        break;
      } catch (TimeoutException e) {
        // keep emptying the queue
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new FatalPauseException("interrupted while stopping the Event Hub processor", e);
      } catch (ExecutionException e) {
        // The queue is already dropped; a still-running processor would checkpoint past it.
        throw new FatalPauseException("failed to stop the Event Hub processor", e.getCause());
      }
    }
    queue.clear();
    progress.next(List.of());
    latestCheckpointByPartition.clear();
    lastEnqueuedByPartition.clear();
    checkpointedByPartition.clear();
    backlog = SourceBacklog.UNKNOWN;
  }

  @Override
  public void resume() {
    processorClient.start();
  }

  @Override
  public void close() {
    // stop() blocks until partition processing has wound down; bound it so a wedged connection
    // can't hang terminate() indefinitely.
    processorClient.stop(STOP_TIMEOUT);
  }
}
