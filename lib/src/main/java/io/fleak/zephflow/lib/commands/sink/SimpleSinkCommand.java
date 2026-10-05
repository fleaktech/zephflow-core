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
package io.fleak.zephflow.lib.commands.sink;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import com.google.common.collect.Lists;
import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.execution.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Created by bolei on 4/17/24 <br>
 * This sink command takes a list of records, partition them into multiple batches according to the
 * predefined batch size, and flush each batch into sink synchronously.
 */
@Slf4j
public abstract class SimpleSinkCommand<T> extends ScalarSinkCommand {

  protected SimpleSinkCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator) {
    super(nodeId, jobContext, configParser, configValidator);
  }

  @Override
  public SinkResult writeToSink(
      List<RecordFleakData> events, @NonNull String callingUser, ExecutionContext context) {
    Map<String, String> tags =
        getCallingUserTagAndEventTags(callingUser, events.isEmpty() ? null : events.getFirst());

    //noinspection unchecked
    SinkExecutionContext<T> sinkContext = (SinkExecutionContext<T>) context;
    if (executionHooks != null) {
      SinkResult result = new SinkResult();
      try {
        for (List<RecordFleakData> batch : Lists.partition(events, batchSize())) {
          executionCheckpoint();
          result.merge(writeBoundedBatch(batch, tags, sinkContext));
        }
      } catch (ExecutionStoppedException stopped) {
        throw new ExecutionProgressStoppedException(stopped, List.of(), result.getFailureEvents());
      }
      return result;
    }
    // Outage fast-path: while store-and-forward is buffering, route everything straight to disk
    // (no remote attempt) so we don't reorder ahead of already-queued records.
    if (sinkContext.storeForward().isBuffering()) {
      return bufferDuringOutage(events, tags, sinkContext);
    }

    List<List<RecordFleakData>> batches = Lists.partition(events, batchSize());
    long ts = System.currentTimeMillis();

    SinkResult sinkResult = new SinkResult();
    batches.stream().map(p -> writeOneBatch(p, ts, tags, context)).forEach(sinkResult::merge);

    return sinkResult;
  }

  protected abstract int batchSize();

  private SinkResult writeBoundedBatch(
      List<RecordFleakData> batch, Map<String, String> tags, SinkExecutionContext<T> context) {
    context.inputMessageCounter().increase(batch.size(), tags);
    PreparedInputEvents<T> prepared = new PreparedInputEvents<>();
    List<ErrorOutput> errors = new ArrayList<>();
    long effectId = executionHooks.effects().started();
    boolean receiptPublished = false;
    try {
      long timestamp = System.currentTimeMillis();
      for (RecordFleakData record : batch) {
        executionCheckpoint();
        try {
          prepared.add(record, context.messagePreProcessor().preprocess(record, timestamp));
        } catch (ExecutionStoppedException failure) {
          throw failure;
        } catch (Exception failure) {
          errors.add(new ErrorOutput(record, failure.getMessage()));
          context.errorCounter().increase(tags);
        }
      }
      FlushResult flush;
      Throwable invocationFailure = null;
      try {
        flush =
            prepared.preparedList().isEmpty()
                ? new FlushResult(0, 0, List.of())
                : context.flusher().flushBounded(prepared, tags, executionHooks);
      } catch (ExecutionStoppedException failure) {
        flush =
            new FlushResult(0, 0, List.of(), EffectOutcome.unknown(prepared.preparedList().size()));
        invocationFailure = failure;
      } catch (BoundedFlushException failure) {
        flush = failure.result();
        invocationFailure = failure.getCause();
      } catch (Exception failure) {
        flush =
            new FlushResult(
                0,
                0,
                prepared.rawAndPreparedList().stream()
                    .map(pair -> new ErrorOutput(pair.getKey(), failure.getMessage()))
                    .toList(),
                EffectOutcome.unknown(prepared.preparedList().size()));
      }
      EffectOutcome outcome = flush.boundedOutcome(prepared.preparedList().size());
      if (!errors.isEmpty()) {
        outcome =
            outcome.merge(
                new EffectOutcome(
                    0L,
                    0L,
                    (long) errors.size(),
                    0L,
                    (long) errors.size(),
                    "preprocessing_rejected",
                    EffectOutcome.Delivery.FAILED));
      }
      errors.addAll(flush.errorOutputList());
      receiptPublished = true;
      executionHooks.effects().finished(effectId, outcome);
      context.sinkOutputCounter().increase(flush.successCount(), tags);
      context.outputSizeCounter().increase(flush.flushedDataSize(), tags);
      if (!errors.isEmpty()) context.sinkErrorCounter().increase(errors.size(), tags);
      if (invocationFailure instanceof ExecutionStoppedException stopped) throw stopped;
      executionCheckpoint();
      return new SinkResult(batch.size(), flush.successCount(), errors, outcome, invocationFailure);
    } catch (ExecutionStoppedException stopped) {
      if (!receiptPublished) {
        executionHooks
            .effects()
            .finished(
                effectId,
                EffectOutcome.counted(0, 0, errors.size(), 0, batch.size(), "preparation_stopped"));
      }
      throw new ExecutionProgressStoppedException(stopped, List.of(), errors);
    }
  }

  /**
   * Helper method to create base sink counters. Subclasses can use this to avoid code duplication.
   *
   * @return SinkCounters containing the standard 5 counters for sinks
   */
  protected static SinkCounters createSinkCounters(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      String commandName,
      String nodeId) {
    Map<String, String> metricTags =
        basicCommandMetricTags(jobContext.getMetricTags(), commandName, nodeId);
    FleakCounter inputMessageCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_EVENT_COUNT, metricTags);
    FleakCounter errorCounter =
        metricClientProvider.counter(METRIC_NAME_ERROR_EVENT_COUNT, metricTags);
    FleakCounter sinkOutputCounter =
        metricClientProvider.counter(METRIC_NAME_SINK_OUTPUT_COUNT, metricTags);
    FleakCounter outputSizeCounter =
        metricClientProvider.counter(METRIC_NAME_OUTPUT_EVENT_SIZE_COUNT, metricTags);
    FleakCounter sinkErrorCounter =
        metricClientProvider.counter(METRIC_NAME_SINK_ERROR_COUNT, metricTags);
    return new SinkCounters(
        inputMessageCounter, errorCounter, sinkOutputCounter, outputSizeCounter, sinkErrorCounter);
  }

  /** Helper record to hold sink counters */
  protected record SinkCounters(
      @NonNull FleakCounter inputMessageCounter,
      @NonNull FleakCounter errorCounter,
      @NonNull FleakCounter sinkOutputCounter,
      @NonNull FleakCounter outputSizeCounter,
      @NonNull FleakCounter sinkErrorCounter) {}

  private SinkResult writeOneBatch(
      List<RecordFleakData> batch,
      long ts,
      Map<String, String> callingUserTag,
      ExecutionContext context) {
    //noinspection unchecked
    SinkExecutionContext<T> sinkContext = (SinkExecutionContext<T>) context;

    sinkContext.inputMessageCounter().increase(batch.size(), callingUserTag);
    List<ErrorOutput> errorOutputs = new ArrayList<>();
    PreparedInputEvents<T> preparedInputEvents = new PreparedInputEvents<>();
    batch.forEach(
        rd -> {
          try {
            T prepared = sinkContext.messagePreProcessor().preprocess(rd, ts);
            preparedInputEvents.add(rd, prepared);
          } catch (Exception e) {
            log.debug("failed to preprocess event", e);
            sinkContext.errorCounter().increase(callingUserTag);
            errorOutputs.add(new ErrorOutput(rd, e.getMessage()));
          }
        });
    if (preparedInputEvents.rawAndPreparedList.isEmpty()) {
      return new SinkResult(batch.size(), 0, errorOutputs);
    }
    FlushResult flushResult;
    try {
      flushResult = sinkContext.flusher().flush(preparedInputEvents, callingUserTag);
    } catch (Exception e) {
      // Connectivity failure with store-and-forward enrolled: persist the prepared records to local
      // storage instead of dropping them. Only records that preprocessed successfully are buffered;
      // preprocess errors stay on the normal error path.
      if (sinkContext.storeForward().shouldBuffer(e)) {
        return bufferFailedBatch(
            batch, preparedInputEvents, errorOutputs, callingUserTag, sinkContext);
      }
      log.debug("failed to write to sink", e);
      // if error is thrown, it's a complete failure
      List<ErrorOutput> error =
          preparedInputEvents.rawAndPreparedList().stream()
              .map(pair -> new ErrorOutput(pair.getKey(), e.getMessage()))
              .toList();
      flushResult = new FlushResult(0, 0, error);
    }
    errorOutputs.addAll(flushResult.errorOutputList);
    sinkContext.sinkOutputCounter().increase(flushResult.successCount, callingUserTag);
    sinkContext.outputSizeCounter().increase(flushResult.flushedDataSize, callingUserTag);
    SinkResult sinkResult = new SinkResult(batch.size(), flushResult.successCount, errorOutputs);
    if (!errorOutputs.isEmpty()) {
      sinkContext.sinkErrorCounter().increase(errorOutputs.size(), callingUserTag);
    }
    return sinkResult;
  }

  private static final String STORE_FORWARD_FULL_MSG =
      "store-and-forward local buffer is full; record dropped";

  /**
   * Outage path from {@link #writeToSink}: persist all events to local storage, none sent to
   * remote.
   */
  private SinkResult bufferDuringOutage(
      List<RecordFleakData> events, Map<String, String> tags, SinkExecutionContext<T> sinkContext) {
    sinkContext.inputMessageCounter().increase(events.size(), tags);
    int stored = sinkContext.storeForward().offer(events);
    List<ErrorOutput> errors = new ArrayList<>();
    for (int i = stored; i < events.size(); i++) {
      errors.add(new ErrorOutput(events.get(i), STORE_FORWARD_FULL_MSG));
    }
    // Buffered records are safely persisted, so they count as success and the pipeline proceeds.
    SinkResult result = new SinkResult(events.size(), stored, errors);
    if (!errors.isEmpty()) {
      sinkContext.sinkErrorCounter().increase(errors.size(), tags);
    }
    return result;
  }

  /**
   * Failure path from {@link #writeOneBatch}: buffer the prepared records of a batch that hit a
   * connectivity failure.
   */
  private SinkResult bufferFailedBatch(
      List<RecordFleakData> batch,
      PreparedInputEvents<T> preparedInputEvents,
      List<ErrorOutput> preprocessErrors,
      Map<String, String> tags,
      SinkExecutionContext<T> sinkContext) {
    List<RecordFleakData> raws =
        preparedInputEvents.rawAndPreparedList().stream().map(Pair::getKey).toList();
    int stored = sinkContext.storeForward().offer(raws);
    List<ErrorOutput> errors = new ArrayList<>(preprocessErrors);
    for (int i = stored; i < raws.size(); i++) {
      errors.add(new ErrorOutput(raws.get(i), STORE_FORWARD_FULL_MSG));
    }
    SinkResult result = new SinkResult(batch.size(), stored, errors);
    if (!errors.isEmpty()) {
      sinkContext.sinkErrorCounter().increase(errors.size(), tags);
    }
    return result;
  }

  public interface SinkMessagePreProcessor<T> {
    T preprocess(RecordFleakData event, long ts) throws Exception;
  }

  /** Checks admission before a remote call, preserving a zero-attempt receipt on control stop. */
  public static void requireBoundedWriteAllowed(int count, ExecutionHooks hooks)
      throws BoundedFlushException {
    try {
      hooks.control().checkpoint();
    } catch (ExecutionStoppedException stopped) {
      throw new BoundedFlushException(
          new FlushResult(
              0,
              0,
              List.of(),
              new EffectOutcome(
                  0L, 0L, 0L, 0L, (long) count, "none", EffectOutcome.Delivery.NOT_ATTEMPTED)),
          stopped);
    }
  }

  public interface Flusher<T> extends Closeable {
    /**
     * Flushes a batch of events to target system.
     *
     * <p>Note: The implementation should handle partial failures and return a FlushResult object.
     * An exception does not establish whether an external system accepted the write.
     *
     * @param preparedInputEvents preprocessed input events and their corresponding raw input
     * @param metricTags tags to use when reporting metrics (e.g., callingUser, event metadata)
     * @return the flush result. It contains - successful write count - error event list if any
     * @throws Exception when the operation cannot return a receipt
     */
    FlushResult flush(
        final PreparedInputEvents<T> preparedInputEvents, Map<String, String> metricTags)
        throws Exception;

    /**
     * Synchronous bounded entry; buffered adapters must drain this exact operation before return.
     */
    default FlushResult flushBounded(
        PreparedInputEvents<T> events, Map<String, String> tags, ExecutionHooks hooks)
        throws Exception {
      requireBoundedWriteAllowed(events.preparedList().size(), hooks);
      return flush(events, tags);
    }

    /** Discards unsent records before closing resources. Buffered implementations override this. */
    default void abort() throws java.io.IOException {
      close();
    }
  }

  public record PreparedInputEvents<T>(
      List<T> preparedList, List<Pair<RecordFleakData, T>> rawAndPreparedList) {
    public PreparedInputEvents() {
      this(new ArrayList<>(), new ArrayList<>());
    }

    public void add(RecordFleakData raw, T prepared) {
      preparedList.add(prepared);
      rawAndPreparedList.add(Pair.of(raw, prepared));
    }
  }

  public record FlushResult(
      int successCount,
      long flushedDataSize,
      List<ErrorOutput> errorOutputList,
      EffectOutcome effectOutcome) {
    public FlushResult(int successCount, long flushedDataSize, List<ErrorOutput> errors) {
      this(successCount, flushedDataSize, errors, null);
    }

    /** Existing success receipts remain authoritative; an unclassified remainder stays unknown. */
    public EffectOutcome boundedOutcome(long attempted) {
      if (effectOutcome != null) return effectOutcome;
      if (successCount < 0 || successCount > attempted) {
        throw new IllegalStateException("Sink returned an invalid acknowledged count");
      }
      return new EffectOutcome(
          attempted,
          (long) successCount,
          0L,
          attempted - successCount,
          0L,
          "adapter_receipt",
          successCount == attempted
              ? EffectOutcome.Delivery.ACKNOWLEDGED
              : successCount > 0 ? EffectOutcome.Delivery.PARTIAL : EffectOutcome.Delivery.UNKNOWN);
    }
  }
}
