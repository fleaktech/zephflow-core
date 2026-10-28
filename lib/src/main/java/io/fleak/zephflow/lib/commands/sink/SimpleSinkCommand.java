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
import java.util.concurrent.TimeUnit;
import java.util.function.ToIntFunction;
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

  private static final long SPILL_MAX_BYTES = 1_073_741_824L; // 1 GiB
  private static final long SPILL_RETRY_INTERVAL_MS = 5_000L;
  private static final int SPILL_MAX_RETRIES = 3;
  // A spilled record has already been committed upstream, so retry it before giving up on it.
  private static final ConnectionFailureClassifier RETRY_EVERY_FAILURE = failure -> true;

  private final long spillAfterMillis;
  private transient volatile boolean spillNextBatch;

  protected SimpleSinkCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator) {
    super(nodeId, jobContext, configParser, configValidator);
    this.spillAfterMillis = spillAfterMillis(jobContext);
  }

  static long spillAfterMillis(JobContext jobContext) {
    Object value =
        jobContext == null || jobContext.getOtherProperties() == null
            ? null
            : jobContext.getOtherProperties().get(JobContext.SINK_SPILL_AFTER_MS);
    if (value == null) {
      return 0;
    }
    try {
      long millis = Long.parseLong(value.toString().trim());
      if (millis > 0) {
        return millis;
      }
    } catch (NumberFormatException ignored) {
    }
    log.warn("invalid {}={}; slow sinks will not spill", JobContext.SINK_SPILL_AFTER_MS, value);
    return 0;
  }

  @Override
  public void initialize(MetricClientProvider metricClientProvider) {
    super.initialize(metricClientProvider);
    if (spillAfterMillis == 0 || executionHooks != null) {
      return;
    }
    synchronized (this) {
      //noinspection unchecked
      SinkExecutionContext<T> context = (SinkExecutionContext<T>) executionContext;
      if (context.storeForward() == SinkStoreForward.noop()) {
        executionContext = withSpillBuffer(context, metricClientProvider);
      }
    }
  }

  private SinkExecutionContext<T> withSpillBuffer(
      SinkExecutionContext<T> context, MetricClientProvider metricClientProvider) {
    Map<String, String> metricTags =
        basicCommandMetricTags(jobContext.getMetricTags(), commandName(), nodeId);
    ChronicleStoreForward spillBuffer =
        new ChronicleStoreForward(
            new ChronicleStoreForward.Config(
                StoreForwardPaths.resolve(null, jobContext, nodeId),
                SPILL_MAX_BYTES,
                SPILL_RETRY_INTERVAL_MS,
                batchSize(),
                nodeId,
                SPILL_MAX_RETRIES),
            RETRY_EVERY_FAILURE,
            metricClientProvider.counter(METRIC_NAME_STORE_FORWARD_BUFFERED, metricTags),
            metricClientProvider.counter(METRIC_NAME_STORE_FORWARD_REPLAYED, metricTags),
            metricClientProvider.counter(METRIC_NAME_STORE_FORWARD_DROPPED, metricTags));
    spillBuffer.start(records -> replaySpilled(records, context));
    log.info("sink {} spills to disk after a flush slower than {} ms", nodeId, spillAfterMillis);
    return new SinkExecutionContext<>(
        context.flusher(),
        context.messagePreProcessor(),
        spillBuffer,
        context.inputMessageCounter(),
        context.errorCounter(),
        context.sinkOutputCounter(),
        context.outputSizeCounter(),
        context.sinkErrorCounter());
  }

  private void replaySpilled(List<RecordFleakData> records, SinkExecutionContext<T> context)
      throws Exception {
    Map<String, String> tags = Map.of();
    PreparedInputEvents<T> prepared = new PreparedInputEvents<>();
    List<ErrorOutput> errors = new ArrayList<>();
    long ts = System.currentTimeMillis();
    for (RecordFleakData record : records) {
      try {
        prepared.add(record, context.messagePreProcessor().preprocess(record, ts));
      } catch (Exception e) {
        context.errorCounter().increase(tags);
        errors.add(new ErrorOutput(record, e.getMessage()));
      }
    }
    if (!prepared.preparedList().isEmpty()) {
      FlushResult flushResult = context.flusher().flush(prepared, tags);
      context.sinkOutputCounter().increase(flushResult.successCount(), tags);
      context.outputSizeCounter().increase(flushResult.flushedDataSize(), tags);
      errors.addAll(flushResult.errorOutputList());
    }
    countSinkErrors(context.sinkErrorCounter(), errors, tags);
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
      countSinkErrors(context.sinkErrorCounter(), errors, tags);
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

  /**
   * Counts lost records on {@code sink_error_count} and keeps the first message as the run
   * summary's example of why they were lost.
   */
  static void countSinkErrors(
      FleakCounter sinkErrorCounter, List<ErrorOutput> errors, Map<String, String> tags) {
    if (errors.isEmpty()) {
      return;
    }
    sinkErrorCounter.increase(errors.size(), tags);
    sinkErrorCounter.recordErrorSample(errors.getFirst().errorMessage());
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
    SinkStoreForward storeForward = sinkContext.storeForward();
    if (spillNextBatch && storeForward != SinkStoreForward.noop()) {
      spillNextBatch = false;
      return bufferToDisk(
          batch,
          preparedInputEvents,
          errorOutputs,
          storeForward::spill,
          callingUserTag,
          sinkContext);
    }
    // While store-and-forward is buffering, go straight to disk so we don't reorder ahead of
    // already-queued records.
    if (storeForward.isBuffering()) {
      return bufferToDisk(
          batch,
          preparedInputEvents,
          errorOutputs,
          storeForward::offer,
          callingUserTag,
          sinkContext);
    }
    FlushResult flushResult;
    long flushStartNanos = System.nanoTime();
    try {
      flushResult = sinkContext.flusher().flush(preparedInputEvents, callingUserTag);
      if (spillAfterMillis > 0
          && System.nanoTime() - flushStartNanos
              > TimeUnit.MILLISECONDS.toNanos(spillAfterMillis)) {
        spillNextBatch = true;
      }
    } catch (Exception e) {
      // Connectivity failure with store-and-forward enrolled: persist the prepared records to local
      // storage instead of dropping them. Only records that preprocessed successfully are buffered;
      // preprocess errors stay on the normal error path.
      if (storeForward.shouldBuffer(e)) {
        return bufferToDisk(
            batch,
            preparedInputEvents,
            errorOutputs,
            storeForward::offer,
            callingUserTag,
            sinkContext);
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
    countSinkErrors(sinkContext.sinkErrorCounter(), errorOutputs, callingUserTag);
    return sinkResult;
  }

  private static final String METRIC_NAME_STORE_FORWARD_BUFFERED = "store_forward_buffered_count";
  private static final String METRIC_NAME_STORE_FORWARD_REPLAYED = "store_forward_replayed_count";
  private static final String METRIC_NAME_STORE_FORWARD_DROPPED = "store_forward_dropped_count";
  private static final String STORE_FORWARD_FULL_MSG =
      "store-and-forward local buffer is full; record dropped";

  private SinkResult bufferToDisk(
      List<RecordFleakData> batch,
      PreparedInputEvents<T> preparedInputEvents,
      List<ErrorOutput> preprocessErrors,
      ToIntFunction<List<RecordFleakData>> store,
      Map<String, String> tags,
      SinkExecutionContext<T> sinkContext) {
    List<RecordFleakData> raws =
        preparedInputEvents.rawAndPreparedList().stream().map(Pair::getKey).toList();
    int stored = store.applyAsInt(raws);
    List<ErrorOutput> errors = new ArrayList<>(preprocessErrors);
    for (int i = stored; i < raws.size(); i++) {
      errors.add(new ErrorOutput(raws.get(i), STORE_FORWARD_FULL_MSG));
    }
    SinkResult result = new SinkResult(batch.size(), stored, errors);
    countSinkErrors(sinkContext.sinkErrorCounter(), errors, tags);
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
