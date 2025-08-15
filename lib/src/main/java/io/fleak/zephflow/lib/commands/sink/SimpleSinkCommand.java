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
      ConfigValidator configValidator,
      SinkCommandInitializerFactory<T> sinkCommandInitializerFactory) {
    super(nodeId, jobContext, configParser, configValidator, sinkCommandInitializerFactory);
  }

  @Override
  public SinkResult doWriteToSink(List<RecordFleakData> events, @NonNull String callingUser) {
    Map<String, String> tags =
        getCallingUserTagAndEventTags(callingUser, events.isEmpty() ? null : events.get(0));
    List<List<RecordFleakData>> batches = Lists.partition(events, batchSize());
    long ts = System.currentTimeMillis();

    SinkResult sinkResult = new SinkResult();
    batches.stream().map(p -> writeOneBatch(p, ts, tags)).forEach(sinkResult::merge);

    return sinkResult;
  }

  protected abstract int batchSize();

  private SinkResult writeOneBatch(
      List<RecordFleakData> batch, long ts, Map<String, String> callingUserTag) {
    //noinspection unchecked
    SinkInitializedConfig<T> sinkInitializedConfig =
        (SinkInitializedConfig<T>) initializedConfigThreadLocal.get();

    sinkInitializedConfig.inputMessageCounter().increase(batch.size(), callingUserTag);
    List<ErrorOutput> errorOutputs = new ArrayList<>();
    PreparedInputEvents<T> preparedInputEvents = new PreparedInputEvents<>();
    batch.forEach(
        rd -> {
          try {
            T prepared = sinkInitializedConfig.messagePreProcessor().preprocess(rd, ts);
            preparedInputEvents.add(rd, prepared);
          } catch (Exception e) {
            log.debug("failed to preprocess event", e);
            sinkInitializedConfig.errorCounter().increase(callingUserTag);
            errorOutputs.add(new ErrorOutput(rd, e.getMessage()));
          }
        });
    if (preparedInputEvents.rawAndPreparedList.isEmpty()) {
      return new SinkResult(batch.size(), 0, errorOutputs);
    }
    FlushResult flushResult;
    try {
      flushResult = sinkInitializedConfig.flusher().flush(preparedInputEvents);
    } catch (Exception e) {
      log.debug("failed to write to sink", e);
      // if error is thrown, it's a complete failure
      List<ErrorOutput> error =
          preparedInputEvents.rawAndPreparedList().stream()
              .map(pair -> new ErrorOutput(pair.getKey(), e.getMessage()))
              .toList();
      flushResult = new FlushResult(0, 0, error);
    }
    errorOutputs.addAll(flushResult.errorOutputList);
    sinkInitializedConfig.sinkOutputCounter().increase(flushResult.successCount, callingUserTag);
    sinkInitializedConfig.outputSizeCounter().increase(flushResult.flushedDataSize, callingUserTag);
    SinkResult sinkResult = new SinkResult(batch.size(), flushResult.successCount, errorOutputs);
    sinkInitializedConfig.sinkErrorCounter().increase(sinkResult.errorCount(), callingUserTag);
    return sinkResult;
  }

  public interface SinkMessagePreProcessor<T> {
    T preprocess(RecordFleakData event, long ts) throws Exception;
  }

  public interface Flusher<T> extends Closeable {
    /**
     * Flushes a batch of events to target system.
     *
     * <p>Note: The implementation should handle partial failures and return a FlushResult object.
     * If an exception is thrown, it means complete failure
     *
     * @param preparedInputEvents preprocessed input events and their corresponding raw input
     * @return the flush result. It contains - successful write count - error event list if any
     * @throws Exception If any exception is thrown, it means nothing is written
     */
    FlushResult flush(final PreparedInputEvents<T> preparedInputEvents) throws Exception;
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
      int successCount, long flushedDataSize, List<ErrorOutput> errorOutputList) {}
}
