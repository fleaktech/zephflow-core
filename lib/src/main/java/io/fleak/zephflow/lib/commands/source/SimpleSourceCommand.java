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
package io.fleak.zephflow.lib.commands.source;

import static io.fleak.zephflow.lib.utils.MiscUtils.threadSleep;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.serdes.SerializedEvent;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

/** Created by bolei on 9/23/24 */
@Slf4j
public abstract class SimpleSourceCommand<T> extends SourceCommand {
  private static final int SLEEP_INIT = 100;
  private static final int SLEEP_INC = 100;
  private static final int SLEEP_MAX = 2000;

  private final AtomicBoolean finished = new AtomicBoolean();
  private final boolean singleEventSource;

  protected SimpleSourceCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      CommandInitializerFactory commandInitializerFactory) {
    this(nodeId, jobContext, configParser, configValidator, commandInitializerFactory, false);
  }

  protected SimpleSourceCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      CommandInitializerFactory commandInitializerFactory,
      boolean singleEventSource) {
    super(nodeId, jobContext, configParser, configValidator, commandInitializerFactory);
    this.singleEventSource = singleEventSource;
  }

  @Override
  public void execute(
      String callingUser,
      MetricClientProvider metricClientProvider,
      SourceEventAcceptor sourceEventAcceptor) {
    lazyInitialize(metricClientProvider);
    //noinspection unchecked
    SourceInitializedConfig<T> sourceInitializedConfig =
        (SourceInitializedConfig<T>) initializedConfigThreadLocal.get();

    RawDataConverter<T> converter = sourceInitializedConfig.converter();
    RawDataEncoder<T> encoder = sourceInitializedConfig.encoder();
    Fetcher.Committer committer = sourceInitializedConfig.fetcher().commiter();

    DlqWriter dlqWriter = sourceInitializedConfig.dlqWriter();
    try {
      int sleep = SLEEP_INIT;
      while (!finished.get()) {
        // 1. Fetch source-specific records
        List<T> fetchedData = doFetch(sourceInitializedConfig.fetcher());
        if (singleEventSource) {
          finished.set(true);
        }
        if (CollectionUtils.isEmpty(fetchedData)) {
          log.trace("No fetched data found, sleeping for {} ms", sleep);
          threadSleep(sleep);
          sleep = Math.min(sleep + SLEEP_INC, SLEEP_MAX);
          continue;
        }
        // 2. Convert raw input into internal data structure
        List<ConvertedResult<T>> convertedResults =
            fetchedData.stream().map(converter::convert).toList();

        processFetchedData(convertedResults, sourceEventAcceptor, committer, dlqWriter, encoder);
      }
    } catch (Exception e) {
      log.error("Fleak Source unexpected exception", e);
    } finally {
      try {
        sourceEventAcceptor.terminate();
      } catch (Exception e) {
        log.error("failed to terminate sourceEventAcceptor", e);
      }

      try {
        terminate();
      } catch (Exception e) {
        log.error("failed to terminate source command", e);
      }
    }
  }

  private List<T> doFetch(Fetcher<T> fetcher) {

    if (!finished.get()) {
      List<T> fetchedData = fetcher.fetch();
      log.trace("fetched {} records from source", CollectionUtils.size(fetchedData));
      return fetchedData;
    }
    return null;
  }

  private void processFetchedData(
      List<ConvertedResult<T>> convertedResults,
      SourceEventAcceptor sourceEventAcceptor,
      Fetcher.Committer committer,
      DlqWriter dlqWriter,
      RawDataEncoder<T> rawDataEncoder) {
    convertedResults.forEach(
        convertedResult -> {
          try {
            if (convertedResult.getTransformedData() == null) {
              throw convertedResult.getError();
            }
            sourceEventAcceptor.accept(convertedResult.getTransformedData());
          } catch (Exception e) {
            log.debug("failed to process data: {}", convertedResult.getTransformedData(), e);
            if (dlqWriter != null) {
              SerializedEvent raw = rawDataEncoder.serialize(convertedResult.getSourceRecord());
              dlqWriter.writeToDlq(
                  System.currentTimeMillis(), raw, ExceptionUtils.getStackTrace(e));
            }
          } finally {
            try {
              if (committer != null) {
                committer.commit();
              }
            } catch (Exception e) {
              log.error("failed to commit", e);
            }
          }
        });
  }

  @Override
  public void terminate() throws Exception {
    finished.set(true);
    super.terminate();
  }
}
