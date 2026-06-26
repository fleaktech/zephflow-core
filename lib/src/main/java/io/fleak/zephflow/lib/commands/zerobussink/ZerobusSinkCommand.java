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
package io.fleak.zephflow.lib.commands.zerobussink;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.ChronicleStoreForward;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.commands.sink.SinkStoreForward;
import io.fleak.zephflow.lib.credentials.DatabricksCredential;
import java.nio.file.Path;
import java.util.Map;

public class ZerobusSinkCommand extends SimpleSinkCommand<Map<String, Object>> {

  protected ZerobusSinkCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator) {
    super(nodeId, jobContext, configParser, configValidator);
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {

    ZerobusSinkDto.Config config = (ZerobusSinkDto.Config) commandConfig;

    SinkCounters counters =
        createSinkCounters(metricClientProvider, jobContext, commandName(), nodeId);

    DatabricksCredential credential =
        lookupDatabricksCredential(jobContext, config.getDatabricksCredentialId());
    Flusher<Map<String, Object>> flusher = ZerobusSinkFlusher.create(config, credential);

    SinkMessagePreProcessor<Map<String, Object>> preprocessor = new ZerobusMessageProcessor();

    SinkStoreForward storeForward =
        createStoreForward(metricClientProvider, jobContext, config, nodeId, flusher, preprocessor);

    return new SinkExecutionContext<>(
        flusher,
        preprocessor,
        storeForward,
        counters.inputMessageCounter(),
        counters.errorCounter(),
        counters.sinkOutputCounter(),
        counters.outputSizeCounter(),
        counters.sinkErrorCounter());
  }

  private SinkStoreForward createStoreForward(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      ZerobusSinkDto.Config config,
      String nodeId,
      Flusher<Map<String, Object>> flusher,
      SinkMessagePreProcessor<Map<String, Object>> preprocessor) {
    if (!config.isStoreAndForwardEnabled()) {
      return SinkStoreForward.noop();
    }

    Path storePath =
        (config.getLocalStorePath() != null && !config.getLocalStorePath().isBlank())
            ? Path.of(config.getLocalStorePath(), nodeId)
            : Path.of(System.getProperty("java.io.tmpdir"), "zephflow-store-forward", nodeId);

    Map<String, String> metricTags =
        basicCommandMetricTags(jobContext.getMetricTags(), commandName(), nodeId);

    ChronicleStoreForward storeForward =
        new ChronicleStoreForward(
            new ChronicleStoreForward.Config(
                storePath,
                config.getLocalStoreMaxBytes(),
                config.getForwardRetryIntervalMillis(),
                batchSize(),
                nodeId),
            new ZerobusConnectionFailureClassifier(),
            metricClientProvider.counter(METRIC_NAME_STORE_FORWARD_BUFFERED, metricTags),
            metricClientProvider.counter(METRIC_NAME_STORE_FORWARD_REPLAYED, metricTags),
            metricClientProvider.counter(METRIC_NAME_STORE_FORWARD_DROPPED, metricTags));

    // Replay re-runs the normal preprocess+flush; the flusher reconnects its stream as needed.
    storeForward.start(
        records -> {
          PreparedInputEvents<Map<String, Object>> prepared = new PreparedInputEvents<>();
          long ts = System.currentTimeMillis();
          for (RecordFleakData record : records) {
            prepared.add(record, preprocessor.preprocess(record, ts));
          }
          flusher.flush(prepared, Map.of());
        });
    return storeForward;
  }

  private static final String METRIC_NAME_STORE_FORWARD_BUFFERED = "store_forward_buffered_count";
  private static final String METRIC_NAME_STORE_FORWARD_REPLAYED = "store_forward_replayed_count";
  private static final String METRIC_NAME_STORE_FORWARD_DROPPED = "store_forward_dropped_count";

  @Override
  public String commandName() {
    return COMMAND_NAME_ZEROBUS_SINK;
  }

  // The flusher encodes a whole flush batch into memory and submits it via ingestRecordsOffset
  // before waiting for the last offset to be acknowledged. Two limits make an unbounded batch
  // dangerous: (1) the JVM would hold every encoded payload of an arbitrarily large upstream batch
  // at once, and (2) the SDK only keeps maxInflightRecords (default 50_000) records in flight. So
  // we cap the flush batch well under that window. SimpleSinkCommand then partitions the input into
  // chunks of this size; each chunk is fully drained (waitForOffset) before the next is ingested,
  // bounding both heap use and in-flight records.
  static final int ZEROBUS_FLUSH_BATCH_SIZE = 10_000;

  @Override
  protected int batchSize() {
    return ZEROBUS_FLUSH_BATCH_SIZE;
  }
}
