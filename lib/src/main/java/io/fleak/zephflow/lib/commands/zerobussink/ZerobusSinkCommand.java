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
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.credentials.DatabricksCredential;
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

    return new SinkExecutionContext<>(
        flusher,
        preprocessor,
        counters.inputMessageCounter(),
        counters.errorCounter(),
        counters.sinkOutputCounter(),
        counters.outputSizeCounter(),
        counters.sinkErrorCounter());
  }

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
