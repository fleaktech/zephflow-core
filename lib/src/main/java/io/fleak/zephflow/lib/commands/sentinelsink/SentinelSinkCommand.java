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
package io.fleak.zephflow.lib.commands.sentinelsink;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import java.net.http.HttpClient;
import java.time.Duration;

public class SentinelSinkCommand extends SimpleSinkCommand<SentinelOutboundEvent> {

  protected SentinelSinkCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator) {
    super(nodeId, jobContext, configParser, configValidator);
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_SENTINEL_SINK;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    SinkCounters counters =
        createSinkCounters(metricClientProvider, jobContext, commandName(), nodeId);

    SentinelSinkDto.Config config = (SentinelSinkDto.Config) commandConfig;

    // TODO (Task 5): Wire up real EntraIdTokenProvider from config
    HttpClient httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
    EntraIdTokenProvider tokenProvider =
        new EntraIdTokenProvider("dummy-tenant", "dummy-client", "dummy-secret", httpClient);
    SimpleSinkCommand.Flusher<SentinelOutboundEvent> flusher =
        new SentinelSinkFlusher(
            "https://dummy.ingest.monitor.azure.com",
            "dcr-dummy",
            "Custom-Dummy_CL",
            config.getTimeGeneratedField(),
            tokenProvider,
            httpClient);

    SimpleSinkCommand.SinkMessagePreProcessor<SentinelOutboundEvent> messagePreProcessor =
        new SentinelSinkMessageProcessor(config.getTimeGeneratedField());

    return new SinkExecutionContext<>(
        flusher,
        messagePreProcessor,
        counters.inputMessageCounter(),
        counters.errorCounter(),
        counters.sinkOutputCounter(),
        counters.outputSizeCounter(),
        counters.sinkErrorCounter());
  }

  @Override
  protected int batchSize() {
    SentinelSinkDto.Config config = (SentinelSinkDto.Config) commandConfig;
    return config.getBatchSize() != null
        ? config.getBatchSize()
        : SentinelSinkDto.DEFAULT_BATCH_SIZE;
  }
}
