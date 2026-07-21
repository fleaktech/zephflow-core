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
package io.fleak.zephflow.lib.commands.influxdbsink;

import static io.fleak.zephflow.lib.utils.MiscUtils.COMMAND_NAME_INFLUXDB_SINK;
import static io.fleak.zephflow.lib.utils.MiscUtils.lookupApiKeyCredentialOpt;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.write.Point;
import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.credentials.ApiKeyCredential;

/** Sink that writes records to InfluxDB via the v2 write API. */
public class InfluxDbSinkCommand extends SimpleSinkCommand<Point> {

  private final InfluxDbClientProvider clientProvider;

  protected InfluxDbSinkCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      InfluxDbClientProvider clientProvider) {
    super(nodeId, jobContext, configParser, configValidator);
    this.clientProvider = clientProvider;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_INFLUXDB_SINK;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    SinkCounters counters =
        createSinkCounters(metricClientProvider, jobContext, commandName(), nodeId);

    InfluxDbSinkDto.Config config = (InfluxDbSinkDto.Config) commandConfig;

    SimpleSinkCommand.Flusher<Point> flusher = createFlusher(config, jobContext);
    SimpleSinkCommand.SinkMessagePreProcessor<Point> messagePreProcessor =
        new InfluxDbSinkMessageProcessor(
            config.getMeasurement(),
            config.getMeasurementField(),
            config.getTagFields(),
            config.getFieldFields(),
            config.getTimestampField(),
            config.getPrecision());

    return new SinkExecutionContext<>(
        flusher,
        messagePreProcessor,
        counters.inputMessageCounter(),
        counters.errorCounter(),
        counters.sinkOutputCounter(),
        counters.outputSizeCounter(),
        counters.sinkErrorCounter());
  }

  private SimpleSinkCommand.Flusher<Point> createFlusher(
      InfluxDbSinkDto.Config config, JobContext jobContext) {
    String token =
        lookupApiKeyCredentialOpt(jobContext, config.getCredentialId())
            .map(ApiKeyCredential::getKey)
            .orElse(null);
    InfluxDBClient client =
        clientProvider.create(config.getUrl(), token, config.getOrg(), config.getBucket());
    return new InfluxDbSinkFlusher(client);
  }

  @Override
  protected int batchSize() {
    InfluxDbSinkDto.Config config = (InfluxDbSinkDto.Config) commandConfig;
    return config.getBatchSize() == null
        ? InfluxDbSinkDto.DEFAULT_BATCH_SIZE
        : config.getBatchSize();
  }
}
