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
package io.fleak.zephflow.lib.commands.oktasource;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
import io.fleak.zephflow.lib.serdes.EncodingType;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.util.Map;
import java.util.Optional;

public class OktaSourceCommand extends SimpleSourceCommand<OktaLogEvent> {

  public OktaSourceCommand(
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
    OktaSourceDto.Config config = (OktaSourceDto.Config) commandConfig;

    UsernamePasswordCredential credential =
        lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
    String apiToken = credential.getPassword();

    int limit =
        config.getLimit() != null ? config.getLimit() : OktaSourceDto.DEFAULT_LIMIT;

    Fetcher<OktaLogEvent> fetcher =
        new OktaSourceFetcher(
            config.getOktaDomain(), apiToken, config.getSinceTimestamp(), config.getFilter(), limit);

    // Okta events are JSON; always use JSON deserializer
    FleakDeserializer<?> deserializer =
        DeserializerFactory.createDeserializerFactory(EncodingType.JSON).createDeserializer();
    RawDataConverter<OktaLogEvent> converter = new OktaRawDataConverter(deserializer);
    RawDataEncoder<OktaLogEvent> encoder = new OktaRawDataEncoder();

    Map<String, String> metricTags =
        basicCommandMetricTags(jobContext.getMetricTags(), commandName(), nodeId);
    FleakCounter dataSizeCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_EVENT_SIZE_COUNT, metricTags);
    FleakCounter inputEventCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_EVENT_COUNT, metricTags);
    FleakCounter deserializeFailureCounter =
        metricClientProvider.counter(METRIC_NAME_INPUT_DESER_ERR_COUNT, metricTags);

    String keyPrefix = (String) jobContext.getOtherProperties().get(JobContext.DATA_KEY_PREFIX);
    DlqWriter dlqWriter =
        Optional.of(jobContext)
            .map(JobContext::getDlqConfig)
            .map(c -> DlqWriterFactory.createDlqWriter(c, keyPrefix))
            .orElse(null);
    if (dlqWriter != null) {
      dlqWriter.open();
    }

    return new SourceExecutionContext<>(
        fetcher,
        converter,
        encoder,
        dataSizeCounter,
        inputEventCounter,
        deserializeFailureCounter,
        dlqWriter);
  }

  @Override
  public SourceType sourceType() {
    return SourceType.STREAMING;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_OKTA_SOURCE;
  }
}
