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
package io.fleak.zephflow.lib.commands.gcssource;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import com.google.cloud.storage.Storage;
import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.credentials.GcpCredential;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
import io.fleak.zephflow.lib.gcp.GcsClientFactory;
import io.fleak.zephflow.lib.serdes.des.DeserializerFactory;
import io.fleak.zephflow.lib.serdes.des.FleakDeserializer;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

public class GcsSourceCommand extends SimpleSourceCommand<GcsObjectData> {

  private final GcsClientFactory gcsClientFactory;

  public GcsSourceCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator,
      GcsClientFactory gcsClientFactory) {
    super(nodeId, jobContext, configParser, configValidator);
    this.gcsClientFactory = gcsClientFactory;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    GcsSourceDto.Config config = (GcsSourceDto.Config) commandConfig;

    Fetcher<GcsObjectData> fetcher = createFetcher(config, jobContext);
    RawDataEncoder<GcsObjectData> encoder = new GcsRawDataEncoder();
    RawDataConverter<GcsObjectData> converter = createConverter(config);

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

  private Fetcher<GcsObjectData> createFetcher(GcsSourceDto.Config config, JobContext jobContext) {
    Storage storage;
    if (StringUtils.isNotBlank(config.getCredentialId())) {
      GcpCredential credential = lookupGcpCredential(jobContext, config.getCredentialId());
      storage = gcsClientFactory.createStorageClient(credential);
    } else {
      storage = gcsClientFactory.createStorageClient();
    }
    return new GcsSourceFetcher(storage, config.getBucketName(), config.getObjectPrefix());
  }

  private RawDataConverter<GcsObjectData> createConverter(GcsSourceDto.Config config) {
    FleakDeserializer<?> deserializer =
        DeserializerFactory.createDeserializerFactory(config.getEncodingType())
            .createDeserializer();
    return new GcsRawDataConverter(deserializer);
  }

  @Override
  public SourceType sourceType() {
    return SourceType.BATCH;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_GCS_SOURCE;
  }
}
