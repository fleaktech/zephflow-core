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
package io.fleak.zephflow.lib.commands.clickhousesink;

import static io.fleak.zephflow.lib.utils.MiscUtils.lookupUsernamePasswordCredentialOpt;

import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkCommandPartsFactory;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ClickHouseSinkPartsFactory extends SinkCommandPartsFactory<Map<String, Object>> {

  private final ClickHouseSinkDto.Config config;

  protected ClickHouseSinkPartsFactory(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      ClickHouseSinkDto.Config config) {
    super(metricClientProvider, jobContext);
    this.config = config;
  }

  @Override
  public SimpleSinkCommand.Flusher<Map<String, Object>> createFlusher() {
    var writer =
        new ClickHouseWriter(
            config,
            lookupUsernamePasswordCredentialOpt(jobContext, config.getCredentialId()).orElse(null));
    try {
      writer.downloadAndSetSchema(config.getDatabase(), config.getTable());
    } catch (Exception e) {
      log.error("Error downloading schema", e);
      throw new RuntimeException(e);
    }
    return writer;
  }

  @Override
  public ClickHouseMessageProcessor createMessagePreProcessor() {
    return new ClickHouseMessageProcessor();
  }
}
