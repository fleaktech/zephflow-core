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
package io.fleak.zephflow.lib.commands.elasticsearchsink;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

public class ElasticsearchSinkCommand extends SimpleSinkCommand<ElasticsearchOutboundDoc> {

  protected ElasticsearchSinkCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator) {
    super(nodeId, jobContext, configParser, configValidator);
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_ELASTICSEARCH_SINK;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    SinkCounters counters =
        createSinkCounters(metricClientProvider, jobContext, commandName(), nodeId);

    ElasticsearchSinkDto.Config config = (ElasticsearchSinkDto.Config) commandConfig;

    String username = null;
    String password = null;
    if (StringUtils.isNotBlank(config.getCredentialId())) {
      Optional<UsernamePasswordCredential> credOpt =
          lookupUsernamePasswordCredentialOpt(jobContext, config.getCredentialId());
      if (credOpt.isPresent()) {
        username = credOpt.get().getUsername();
        password = credOpt.get().getPassword();
      }
    }

    SimpleSinkCommand.Flusher<ElasticsearchOutboundDoc> flusher =
        new ElasticsearchSinkFlusher(config.getHost(), config.getIndex(), username, password);

    SimpleSinkCommand.SinkMessagePreProcessor<ElasticsearchOutboundDoc> messagePreProcessor =
        new ElasticsearchSinkMessageProcessor();

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
    ElasticsearchSinkDto.Config config = (ElasticsearchSinkDto.Config) commandConfig;
    return config.getBatchSize() != null
        ? config.getBatchSize()
        : ElasticsearchSinkDto.DEFAULT_BATCH_SIZE;
  }
}
