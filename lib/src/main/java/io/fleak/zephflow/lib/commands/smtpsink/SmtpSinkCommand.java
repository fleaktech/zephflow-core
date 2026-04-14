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
package io.fleak.zephflow.lib.commands.smtpsink;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import jakarta.mail.Session;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SmtpSinkCommand extends SimpleSinkCommand<PreparedEmail> {

  private static final int SMTP_SINK_BATCH_SIZE = 50;

  protected SmtpSinkCommand(
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
    SinkCounters counters =
        createSinkCounters(metricClientProvider, jobContext, commandName(), nodeId);

    SmtpSinkDto.Config config = (SmtpSinkDto.Config) commandConfig;

    UsernamePasswordCredential credential =
        lookupUsernamePasswordCredential(jobContext, config.getCredentialId());

    Session session = createSmtpSession(config);

    SimpleSinkCommand.Flusher<PreparedEmail> flusher =
        new SmtpSinkFlusher(session, credential.getUsername(), credential.getPassword());
    SimpleSinkCommand.SinkMessagePreProcessor<PreparedEmail> messagePreProcessor =
        new SmtpSinkMessageProcessor(config);

    return new SinkExecutionContext<>(
        flusher,
        messagePreProcessor,
        counters.inputMessageCounter(),
        counters.errorCounter(),
        counters.sinkOutputCounter(),
        counters.outputSizeCounter(),
        counters.sinkErrorCounter());
  }

  static Session createSmtpSession(SmtpSinkDto.Config config) {
    Properties props = new Properties();
    props.setProperty("mail.smtp.host", config.getHost());
    props.setProperty("mail.smtp.port", String.valueOf(config.getPort()));
    props.setProperty("mail.smtp.auth", "true");

    if (Boolean.TRUE.equals(config.getUseTls())) {
      props.setProperty("mail.smtp.starttls.enable", "true");
    }

    return Session.getInstance(props);
  }

  @Override
  protected int batchSize() {
    return SMTP_SINK_BATCH_SIZE;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_SMTP_SINK;
  }
}
