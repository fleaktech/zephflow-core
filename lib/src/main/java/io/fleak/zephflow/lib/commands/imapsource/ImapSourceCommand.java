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
package io.fleak.zephflow.lib.commands.imapsource;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.source.*;
import io.fleak.zephflow.lib.credentials.ApiKeyCredential;
import io.fleak.zephflow.lib.credentials.UsernamePasswordCredential;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import io.fleak.zephflow.lib.dlq.DlqWriterFactory;
import jakarta.mail.Session;
import jakarta.mail.Store;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ImapSourceCommand extends SimpleSourceCommand<EmailMessage> {

  public ImapSourceCommand(
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
    ImapSourceDto.Config config = (ImapSourceDto.Config) commandConfig;

    Fetcher<EmailMessage> fetcher = createImapFetcher(config, jobContext);
    RawDataEncoder<EmailMessage> encoder = new EmailRawDataEncoder();
    RawDataConverter<EmailMessage> converter = new EmailRawDataConverter();

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

  private Fetcher<EmailMessage> createImapFetcher(
      ImapSourceDto.Config config, JobContext jobContext) {
    try {
      Properties props = new Properties();
      String protocol = Boolean.TRUE.equals(config.getUseSsl()) ? "imaps" : "imap";

      props.setProperty("mail.store.protocol", protocol);
      props.setProperty("mail." + protocol + ".host", config.getHost());
      props.setProperty("mail." + protocol + ".port", String.valueOf(config.getPort()));

      if (Boolean.TRUE.equals(config.getUseSsl())) {
        props.setProperty("mail." + protocol + ".ssl.enable", "true");
      }

      if (config.getAuthType() == ImapSourceDto.AuthType.OAUTH2) {
        props.setProperty("mail." + protocol + ".auth.mechanisms", "XOAUTH2");
      }

      Session session = Session.getInstance(props);
      Store store = session.getStore(protocol);

      if (config.getAuthType() == ImapSourceDto.AuthType.PASSWORD) {
        UsernamePasswordCredential credential =
            lookupUsernamePasswordCredential(jobContext, config.getCredentialId());
        store.connect(
            config.getHost(), config.getPort(), credential.getUsername(), credential.getPassword());
      } else {
        ApiKeyCredential credential = lookupApiKeyCredential(jobContext, config.getCredentialId());
        store.connect(config.getHost(), config.getPort(), null, credential.getKey());
      }

      return new ImapSourceFetcher(
          store,
          config.getFolder(),
          config.getSearchCriteria(),
          Boolean.TRUE.equals(config.getMarkAsRead()),
          Boolean.TRUE.equals(config.getIncludeAttachments()),
          config.getMaxMessages());
    } catch (Exception e) {
      throw new RuntimeException("Failed to create IMAP fetcher", e);
    }
  }

  @Override
  public SourceType sourceType() {
    return SourceType.STREAMING;
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_IMAP_SOURCE;
  }
}
