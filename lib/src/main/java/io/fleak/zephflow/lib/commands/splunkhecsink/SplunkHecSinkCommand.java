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
package io.fleak.zephflow.lib.commands.splunkhecsink;

import static io.fleak.zephflow.lib.utils.MiscUtils.*;

import io.fleak.zephflow.api.*;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.commands.sink.SinkExecutionContext;
import io.fleak.zephflow.lib.credentials.ApiKeyCredential;
import java.net.http.HttpClient;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.time.Duration;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

public class SplunkHecSinkCommand extends SimpleSinkCommand<SplunkHecOutboundEvent> {

  protected SplunkHecSinkCommand(
      String nodeId,
      JobContext jobContext,
      ConfigParser configParser,
      ConfigValidator configValidator) {
    super(nodeId, jobContext, configParser, configValidator);
  }

  @Override
  public String commandName() {
    return COMMAND_NAME_SPLUNK_HEC_SINK;
  }

  @Override
  protected ExecutionContext createExecutionContext(
      MetricClientProvider metricClientProvider,
      JobContext jobContext,
      CommandConfig commandConfig,
      String nodeId) {
    SinkCounters counters =
        createSinkCounters(metricClientProvider, jobContext, commandName(), nodeId);

    SplunkHecSinkDto.Config config = (SplunkHecSinkDto.Config) commandConfig;

    ApiKeyCredential credential = lookupApiKeyCredential(jobContext, config.getCredentialId());
    String hecToken = credential.getKey();

    boolean verifySsl = config.getVerifySsl() == null || config.getVerifySsl();
    HttpClient httpClient = buildHttpClient(verifySsl);

    SimpleSinkCommand.Flusher<SplunkHecOutboundEvent> flusher =
        new SplunkHecSinkFlusher(config.getHecUrl(), hecToken, httpClient);

    SimpleSinkCommand.SinkMessagePreProcessor<SplunkHecOutboundEvent> messagePreProcessor =
        new SplunkHecSinkMessageProcessor(
            config.getIndex(), config.getSourcetype(), config.getSource());

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
    SplunkHecSinkDto.Config config = (SplunkHecSinkDto.Config) commandConfig;
    return config.getBatchSize() != null
        ? config.getBatchSize()
        : SplunkHecSinkDto.DEFAULT_BATCH_SIZE;
  }

  /**
   * Builds the HttpClient. When verifySsl is false, the client trusts all certificates and skips
   * hostname verification — intended for self-signed Splunk deployments. The unsafe path is
   * deliberately confined to this method so it is easy to grep for.
   */
  static HttpClient buildHttpClient(boolean verifySsl) {
    HttpClient.Builder builder = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30));
    if (verifySsl) {
      return builder.build();
    }
    try {
      TrustManager[] trustAll =
          new TrustManager[] {
            new X509TrustManager() {
              @Override
              public void checkClientTrusted(X509Certificate[] chain, String authType) {}

              @Override
              public void checkServerTrusted(X509Certificate[] chain, String authType) {}

              @Override
              public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
              }
            }
          };
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(null, trustAll, new SecureRandom());
      SSLParameters sslParameters = new SSLParameters();
      sslParameters.setEndpointIdentificationAlgorithm("");
      return builder.sslContext(sslContext).sslParameters(sslParameters).build();
    } catch (Exception e) {
      throw new RuntimeException("Failed to build trust-all HttpClient", e);
    }
  }
}
