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
package io.fleak.zephflow.lib.commands.azuremonitorsink;

import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.lib.commands.azure.EntraIdTokenProvider;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AzureMonitorSinkFlusher
    implements SimpleSinkCommand.Flusher<AzureMonitorSinkOutboundEvent> {

  private static final String API_VERSION = "2023-01-01";

  private final String dceEndpoint;
  private final String dcrImmutableId;
  private final String streamName;
  private final EntraIdTokenProvider tokenProvider;
  private final HttpClient httpClient;

  public AzureMonitorSinkFlusher(
      String dceEndpoint,
      String dcrImmutableId,
      String streamName,
      EntraIdTokenProvider tokenProvider,
      HttpClient httpClient) {
    this.dceEndpoint = dceEndpoint;
    this.dcrImmutableId = dcrImmutableId;
    this.streamName = streamName;
    this.tokenProvider = tokenProvider;
    this.httpClient = httpClient;
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<AzureMonitorSinkOutboundEvent> preparedInputEvents,
      Map<String, String> metricTags)
      throws Exception {
    List<AzureMonitorSinkOutboundEvent> events = preparedInputEvents.preparedList();
    if (events.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    String jsonBody =
        "["
            + events.stream()
                .map(AzureMonitorSinkOutboundEvent::jsonPayload)
                .collect(Collectors.joining(","))
            + "]";
    byte[] bodyBytes = jsonBody.getBytes(StandardCharsets.UTF_8);

    try {
      String token = tokenProvider.getToken();
      HttpResponse<String> response = sendRequest(token, bodyBytes);

      if (response.statusCode() == 401) {
        tokenProvider.invalidate();
        token = tokenProvider.getToken();
        response = sendRequest(token, bodyBytes);
      }

      if (response.statusCode() == 204) {
        log.debug("Successfully sent {} events to Azure Monitor", events.size());
        return new SimpleSinkCommand.FlushResult(events.size(), bodyBytes.length, List.of());
      } else {
        final int statusCode = response.statusCode();
        final String responseBody = response.body();
        log.error("Azure Monitor API returned status {}: {}", statusCode, responseBody);
        List<ErrorOutput> errors =
            preparedInputEvents.rawAndPreparedList().stream()
                .map(
                    p ->
                        new ErrorOutput(
                            p.getLeft(),
                            "Azure Monitor API error " + statusCode + ": " + responseBody))
                .toList();
        return new SimpleSinkCommand.FlushResult(0, 0, errors);
      }
    } catch (Exception e) {
      log.error("Failed to send events to Azure Monitor", e);
      List<ErrorOutput> errors =
          preparedInputEvents.rawAndPreparedList().stream()
              .map(
                  p ->
                      new ErrorOutput(
                          p.getLeft(), "Azure Monitor connection error: " + e.getMessage()))
              .toList();
      return new SimpleSinkCommand.FlushResult(0, 0, errors);
    }
  }

  private HttpResponse<String> sendRequest(String token, byte[] bodyBytes) throws Exception {
    String endpoint =
        dceEndpoint
            + "/dataCollectionRules/"
            + dcrImmutableId
            + "/streams/"
            + streamName
            + "?api-version="
            + API_VERSION;

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(endpoint))
            .header("Authorization", "Bearer " + token)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes))
            .timeout(Duration.ofSeconds(60))
            .build();

    return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
  }

  @Override
  public void close() {}
}
