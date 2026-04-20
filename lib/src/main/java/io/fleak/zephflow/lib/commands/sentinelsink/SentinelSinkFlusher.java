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

import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import lombok.extern.slf4j.Slf4j;

/**
 * Flushes events to Azure Log Analytics via the HTTP Data Collector API.
 *
 * <p>Reference: https://docs.microsoft.com/en-us/azure/azure-monitor/logs/data-collector-api
 */
@Slf4j
public class SentinelSinkFlusher implements SimpleSinkCommand.Flusher<SentinelOutboundEvent> {

  private static final DateTimeFormatter RFC_1123_FORMATTER =
      DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'", Locale.ENGLISH)
          .withZone(ZoneOffset.UTC);
  private static final String API_VERSION = "2016-04-01";

  private final String workspaceId;
  private final String workspaceKey;
  private final String logType;
  private final String timeGeneratedField;
  private final HttpClient httpClient;

  public SentinelSinkFlusher(
      String workspaceId, String workspaceKey, String logType, String timeGeneratedField) {
    this.workspaceId = workspaceId;
    this.workspaceKey = workspaceKey;
    this.logType = logType;
    this.timeGeneratedField = timeGeneratedField;
    this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<SentinelOutboundEvent> preparedInputEvents,
      Map<String, String> metricTags)
      throws Exception {
    List<SentinelOutboundEvent> events = preparedInputEvents.preparedList();
    if (events.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    // Build JSON array body
    String jsonBody =
        "["
            + events.stream()
                .map(SentinelOutboundEvent::jsonPayload)
                .collect(Collectors.joining(","))
            + "]";
    byte[] bodyBytes = jsonBody.getBytes(StandardCharsets.UTF_8);

    String dateStr = RFC_1123_FORMATTER.format(ZonedDateTime.now(ZoneOffset.UTC));
    String authorization = buildAuthorization(dateStr, bodyBytes.length);

    String endpoint =
        "https://" + workspaceId + ".ods.opinsights.azure.com/api/logs?api-version=" + API_VERSION;

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(endpoint))
            .header("Log-Type", logType)
            .header("x-ms-date", dateStr)
            .header("time-generated-field", timeGeneratedField)
            .header("Content-Type", "application/json")
            .header("Authorization", authorization)
            .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes))
            .timeout(Duration.ofSeconds(60))
            .build();

    try {
      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() == 200) {
        log.debug("Successfully sent {} events to Microsoft Sentinel", events.size());
        return new SimpleSinkCommand.FlushResult(events.size(), bodyBytes.length, List.of());
      } else {
        log.error("Sentinel API returned status {}: {}", response.statusCode(), response.body());
        List<ErrorOutput> errors =
            preparedInputEvents.rawAndPreparedList().stream()
                .map(
                    p ->
                        new ErrorOutput(
                            p.getLeft(),
                            "Sentinel API error " + response.statusCode() + ": " + response.body()))
                .collect(Collectors.toList());
        return new SimpleSinkCommand.FlushResult(0, 0, errors);
      }
    } catch (Exception e) {
      log.error("Failed to send events to Microsoft Sentinel", e);
      List<ErrorOutput> errors =
          preparedInputEvents.rawAndPreparedList().stream()
              .map(
                  p -> new ErrorOutput(p.getLeft(), "Sentinel connection error: " + e.getMessage()))
              .collect(Collectors.toList());
      return new SimpleSinkCommand.FlushResult(0, 0, errors);
    }
  }

  private String buildAuthorization(String dateStr, int contentLength) throws Exception {
    String stringToSign =
        "POST\n" + contentLength + "\napplication/json\nx-ms-date:" + dateStr + "\n/api/logs";

    Mac mac = Mac.getInstance("HmacSHA256");
    mac.init(new SecretKeySpec(Base64.getDecoder().decode(workspaceKey), "HmacSHA256"));
    String signature =
        Base64.getEncoder()
            .encodeToString(mac.doFinal(stringToSign.getBytes(StandardCharsets.UTF_8)));

    return "SharedKey " + workspaceId + ":" + signature;
  }

  @Override
  public void close() {
    // HttpClient does not require explicit closing
  }
}
