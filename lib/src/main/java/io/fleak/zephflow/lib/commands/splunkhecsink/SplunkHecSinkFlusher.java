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

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;

import com.fasterxml.jackson.databind.JsonNode;
import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SplunkHecSinkFlusher implements SimpleSinkCommand.Flusher<SplunkHecOutboundEvent> {

  private static final int MAX_BODY_SNIPPET = 500;

  private final String hecUrl;
  private final String authHeader;
  private final HttpClient httpClient;

  public SplunkHecSinkFlusher(String hecUrl, String hecToken, HttpClient httpClient) {
    this.hecUrl = hecUrl;
    this.authHeader = "Splunk " + hecToken;
    this.httpClient = httpClient;
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<SplunkHecOutboundEvent> preparedInputEvents,
      Map<String, String> metricTags)
      throws Exception {
    List<SplunkHecOutboundEvent> events = preparedInputEvents.preparedList();
    if (events.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    for (SplunkHecOutboundEvent e : events) {
      buf.write(e.preEncodedNdjsonLine());
    }
    byte[] bodyBytes = buf.toByteArray();

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(hecUrl))
            .header("Authorization", authHeader)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes))
            .timeout(Duration.ofSeconds(60))
            .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    int status = response.statusCode();
    if (status == 200) {
      log.debug("Successfully sent {} events to Splunk HEC", events.size());
      return new SimpleSinkCommand.FlushResult(events.size(), bodyBytes.length, List.of());
    }

    String body = response.body() == null ? "" : response.body();
    String reason = "Splunk HEC error " + status + ": " + extractHecErrorText(body);
    log.error("Splunk HEC returned status {}: {}", status, body);
    List<ErrorOutput> errors =
        preparedInputEvents.rawAndPreparedList().stream()
            .map(p -> new ErrorOutput(p.getLeft(), reason))
            .toList();
    return new SimpleSinkCommand.FlushResult(0, 0, errors);
  }

  /** Returns the HEC `text` field from a JSON error body, or a truncated raw body fallback. */
  private static String extractHecErrorText(String body) {
    try {
      JsonNode node = OBJECT_MAPPER.readTree(body);
      JsonNode text = node.get("text");
      if (text != null && text.isTextual()) {
        return text.asText();
      }
    } catch (Exception ignore) {
      // Not JSON or not the expected shape — fall through to raw body.
    }
    return body.length() > MAX_BODY_SNIPPET ? body.substring(0, MAX_BODY_SNIPPET) : body;
  }

  @Override
  public void close() {}
}
