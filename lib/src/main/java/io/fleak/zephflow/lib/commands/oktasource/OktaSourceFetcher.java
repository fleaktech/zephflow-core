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

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;

import com.fasterxml.jackson.core.type.TypeReference;
import io.fleak.zephflow.lib.commands.source.CommitStrategy;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.commands.source.NoCommitStrategy;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class OktaSourceFetcher implements Fetcher<OktaLogEvent> {

  private final String oktaDomain;
  private final String apiToken;
  private final String filter;
  private final int limit;

  private final HttpClient httpClient;

  /** URL for the next page of results (from Link header). Null means use sinceTimestamp. */
  private String nextUrl;
  /** ISO 8601 timestamp for the since parameter when nextUrl is null. */
  private String sinceTimestamp;

  public OktaSourceFetcher(
      String oktaDomain, String apiToken, String sinceTimestamp, String filter, int limit) {
    this.oktaDomain = oktaDomain;
    this.apiToken = apiToken;
    this.filter = filter;
    this.limit = limit;
    this.sinceTimestamp =
        StringUtils.isNotBlank(sinceTimestamp)
            ? sinceTimestamp
            : DateTimeFormatter.ISO_INSTANT.format(Instant.now());
    this.httpClient =
        HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();
  }

  @Override
  public List<OktaLogEvent> fetch() {
    String url = buildUrl();
    log.debug("Fetching Okta System Log events from: {}", url);

    try {
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(url))
              .header("Authorization", "SSWS " + apiToken)
              .header("Accept", "application/json")
              .GET()
              .timeout(Duration.ofSeconds(60))
              .build();

      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() == 401) {
        throw new RuntimeException("Okta authentication failed: invalid API token");
      }
      if (response.statusCode() != 200) {
        log.warn(
            "Okta API returned status {}: {}", response.statusCode(), response.body());
        return List.of();
      }

      // Parse next URL from Link header
      nextUrl = extractNextUrl(response.headers().firstValue("Link").orElse(null));

      // Parse events
      List<Map<String, Object>> rawEvents =
          OBJECT_MAPPER.readValue(
              response.body(), new TypeReference<List<Map<String, Object>>>() {});

      if (rawEvents.isEmpty()) {
        // No new events; update sinceTimestamp to now for the next fetch
        if (nextUrl == null) {
          sinceTimestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
        }
        return List.of();
      }

      List<OktaLogEvent> events = new ArrayList<>(rawEvents.size());
      for (Map<String, Object> rawEvent : rawEvents) {
        String eventId = rawEvent.getOrDefault("uuid", "").toString();
        events.add(new OktaLogEvent(rawEvent, eventId));
      }

      log.debug("Fetched {} Okta System Log events", events.size());
      return events;

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      log.warn("Error fetching Okta events: {}", e.getMessage(), e);
      return List.of();
    }
  }

  private String buildUrl() {
    if (nextUrl != null) {
      return nextUrl;
    }
    StringBuilder sb = new StringBuilder();
    sb.append("https://").append(oktaDomain).append("/api/v1/logs");
    sb.append("?limit=").append(limit);
    sb.append("&since=").append(URLEncoder.encode(sinceTimestamp, StandardCharsets.UTF_8));
    if (StringUtils.isNotBlank(filter)) {
      sb.append("&filter=").append(URLEncoder.encode(filter, StandardCharsets.UTF_8));
    }
    return sb.toString();
  }

  private String extractNextUrl(String linkHeader) {
    if (StringUtils.isBlank(linkHeader)) {
      return null;
    }
    for (String part : linkHeader.split(",")) {
      if (part.contains("rel=\"next\"")) {
        String url = part.trim().replaceAll("<(.+)>;.*", "$1").trim();
        return url.isEmpty() ? null : url;
      }
    }
    return null;
  }

  @Override
  public boolean isExhausted() {
    return false; // Streaming source - never exhausted
  }

  @Override
  public CommitStrategy commitStrategy() {
    return NoCommitStrategy.INSTANCE;
  }

  @Override
  public void close() throws IOException {
    // HttpClient does not require explicit closing
  }
}
