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
package io.fleak.zephflow.lib.commands.elasticsearchsource;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;

import com.fasterxml.jackson.databind.JsonNode;
import io.fleak.zephflow.lib.commands.source.CommitStrategy;
import io.fleak.zephflow.lib.commands.source.Fetcher;
import io.fleak.zephflow.lib.commands.source.NoCommitStrategy;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class ElasticsearchSourceFetcher implements Fetcher<ElasticsearchDocument> {

  private final String host;
  private final String index;
  private final String query;
  private final String scrollTimeout;
  private final int batchSize;
  private final HttpClient httpClient;
  private final String authHeader;

  private String scrollId;
  private boolean exhausted = false;

  public ElasticsearchSourceFetcher(
      String host,
      String index,
      String query,
      String scrollTimeout,
      int batchSize,
      String username,
      String password) {
    this.host = host.endsWith("/") ? host.substring(0, host.length() - 1) : host;
    this.index = index;
    this.query = query;
    this.scrollTimeout = scrollTimeout;
    this.batchSize = batchSize;
    this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();

    if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
      String credentials = username + ":" + password;
      this.authHeader =
          "Basic "
              + Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    } else {
      this.authHeader = null;
    }
  }

  @Override
  public List<ElasticsearchDocument> fetch() {
    try {
      if (exhausted) {
        return List.of();
      }

      if (scrollId == null) {
        return initialSearch();
      } else {
        return continueScroll();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Elasticsearch fetch interrupted", e);
    } catch (Exception e) {
      throw new RuntimeException("Failed to fetch from Elasticsearch", e);
    }
  }

  private List<ElasticsearchDocument> initialSearch() throws Exception {
    String bodyQuery = buildInitialBody();
    String url = host + "/" + index + "/_search?scroll=" + scrollTimeout;

    HttpRequest request = buildRequest(url, bodyQuery);
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new RuntimeException(
          "Elasticsearch initial search failed with status "
              + response.statusCode()
              + ": "
              + response.body());
    }

    return parseScrollResponse(response.body());
  }

  private List<ElasticsearchDocument> continueScroll() throws Exception {
    var bodyNode = OBJECT_MAPPER.createObjectNode();
    bodyNode.put("scroll", scrollTimeout);
    bodyNode.put("scroll_id", scrollId);
    String url = host + "/_search/scroll";

    HttpRequest request = buildRequest(url, OBJECT_MAPPER.writeValueAsString(bodyNode));
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new RuntimeException(
          "Elasticsearch scroll failed with status "
              + response.statusCode()
              + ": "
              + response.body());
    }

    return parseScrollResponse(response.body());
  }

  private HttpRequest buildRequest(String url, String body) {
    HttpRequest.Builder builder =
        HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
            .timeout(Duration.ofSeconds(60));

    if (authHeader != null) {
      builder.header("Authorization", authHeader);
    }

    return builder.build();
  }

  @SuppressWarnings("unchecked")
  private List<ElasticsearchDocument> parseScrollResponse(String body) throws IOException {
    JsonNode root = OBJECT_MAPPER.readTree(body);

    JsonNode scrollIdNode = root.get("_scroll_id");
    if (scrollIdNode != null) {
      scrollId = scrollIdNode.asText();
    }

    JsonNode hitsNode = root.path("hits").path("hits");
    if (!hitsNode.isArray() || hitsNode.isEmpty()) {
      exhausted = true;
      clearScroll();
      return List.of();
    }

    List<ElasticsearchDocument> docs = new ArrayList<>();
    for (JsonNode hit : hitsNode) {
      String id = hit.path("_id").asText();
      String docIndex = hit.path("_index").asText();
      JsonNode sourceNode = hit.path("_source");
      Map<String, Object> source =
          sourceNode.isObject()
              ? OBJECT_MAPPER.convertValue(sourceNode, Map.class)
              : new LinkedHashMap<>();
      docs.add(new ElasticsearchDocument(id, docIndex, source));
    }

    log.debug("Fetched {} documents from Elasticsearch scroll", docs.size());
    return docs;
  }

  private void clearScroll() {
    if (scrollId == null) {
      return;
    }
    try {
      var bodyNode = OBJECT_MAPPER.createObjectNode();
      bodyNode.putArray("scroll_id").add(scrollId);
      String bodyJson = OBJECT_MAPPER.writeValueAsString(bodyNode);
      HttpRequest.Builder builder =
          HttpRequest.newBuilder()
              .uri(URI.create(host + "/_search/scroll"))
              .header("Content-Type", "application/json")
              .method(
                  "DELETE", HttpRequest.BodyPublishers.ofString(bodyJson, StandardCharsets.UTF_8))
              .timeout(Duration.ofSeconds(10));
      if (authHeader != null) {
        builder.header("Authorization", authHeader);
      }
      httpClient.send(builder.build(), HttpResponse.BodyHandlers.discarding());
    } catch (Exception e) {
      log.warn("Failed to clear Elasticsearch scroll context", e);
    }
  }

  private String buildInitialBody() throws IOException {
    var body = OBJECT_MAPPER.createObjectNode();
    body.put("size", batchSize);
    if (StringUtils.isNotBlank(query)) {
      body.set("query", OBJECT_MAPPER.readTree(query));
    } else {
      body.putObject("query").putObject("match_all");
    }
    return OBJECT_MAPPER.writeValueAsString(body);
  }

  @Override
  public boolean isExhausted() {
    return exhausted;
  }

  @Override
  public CommitStrategy commitStrategy() {
    return NoCommitStrategy.INSTANCE;
  }

  @Override
  public void close() throws IOException {
    clearScroll();
  }
}
