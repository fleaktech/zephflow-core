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

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;

import com.fasterxml.jackson.databind.JsonNode;
import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

/**
 * Flushes events to Elasticsearch using the Bulk API.
 *
 * <p>Reference:
 * https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
 */
@Slf4j
public class ElasticsearchSinkFlusher
    implements SimpleSinkCommand.Flusher<ElasticsearchOutboundDoc> {

  private final String host;
  private final String index;
  private final String authHeader;
  private final HttpClient httpClient;

  public ElasticsearchSinkFlusher(String host, String index, String username, String password) {
    this.host = host.endsWith("/") ? host.substring(0, host.length() - 1) : host;
    this.index = index;
    this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(30)).build();

    if (StringUtils.isNotBlank(username) && StringUtils.isNotBlank(password)) {
      String credentials = username + ":" + password;
      this.authHeader =
          "Basic "
              + Base64.getEncoder()
                  .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    } else {
      this.authHeader = null;
    }
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<ElasticsearchOutboundDoc> preparedInputEvents,
      Map<String, String> metricTags)
      throws Exception {
    List<ElasticsearchOutboundDoc> docs = preparedInputEvents.preparedList();
    if (docs.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    // Build NDJSON bulk request body
    String actionMeta = "{\"index\":{\"_index\":\"" + index + "\"}}";
    StringBuilder ndjson = new StringBuilder();
    for (ElasticsearchOutboundDoc doc : docs) {
      ndjson.append(actionMeta).append("\n");
      ndjson.append(doc.jsonPayload()).append("\n");
    }

    byte[] bodyBytes = ndjson.toString().getBytes(StandardCharsets.UTF_8);
    String url = host + "/_bulk";

    HttpRequest.Builder builder =
        HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Content-Type", "application/x-ndjson")
            .POST(HttpRequest.BodyPublishers.ofByteArray(bodyBytes))
            .timeout(Duration.ofSeconds(60));

    if (authHeader != null) {
      builder.header("Authorization", authHeader);
    }

    HttpResponse<String> response =
        httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      log.error(
          "Elasticsearch bulk request failed with status {}: {}",
          response.statusCode(),
          response.body());
      List<ErrorOutput> errors =
          preparedInputEvents.rawAndPreparedList().stream()
              .map(
                  p ->
                      new ErrorOutput(
                          p.getLeft(),
                          "Elasticsearch bulk error "
                              + response.statusCode()
                              + ": "
                              + response.body()))
              .toList();
      return new SimpleSinkCommand.FlushResult(0, 0, errors);
    }

    // Parse bulk response to detect per-item errors
    JsonNode bulkResponse = OBJECT_MAPPER.readTree(response.body());
    boolean hasErrors = bulkResponse.path("errors").asBoolean(false);
    if (!hasErrors) {
      log.debug("Successfully indexed {} documents to Elasticsearch", docs.size());
      return new SimpleSinkCommand.FlushResult(docs.size(), bodyBytes.length, List.of());
    }

    // Collect per-item errors
    List<ErrorOutput> errors = new ArrayList<>();
    int successCount = 0;
    JsonNode items = bulkResponse.path("items");
    var rawAndPrepared = preparedInputEvents.rawAndPreparedList();
    for (int i = 0; i < items.size(); i++) {
      JsonNode item = items.get(i);
      JsonNode indexResult = item.path("index");
      int status = indexResult.path("status").asInt(200);
      if (status >= 200 && status < 300) {
        successCount++;
      } else {
        String errorReason =
            indexResult.path("error").path("reason").asText("unknown error");
        if (i < rawAndPrepared.size()) {
          errors.add(new ErrorOutput(rawAndPrepared.get(i).getLeft(), errorReason));
        }
      }
    }

    log.debug(
        "Elasticsearch bulk: {} succeeded, {} failed", successCount, errors.size());
    return new SimpleSinkCommand.FlushResult(successCount, bodyBytes.length, errors);
  }

  @Override
  public void close() {
    // HttpClient does not require explicit closing
  }
}
