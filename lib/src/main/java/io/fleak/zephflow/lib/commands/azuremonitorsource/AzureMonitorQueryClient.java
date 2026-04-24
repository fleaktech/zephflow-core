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
package io.fleak.zephflow.lib.commands.azuremonitorsource;

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;

import com.fasterxml.jackson.databind.JsonNode;
import io.fleak.zephflow.lib.commands.azure.EntraIdTokenProvider;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AzureMonitorQueryClient {

  private static final String QUERY_ENDPOINT_TEMPLATE =
      "https://api.loganalytics.io/v1/workspaces/%s/query";

  private final String workspaceId;
  private final EntraIdTokenProvider tokenProvider;
  private final HttpClient httpClient;

  public AzureMonitorQueryClient(
      String workspaceId, EntraIdTokenProvider tokenProvider, HttpClient httpClient) {
    this.workspaceId = workspaceId;
    this.tokenProvider = tokenProvider;
    this.httpClient = httpClient;
  }

  public List<Map<String, String>> executeQuery(String kqlQuery) throws Exception {
    String token = tokenProvider.getToken();
    HttpResponse<String> response = sendRequest(kqlQuery, token);

    if (response.statusCode() == 401) {
      tokenProvider.invalidate();
      token = tokenProvider.getToken();
      response = sendRequest(kqlQuery, token);
      if (response.statusCode() == 401) {
        throw new RuntimeException("Azure Monitor auth error 401: token rejected after refresh");
      }
    }

    if (response.statusCode() != 200) {
      throw new RuntimeException(
          "Azure Monitor API returned status " + response.statusCode() + ": " + response.body());
    }

    return parseResponse(response.body());
  }

  private HttpResponse<String> sendRequest(String kqlQuery, String token) throws Exception {
    String url = String.format(QUERY_ENDPOINT_TEMPLATE, workspaceId);
    String body = OBJECT_MAPPER.writeValueAsString(Map.of("query", kqlQuery));

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Authorization", "Bearer " + token)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .timeout(Duration.ofSeconds(60))
            .build();

    return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
  }

  private List<Map<String, String>> parseResponse(String body) throws Exception {
    JsonNode root = OBJECT_MAPPER.readTree(body);
    JsonNode tables = root.path("tables");
    if (tables.isMissingNode() || !tables.isArray() || tables.isEmpty()) {
      throw new RuntimeException("Azure Monitor response missing or empty tables array: " + body);
    }

    JsonNode table = tables.get(0);
    JsonNode columns = table.path("columns");
    JsonNode rows = table.path("rows");

    List<String> columnNames = new ArrayList<>();
    for (JsonNode col : columns) {
      columnNames.add(col.path("name").asText());
    }

    List<Map<String, String>> result = new ArrayList<>();
    for (JsonNode row : rows) {
      Map<String, String> map = new LinkedHashMap<>();
      for (int i = 0; i < columnNames.size(); i++) {
        JsonNode val = row.get(i);
        map.put(columnNames.get(i), (val == null || val.isNull()) ? "" : val.asText());
      }
      result.add(map);
    }
    return result;
  }
}
