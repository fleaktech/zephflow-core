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

import static io.fleak.zephflow.lib.utils.JsonUtils.OBJECT_MAPPER;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

public class EntraIdTokenProvider {

  private static final String TOKEN_ENDPOINT_TEMPLATE =
      "https://login.microsoftonline.com/%s/oauth2/v2.0/token";
  private static final String SCOPE = "https://monitor.azure.com//.default";

  private final String tenantId;
  private final String clientId;
  private final String clientSecret;
  private final HttpClient httpClient;

  private String cachedToken;

  public EntraIdTokenProvider(
      String tenantId, String clientId, String clientSecret, HttpClient httpClient) {
    this.tenantId = tenantId;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.httpClient = httpClient;
  }

  public synchronized String getToken() throws Exception {
    if (cachedToken != null) {
      return cachedToken;
    }
    cachedToken = fetchToken();
    return cachedToken;
  }

  public synchronized void invalidate() {
    cachedToken = null;
  }

  private String fetchToken() throws Exception {
    String body =
        "grant_type=client_credentials"
            + "&client_id="
            + URLEncoder.encode(clientId, StandardCharsets.UTF_8)
            + "&client_secret="
            + URLEncoder.encode(clientSecret, StandardCharsets.UTF_8)
            + "&scope="
            + URLEncoder.encode(SCOPE, StandardCharsets.UTF_8);

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(
                URI.create(
                    String.format(
                        TOKEN_ENDPOINT_TEMPLATE,
                        URLEncoder.encode(tenantId, StandardCharsets.UTF_8))))
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .timeout(Duration.ofSeconds(30))
            .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      throw new RuntimeException(
          "Failed to obtain Entra ID token: HTTP " + response.statusCode() + " " + response.body());
    }

    JsonNode json = OBJECT_MAPPER.readTree(response.body());
    String token = json.path("access_token").asText(null);
    if (token == null || token.isEmpty()) {
      throw new RuntimeException(
          "Entra ID token response missing access_token: " + response.body());
    }
    return token;
  }
}
