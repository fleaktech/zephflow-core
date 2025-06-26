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
package io.fleak.zephflow.api.metric;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InfluxDBMetricSender {

  private final String influxdbToken;
  private final String measurementName;

  private final HttpClient httpClient;
  private final String writeUrl;

  public InfluxDBMetricSender(InfluxDBConfig config) {
    String influxdbUrl = config.getUrl();
    this.influxdbToken = config.getToken();
    String influxdbOrg = config.getOrg();
    String influxdbBucket = config.getBucket();
    this.measurementName = config.getMeasurement();

    if (influxdbToken.isEmpty()) {
      throw new IllegalStateException("InfluxDB token is required. Use --influxdb-token parameter");
    }

    this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    this.writeUrl = influxdbUrl + "/api/v2/write?org=" + influxdbOrg + "&bucket=" + influxdbBucket;

    log.info("InfluxDB Metric Sender initialized with config: {}", config);
  }

  public void sendMetric(
      String type,
      String name,
      Object value,
      Map<String, String> tags,
      Map<String, String> additionalTags) {
    try {
      Map<String, String> allTags = mergeTags(tags, additionalTags);
      addEnvironmentTags(allTags);
      String lineProtocol = buildLineProtocol(allTags, type + "_" + name, value);
      sendToInfluxDB(lineProtocol);
      log.debug("Sent {} metric: {} = {}", type, name, value);
    } catch (Exception e) {
      log.warn("Error sending metric to InfluxDB: {} = {}", name, value, e);
    }
  }

  private void sendToInfluxDB(String lineProtocol) throws IOException, InterruptedException {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(writeUrl))
            .header("Authorization", "Token " + influxdbToken)
            .header("Content-Type", "text/plain; charset=utf-8")
            .POST(HttpRequest.BodyPublishers.ofString(lineProtocol))
            .timeout(Duration.ofSeconds(30))
            .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 204) {
      log.warn("Unexpected InfluxDB response: {} - {}", response.statusCode(), response.body());
    }
  }

  private String buildLineProtocol(Map<String, String> tags, String fieldName, Object value) {
    StringBuilder sb = new StringBuilder();

    // Measurement
    sb.append(measurementName);

    for (Map.Entry<String, String> tag : tags.entrySet()) {
      sb.append(",")
          .append(escapeValue(tag.getKey()))
          .append("=")
          .append(escapeValue(tag.getValue()));
    }

    sb.append(" ");

    // Field
    sb.append(escapeValue(fieldName)).append("=");
    if (value instanceof Number) {
      sb.append(value).append("i");
    } else {
      sb.append("\"").append(value).append("\"");
    }

    return sb.toString();
  }

  private String escapeValue(String value) {
    if (value == null) return "";
    return value.replace(",", "\\,").replace(" ", "\\ ").replace("=", "\\=");
  }

  private Map<String, String> mergeTags(
      Map<String, String> tags, Map<String, String> additionalTags) {
    Map<String, String> merged = new HashMap<>(tags != null ? tags : Map.of());
    if (additionalTags != null) {
      merged.putAll(additionalTags);
    }
    return merged;
  }

  private void addEnvironmentTags(Map<String, String> tags) {
    String podName = System.getenv("HOSTNAME");
    String namespace = System.getenv("POD_NAMESPACE");

    if (podName != null) {
      tags.put("pod", podName);
    }
    if (namespace != null) {
      tags.put("namespace", namespace);
    }
  }

  @Setter
  @Getter
  public static class InfluxDBConfig {
    // Getters and Setters
    private String url;
    private String token;
    private String org;
    private String bucket;
    private String measurement;

    @Override
    public String toString() {
      return "InfluxDBConfig{"
          + "url='"
          + url
          + '\''
          + ", token='"
          + token
          + '\''
          + ", org='"
          + org
          + '\''
          + ", bucket='"
          + bucket
          + '\''
          + ", measurement='"
          + measurement
          + '\''
          + '}';
    }
  }
}
