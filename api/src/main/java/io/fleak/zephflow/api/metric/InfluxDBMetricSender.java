package io.fleak.zephflow.api.metric;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InfluxDBMetricSender {

  private static final String INFLUXDB_URL = "http://influxdb2.fleak.svc.cluster.local:8086";
  private static final String INFLUXDB_TOKEN =
      "wiIOZ1C-CS3jmTDynQ-6G_J-EaqCec21kgabQjLvp_x1Vo0pjoUo2xkVA6NCY7hdv26RJ26h9TNQodlnQ9dMqA==";
  private static final String INFLUXDB_ORG = "377782f14a2b645d";
  private static final String INFLUXDB_BUCKET = "data_critical";
  private static final String MEASUREMENT_NAME = "fleak_mapping_runtime";

  private final HttpClient httpClient;
  private final String writeUrl;

  public InfluxDBMetricSender() {
    this.httpClient = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    this.writeUrl =
        INFLUXDB_URL + "/api/v2/write?org=" + INFLUXDB_ORG + "&bucket=" + INFLUXDB_BUCKET;

    log.info(
        "InfluxDB Metric Sender initialized - URL: {}, Org: {}, Bucket: {}",
        INFLUXDB_URL,
        INFLUXDB_ORG,
        INFLUXDB_BUCKET);
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
            .header("Authorization", "Token " + INFLUXDB_TOKEN)
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
    sb.append(MEASUREMENT_NAME);

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
}
