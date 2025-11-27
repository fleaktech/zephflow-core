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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SplunkMetricSender implements AutoCloseable {

  private static final int BATCH_SIZE = 100;

  private final HttpClient httpClient;
  private final String hecUrl;
  private final String token;
  private final String source;
  private final String index;
  private final ObjectMapper objectMapper;
  private final List<Map<String, Object>> metricBatch;
  private final Object batchLock = new Object();

  public SplunkMetricSender(SplunkConfig config) {
    this.hecUrl = config.getHecUrl();
    this.token = config.getToken();
    this.source = config.getSource() != null ? config.getSource() : "zephflow";
    this.index = config.getIndex();
    this.httpClient =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10))
            .version(HttpClient.Version.HTTP_1_1)
            .build();
    this.objectMapper = configureObjectMapper();
    this.metricBatch = new ArrayList<>(BATCH_SIZE);
    log.info("Splunk Metric Sender initialized with HEC URL: {}", hecUrl);
  }

  public SplunkMetricSender(SplunkConfig config, HttpClient httpClient) {
    this.hecUrl = config.getHecUrl();
    this.token = config.getToken();
    this.source = config.getSource() != null ? config.getSource() : "zephflow";
    this.index = config.getIndex();
    this.httpClient = httpClient;
    this.objectMapper = configureObjectMapper();
    this.metricBatch = new ArrayList<>(BATCH_SIZE);
    log.info("Splunk Metric Sender initialized with HEC URL: {}", hecUrl);
  }

  private ObjectMapper configureObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setLocale(Locale.US);
    mapper.configure(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN, true);
    return mapper;
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

      String metricName = type + "_" + name;
      Map<String, Object> event = buildMetricEvent(metricName, value, allTags);

      synchronized (batchLock) {
        metricBatch.add(event);
        log.debug(
            "Added metric to batch: {} = {} (batch size: {})",
            metricName,
            value,
            metricBatch.size());

        if (metricBatch.size() >= BATCH_SIZE) {
          flushBatch();
        }
      }
    } catch (Exception e) {
      log.warn("Error adding metric to batch: {} = {}", name, value, e);
    }
  }

  public void sendMetrics(Map<String, Object> metrics, Map<String, String> tags) {
    sendMetrics(metrics, tags, System.currentTimeMillis());
  }

  public void sendMetrics(Map<String, Object> metrics, Map<String, String> tags, long timestamp) {
    if (metrics == null || metrics.isEmpty()) {
      log.debug("No metrics to send");
      return;
    }

    try {
      Map<String, String> allTags = new HashMap<>(tags != null ? tags : Map.of());
      addEnvironmentTags(allTags);

      StringBuilder batchPayload = new StringBuilder();
      for (Map.Entry<String, Object> metric : metrics.entrySet()) {
        Map<String, Object> event = buildMetricEvent(metric.getKey(), metric.getValue(), allTags);
        event.put("time", BigDecimal.valueOf(timestamp / 1000.0));
        batchPayload.append(toJson(event)).append("\n");
      }

      String payload = batchPayload.toString();
      sendBatch(payload);
    } catch (Exception e) {
      log.warn("Error sending batch metrics", e);
    }
  }

  public void flush() {
    synchronized (batchLock) {
      flushBatch();
    }
  }

  private void flushBatch() {
    if (metricBatch.isEmpty()) {
      return;
    }

    try {
      StringBuilder batchPayload = new StringBuilder();
      for (Map<String, Object> event : metricBatch) {
        batchPayload.append(toJson(event)).append("\n");
      }

      String payload = batchPayload.toString();
      sendBatch(payload);

      log.debug("Flushed batch of {} metrics", metricBatch.size());
      metricBatch.clear();
    } catch (Exception e) {
      log.warn("Error flushing metric batch of {} metrics", metricBatch.size(), e);
      metricBatch.clear();
    }
  }

  private Map<String, Object> buildMetricEvent(
      String metricName, Object value, Map<String, String> dimensions) {
    Map<String, Object> event = new HashMap<>();

    event.put("index", index);
    event.put("source", source);
    event.put("time", BigDecimal.valueOf(System.currentTimeMillis() / 1000.0));
    event.put("event", "metric");

    Map<String, Object> fields = new HashMap<>();
    fields.put("metric_name:" + metricName, value);

    if (dimensions != null && !dimensions.isEmpty()) {
      fields.putAll(dimensions);
    }

    event.put("fields", fields);
    return event;
  }

  private void sendEvent(Map<String, Object> event) throws IOException, InterruptedException {
    String jsonPayload = toJson(event);
    sendBatch(jsonPayload);
  }

  private void sendBatch(String jsonPayload) throws IOException, InterruptedException {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(hecUrl))
            .header("Authorization", "Splunk " + token)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
            .timeout(Duration.ofSeconds(30))
            .build();

    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 200) {
      log.warn(
          "Splunk HEC returned non-200 status: {} - {}", response.statusCode(), response.body());
    } else {
      log.debug("Splunk HEC response: {}", response.body());
    }
  }

  private String toJson(Object obj) throws IOException {
    return objectMapper.writeValueAsString(obj);
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

    if (podName != null && !podName.isEmpty()) {
      tags.put("pod", podName);
    }
    if (namespace != null && !namespace.isEmpty()) {
      tags.put("namespace", namespace);
    }
  }

  @Override
  public void close() {
    synchronized (batchLock) {
      if (!metricBatch.isEmpty()) {
        log.info("Flushing remaining {} metrics before closing", metricBatch.size());
        flushBatch();
      }
    }
    log.debug("Splunk Metric Sender closed");
  }

  @Data
  public static class SplunkConfig {
    private String hecUrl;
    private String token;
    private String source;
    private String index;
  }
}
