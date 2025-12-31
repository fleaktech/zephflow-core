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

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InfluxDBV2MetricSender implements AutoCloseable {

  private final InfluxDBClient influxDBClient;
  private final WriteApiBlocking writeApi;
  private final String organization;
  private final String bucket;
  private final String measurementName;

  public InfluxDBV2MetricSender(InfluxDBV2Config config, InfluxDBClient influxDBClient) {
    this.organization = config.getOrg();
    this.bucket = config.getBucket();
    this.measurementName = config.getMeasurement();
    this.influxDBClient = influxDBClient;
    this.writeApi = influxDBClient.getWriteApiBlocking();
    log.info("InfluxDB V2 Metric Sender initialized with config: {}", config);
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

      writePointToInfluxDB(type + "_" + name, value, allTags, Instant.now());

      log.debug("Sent {} metric: {} = {}", type, name, value);
    } catch (Exception e) {
      log.warn("Error sending metric to InfluxDB 2.x: {} = {}", name, value, e);
    }
  }

  private void writePointToInfluxDB(
      String fieldName, Object value, Map<String, String> tags, Instant timestamp) {
    Point point = createPoint(fieldName, value, tags, timestamp);
    writeApi.writePoint(bucket, organization, point);
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

  public void sendMetrics(Map<String, Object> metrics, Map<String, String> tags) {
    sendMetrics(metrics, tags, Instant.now());
  }

  public void sendMetrics(
      Map<String, Object> metrics, Map<String, String> tags, Instant timestamp) {
    if (metrics == null || metrics.isEmpty()) {
      log.debug("No metrics to send");
      return;
    }

    try {
      Map<String, String> allTags = new HashMap<>(tags != null ? tags : Map.of());
      addEnvironmentTags(allTags);

      for (Map.Entry<String, Object> metric : metrics.entrySet()) {
        Point point = createPoint(metric.getKey(), metric.getValue(), allTags, timestamp);
        writeApi.writePoint(bucket, organization, point);
      }

      log.debug("Sent batch of {} metrics to InfluxDB 2.x", metrics.size());
    } catch (Exception e) {
      log.warn("Error sending batch metrics to InfluxDB 2.x", e);
    }
  }

  private Point createPoint(
      String fieldName, Object value, Map<String, String> allTags, Instant timestamp) {
    Point point = Point.measurement(measurementName).time(timestamp, WritePrecision.MS);

    // Add tags
    for (Map.Entry<String, String> tag : allTags.entrySet()) {
      String tagKey = tag.getKey() != null ? tag.getKey() : "";
      String tagValue = tag.getValue() != null ? tag.getValue() : "";
      point.addTag(tagKey, tagValue);
    }

    // Add field
    if (value instanceof Number) {
      Number numValue = (Number) value;
      if (value instanceof Double || value instanceof Float) {
        point.addField(fieldName, numValue.doubleValue());
      } else {
        point.addField(fieldName, numValue.longValue());
      }
    } else if (value instanceof Boolean) {
      point.addField(fieldName, (Boolean) value);
    } else {
      point.addField(fieldName, value.toString());
    }

    return point;
  }

  @Override
  public void close() {
    try {
      if (influxDBClient != null) {
        influxDBClient.close();
        log.debug("InfluxDB 2.x client closed");
      }
    } catch (Exception e) {
      log.warn("Error closing InfluxDB 2.x client", e);
    }
  }

  @Data
  public static class InfluxDBV2Config {
    private String url;
    private String org;
    private String bucket;
    private String measurement;
    private String token;
  }
}
