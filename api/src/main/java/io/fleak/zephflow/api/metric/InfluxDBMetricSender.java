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
public class InfluxDBMetricSender implements AutoCloseable {

  private final InfluxDBClient influxDBClient;
  private final WriteApiBlocking writeApi;
  private final String measurementName;

  //  public InfluxDBMetricSender(InfluxDBConfig config) {
  //    this(config, null);
  //  }

  public InfluxDBMetricSender(InfluxDBConfig config, InfluxDBClient influxDBClient) {
    this.influxDBClient = influxDBClient;
    if (config.getToken() == null || config.getToken().isEmpty()) {
      throw new IllegalStateException("InfluxDB token is required. Use --influxdb-token parameter");
    }
    this.measurementName = config.getMeasurement();

    // Initialize InfluxDB client
    this.writeApi = influxDBClient.getWriteApiBlocking();

    log.info("InfluxDB Metric Sender initialized with config: {}", config);

    // Test connection
    try {
      influxDBClient.ping();
      log.info("InfluxDB connection test successful");
    } catch (Exception e) {
      log.warn("InfluxDB connection test failed: {}", e.getMessage());
    }
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

      Point point = buildPoint(type + "_" + name, value, allTags);
      writeApi.writePoint(point);

      log.debug("Sent {} metric: {} = {}", type, name, value);
    } catch (Exception e) {
      log.warn("Error sending metric to InfluxDB: {} = {}", name, value, e);
    }
  }

  private Point buildPoint(String fieldName, Object value, Map<String, String> tags) {
    Point point = Point.measurement(measurementName).time(Instant.now(), WritePrecision.MS);

    // Add tags
    for (Map.Entry<String, String> tag : tags.entrySet()) {
      point.addTag(tag.getKey(), tag.getValue());
    }

    // Add field - simplified version
    if (value instanceof Number) {
      point.addField(fieldName, (Number) value);
    } else if (value instanceof Boolean) {
      point.addField(fieldName, (Boolean) value);
    } else {
      point.addField(fieldName, value.toString());
    }

    return point;
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
    try {
      if (influxDBClient != null) {
        influxDBClient.close();
        log.debug("InfluxDB client closed");
      }
    } catch (Exception e) {
      log.warn("Error closing InfluxDB client", e);
    }
  }

  @Data
  public static class InfluxDBConfig {
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
          + ", org='"
          + org
          + '\''
          + ", bucket='"
          + bucket
          + '\''
          + ", measurement='"
          + measurement
          + '\''
          + ", token='"
          + (token != null && !token.isEmpty() ? "[REDACTED]" : "null")
          + '\''
          + '}';
    }
  }
}
