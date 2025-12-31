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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

@Slf4j
public class InfluxDBMetricSender implements AutoCloseable {

  private final InfluxDB influxDB;
  private final String database;
  private final String measurementName;
  private final String retentionPolicy;

  public InfluxDBMetricSender(InfluxDBConfig config, InfluxDB influxDB) {
    this.database = config.getDatabase();
    this.measurementName = config.getMeasurement();
    this.influxDB = influxDB;
    this.retentionPolicy = config.getRetentionPolicy();
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

      writePointToInfluxDB(type + "_" + name, value, allTags);

      log.debug("Sent {} metric: {} = {}", type, name, value);
    } catch (Exception e) {
      log.warn("Error sending metric to InfluxDB: {} = {}", name, value, e);
    }
  }

  private void writePointToInfluxDB(String fieldName, Object value, Map<String, String> tags) {
    Point point = createPoint(fieldName, value, tags, System.currentTimeMillis());
    BatchPoints.Builder batchBuilder = BatchPoints.database(database);

    if (retentionPolicy != null && !retentionPolicy.isEmpty()) {
      batchBuilder.retentionPolicy(retentionPolicy);
      log.debug("Using retention policy: {}", retentionPolicy);
    }

    BatchPoints batchPoints = batchBuilder.build();
    batchPoints.point(point);
    influxDB.write(batchPoints);
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

      BatchPoints.Builder batchBuilder = BatchPoints.database(database);
      if (retentionPolicy != null && !retentionPolicy.isEmpty()) {
        batchBuilder.retentionPolicy(retentionPolicy);
        log.debug("Using retention policy: {}", retentionPolicy);
      }

      BatchPoints batchPoints = batchBuilder.build();

      for (Map.Entry<String, Object> metric : metrics.entrySet()) {
        Point point = createPoint(metric.getKey(), metric.getValue(), allTags, timestamp);
        batchPoints.point(point);
      }

      influxDB.write(batchPoints);
      log.debug(
          "Sent batch of {} metrics to InfluxDB with timestamp {}", metrics.size(), timestamp);
    } catch (Exception e) {
      log.warn("Error sending batch metrics to InfluxDB with timestamp", e);
    }
  }

  private Point createPoint(
      String fieldName, Object value, Map<String, String> allTags, long timestamp) {
    Point.Builder pointBuilder =
        Point.measurement(measurementName).time(timestamp, TimeUnit.MILLISECONDS);

    for (Map.Entry<String, String> tag : allTags.entrySet()) {
      String tagKey = tag.getKey() != null ? tag.getKey() : "";
      String tagValue = tag.getValue() != null ? tag.getValue() : "";
      pointBuilder.tag(tagKey, tagValue);
    }

    if (value instanceof Number) {
      pointBuilder.addField(fieldName, (Number) value);
    } else if (value instanceof Boolean) {
      pointBuilder.addField(fieldName, (Boolean) value);
    } else {
      pointBuilder.addField(fieldName, value.toString());
    }

    return pointBuilder.build();
  }

  @Override
  public void close() {
    try {
      if (influxDB != null) {
        influxDB.disableBatch();
        log.debug("InfluxDB batch mode disabled");

        influxDB.close();
        log.debug("InfluxDB client closed");
      }
    } catch (Exception e) {
      log.warn("Error closing InfluxDB client", e);
    }
  }

  @Data
  public static class InfluxDBConfig {
    private String url;
    private String database;
    private String measurement;
    private String username;
    private String password;
    private String retentionPolicy;
  }
}
