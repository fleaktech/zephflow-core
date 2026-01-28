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
import com.influxdb.client.WriteApi;
import com.influxdb.client.WriteOptions;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.reactivex.rxjava3.core.BackpressureOverflowStrategy;
import java.time.Instant;
import java.util.*;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InfluxDBV2MetricSender implements AutoCloseable {

  private final InfluxDBClient influxDBClient;
  private final WriteApi writeApi;
  private final String organization;
  private final String bucket;
  private final String measurementName;
  private final Map<String, String> cachedEnvironmentTags;

  public InfluxDBV2MetricSender(InfluxDBV2Config config, InfluxDBClient influxDBClient) {
    this.organization = config.getOrg();
    this.bucket = config.getBucket();
    this.measurementName = config.getMeasurement();
    this.influxDBClient = influxDBClient;

    this.cachedEnvironmentTags = initializeEnvironmentTags();

    WriteOptions writeOptions =
        WriteOptions.builder()
            .batchSize(config.getBatchSize())
            .flushInterval(config.getFlushInterval())
            .bufferLimit(config.getBufferLimit())
            .retryInterval(config.getRetryInterval())
            .maxRetries(config.getMaxRetries())
            .backpressureStrategy(BackpressureOverflowStrategy.DROP_OLDEST)
            .build();

    this.writeApi = influxDBClient.makeWriteApi(writeOptions);

    log.info(
        "InfluxDB V2 Metric Sender initialized with config: {} (batchSize={}, flushInterval={}ms, bufferLimit={}, gzip=enabled)",
        config,
        config.getBatchSize(),
        config.getFlushInterval(),
        config.getBufferLimit());
  }

  private Map<String, String> initializeEnvironmentTags() {
    Map<String, String> envTags = new HashMap<>();
    String podName = System.getenv("HOSTNAME");
    String namespace = System.getenv("POD_NAMESPACE");

    if (podName != null && !podName.isEmpty()) {
      envTags.put("pod", podName);
    }
    if (namespace != null && !namespace.isEmpty()) {
      envTags.put("namespace", namespace);
    }

    return Collections.unmodifiableMap(envTags);
  }

  public void sendMetric(
      String type,
      String name,
      Object value,
      Map<String, String> tags,
      Map<String, String> additionalTags) {
    try {
      Map<String, String> allTags = mergeAllTags(tags, additionalTags);
      Point point = createPoint(type + "_" + name, value, allTags, Instant.now());

      writeApi.writePoint(bucket, organization, point);

      log.debug("Queued {} metric: {} = {}", type, name, value);
    } catch (Exception e) {
      log.warn("Error sending metric to InfluxDB 2.x: {} = {}", name, value, e);
    }
  }

  private Map<String, String> mergeAllTags(
      Map<String, String> tags, Map<String, String> additionalTags) {
    int expectedSize =
        cachedEnvironmentTags.size()
            + (tags != null ? tags.size() : 0)
            + (additionalTags != null ? additionalTags.size() : 0);

    Map<String, String> merged = new HashMap<>(expectedSize);

    merged.putAll(cachedEnvironmentTags);

    if (tags != null) {
      merged.putAll(tags);
    }
    if (additionalTags != null) {
      merged.putAll(additionalTags);
    }

    return merged;
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
      Map<String, String> allTags = mergeAllTags(tags, null);

      List<Point> points = new ArrayList<>(metrics.size());
      for (Map.Entry<String, Object> metric : metrics.entrySet()) {
        Point point = createPoint(metric.getKey(), metric.getValue(), allTags, timestamp);
        points.add(point);
      }

      writeApi.writePoints(bucket, organization, points);

      log.debug("Queued batch of {} metrics to InfluxDB 2.x", metrics.size());
    } catch (Exception e) {
      log.warn("Error queuing batch metrics to InfluxDB 2.x", e);
    }
  }

  private Point createPoint(
      String fieldName, Object value, Map<String, String> allTags, Instant timestamp) {
    Point point = Point.measurement(measurementName).time(timestamp, WritePrecision.MS);

    for (Map.Entry<String, String> tag : allTags.entrySet()) {
      String tagKey = tag.getKey() != null ? tag.getKey() : "";
      String tagValue = tag.getValue() != null ? tag.getValue() : "";
      point.addTag(tagKey, tagValue);
    }

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
      if (writeApi != null) {
        writeApi.flush();
        writeApi.close();
        log.debug("InfluxDB 2.x WriteApi flushed and closed");
      }
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

    private int batchSize = 10000;
    private int flushInterval = 1000;
    private int bufferLimit = 50000;
    private int retryInterval = 5000;
    private int maxRetries = 3;
  }
}
