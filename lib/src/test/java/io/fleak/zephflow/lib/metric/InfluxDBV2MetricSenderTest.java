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
package io.fleak.zephflow.lib.metric;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApi;
import com.influxdb.client.WriteOptions;
import com.influxdb.client.write.Point;
import io.fleak.zephflow.api.metric.InfluxDBV2MetricSender;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class InfluxDBV2MetricSenderTest {

  @Mock private InfluxDBClient mockInfluxDBClient;

  @Mock private WriteApi mockWriteApi;

  private InfluxDBV2MetricSender influxDBV2MetricSender;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);

    when(mockInfluxDBClient.makeWriteApi(any(WriteOptions.class))).thenReturn(mockWriteApi);

    InfluxDBV2MetricSender.InfluxDBV2Config config = new InfluxDBV2MetricSender.InfluxDBV2Config();
    config.setUrl("http://localhost:8086");
    config.setOrg("test-org");
    config.setBucket("test-bucket");
    config.setMeasurement("test-measurement");
    config.setToken("test-token");

    influxDBV2MetricSender = new InfluxDBV2MetricSender(config, mockInfluxDBClient);
  }

  @Test
  void sendMetricSuccess() {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");
    Map<String, String> additionalTags = new HashMap<>();
    additionalTags.put("tag2", "value2");

    influxDBV2MetricSender.sendMetric("counter", "test_metric", 1, tags, additionalTags);

    verify(mockWriteApi, times(1)).writePoint(anyString(), anyString(), any(Point.class));
  }

  @Test
  void sendMetricWithDoubleValue() {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");
    Map<String, String> additionalTags = new HashMap<>();
    additionalTags.put("tag2", "value2");

    influxDBV2MetricSender.sendMetric("gauge", "test_metric", 1.5, tags, additionalTags);

    verify(mockWriteApi, times(1)).writePoint(anyString(), anyString(), any(Point.class));
  }

  @Test
  void sendMetricWithFloatValue() {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");
    Map<String, String> additionalTags = new HashMap<>();
    additionalTags.put("tag2", "value2");

    influxDBV2MetricSender.sendMetric("gauge", "test_metric", 2.5f, tags, additionalTags);

    verify(mockWriteApi, times(1)).writePoint(anyString(), anyString(), any(Point.class));
  }

  @Test
  void sendMetricWithLongValue() {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    influxDBV2MetricSender.sendMetric("counter", "test_metric", 100L, tags, null);

    verify(mockWriteApi, times(1)).writePoint(anyString(), anyString(), any(Point.class));
  }

  @Test
  void sendMetricWithBooleanValue() {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    influxDBV2MetricSender.sendMetric("status", "test_metric", true, tags, null);

    verify(mockWriteApi, times(1)).writePoint(anyString(), anyString(), any(Point.class));
  }

  @Test
  void sendMetricWithStringValue() {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    influxDBV2MetricSender.sendMetric("info", "test_metric", "test_value", tags, null);

    verify(mockWriteApi, times(1)).writePoint(anyString(), anyString(), any(Point.class));
  }

  @Test
  void sendMetricWithNullTags() {
    influxDBV2MetricSender.sendMetric("counter", "test_metric", 1, null, null);

    verify(mockWriteApi, times(1)).writePoint(anyString(), anyString(), any(Point.class));
  }

  @Test
  void sendMetric_Exception() {
    doThrow(new RuntimeException("Test exception"))
        .when(mockWriteApi)
        .writePoint(anyString(), anyString(), any(Point.class));

    Map<String, String> tags = new HashMap<>();
    Map<String, String> additionalTags = new HashMap<>();

    assertDoesNotThrow(
        () -> {
          influxDBV2MetricSender.sendMetric("timer", "test_metric", 100, tags, additionalTags);
        });

    verify(mockWriteApi, times(1)).writePoint(anyString(), anyString(), any(Point.class));
  }

  @Test
  void sendMetricsSuccess() {
    Map<String, Object> metrics = new HashMap<>();
    metrics.put("metric1", 10);
    metrics.put("metric2", 20.5);
    metrics.put("metric3", true);

    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    influxDBV2MetricSender.sendMetrics(metrics, tags);

    verify(mockWriteApi, times(1)).writePoints(anyString(), anyString(), any(List.class));
  }

  @Test
  void sendMetricsWithEmptyMetrics() {
    Map<String, Object> metrics = new HashMap<>();
    Map<String, String> tags = new HashMap<>();

    influxDBV2MetricSender.sendMetrics(metrics, tags);

    verify(mockWriteApi, never()).writePoints(anyString(), anyString(), any(List.class));
  }

  @Test
  void sendMetricsWithNullMetrics() {
    Map<String, String> tags = new HashMap<>();

    influxDBV2MetricSender.sendMetrics(null, tags);

    verify(mockWriteApi, never()).writePoints(anyString(), anyString(), any(List.class));
  }

  @Test
  void sendMetricsException() {
    doThrow(new RuntimeException("Test exception"))
        .when(mockWriteApi)
        .writePoints(anyString(), anyString(), any(List.class));

    Map<String, Object> metrics = new HashMap<>();
    metrics.put("metric1", 10);

    Map<String, String> tags = new HashMap<>();

    assertDoesNotThrow(
        () -> {
          influxDBV2MetricSender.sendMetrics(metrics, tags);
        });

    verify(mockWriteApi, times(1)).writePoints(anyString(), anyString(), any(List.class));
  }

  @Test
  void closeClosesWriteApiAndInfluxDBClient() {
    influxDBV2MetricSender.close();

    verify(mockWriteApi, times(1)).flush();
    verify(mockWriteApi, times(1)).close();
    verify(mockInfluxDBClient, times(1)).close();
  }

  @Test
  void closeWithException_DoesNotPropagate() {
    doThrow(new RuntimeException("Close exception")).when(mockWriteApi).flush();

    assertDoesNotThrow(
        () -> {
          influxDBV2MetricSender.close();
        });

    verify(mockWriteApi, times(1)).flush();
  }
}
