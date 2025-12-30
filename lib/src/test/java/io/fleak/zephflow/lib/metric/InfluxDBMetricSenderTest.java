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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.write.Point;
import io.fleak.zephflow.api.metric.InfluxDBMetricSender;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class InfluxDBMetricSenderTest {

  @Mock private InfluxDBClient mockInfluxDBClient;
  @Mock private WriteApiBlocking mockWriteApi;

  private InfluxDBMetricSender influxDBMetricSender;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);

    when(mockInfluxDBClient.getWriteApiBlocking()).thenReturn(mockWriteApi);

    InfluxDBMetricSender.InfluxDBConfig config = new InfluxDBMetricSender.InfluxDBConfig();
    config.setUrl("http://localhost:8086");
    config.setOrg("test-org");
    config.setBucket("test-bucket");
    config.setMeasurement("test-measurement");
    config.setToken("test-token");

    influxDBMetricSender = new InfluxDBMetricSender(config, mockInfluxDBClient);
  }

  @Test
  void sendMetric_Success() {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");
    Map<String, String> additionalTags = new HashMap<>();
    additionalTags.put("tag2", "value2");

    influxDBMetricSender.sendMetric("counter", "test_metric", 1, tags, additionalTags);

    verify(mockWriteApi, times(1)).writePoint(eq("test-bucket"), eq("test-org"), any(Point.class));
  }

  @Test
  void sendMetric_WithDoubleValue() {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");
    Map<String, String> additionalTags = new HashMap<>();
    additionalTags.put("tag2", "value2");

    influxDBMetricSender.sendMetric("gauge", "test_metric", 1.5, tags, additionalTags);

    verify(mockWriteApi, times(1)).writePoint(eq("test-bucket"), eq("test-org"), any(Point.class));
  }

  @Test
  void sendMetric_WithBooleanValue() {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    influxDBMetricSender.sendMetric("status", "test_metric", true, tags, null);

    verify(mockWriteApi, times(1)).writePoint(eq("test-bucket"), eq("test-org"), any(Point.class));
  }

  @Test
  void sendMetric_WithStringValue() {
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");

    influxDBMetricSender.sendMetric("info", "test_metric", "test_value", tags, null);

    verify(mockWriteApi, times(1)).writePoint(eq("test-bucket"), eq("test-org"), any(Point.class));
  }

  @Test
  void sendMetric_WithNullTags() {
    influxDBMetricSender.sendMetric("counter", "test_metric", 1, null, null);

    verify(mockWriteApi, times(1)).writePoint(eq("test-bucket"), eq("test-org"), any(Point.class));
  }

  @Test
  void sendMetric_Exception() {
    doThrow(new RuntimeException("Test exception"))
        .when(mockWriteApi)
        .writePoint(eq("test-bucket"), eq("test-org"), any(Point.class));

    Map<String, String> tags = new HashMap<>();
    Map<String, String> additionalTags = new HashMap<>();

    assertDoesNotThrow(
        () -> {
          influxDBMetricSender.sendMetric("timer", "test_metric", 100, tags, additionalTags);
        });

    verify(mockWriteApi, times(1)).writePoint(eq("test-bucket"), eq("test-org"), any(Point.class));
  }

  @Test
  void close_ClosesInfluxDBClient() {
    influxDBMetricSender.close();

    verify(mockInfluxDBClient, times(1)).close();
  }

  @Test
  void close_WithException_DoesNotPropagate() {
    doThrow(new RuntimeException("Close exception")).when(mockInfluxDBClient).close();

    assertDoesNotThrow(
        () -> {
          influxDBMetricSender.close();
        });

    verify(mockInfluxDBClient, times(1)).close();
  }
}
