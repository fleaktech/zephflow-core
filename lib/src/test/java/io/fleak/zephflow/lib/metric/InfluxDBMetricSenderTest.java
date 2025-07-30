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
import static org.mockito.Mockito.*;

import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import java.util.HashMap;
import java.util.Map;

import io.fleak.zephflow.api.metric.InfluxDBMetricSender;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

class InfluxDBMetricSenderTest {

    @Mock
    private InfluxDB mockInfluxDB;

    private InfluxDBMetricSender influxDBMetricSender;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);

        InfluxDBMetricSender.InfluxDBConfig config = new InfluxDBMetricSender.InfluxDBConfig();
        config.setUrl("http://localhost:8086");
        config.setDatabase("test-database");
        config.setMeasurement("test-measurement");

        influxDBMetricSender = new InfluxDBMetricSender(config, mockInfluxDB);
    }

    @Test
    void sendMetric_Success() {
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "value1");
        Map<String, String> additionalTags = new HashMap<>();
        additionalTags.put("tag2", "value2");

        influxDBMetricSender.sendMetric("counter", "test_metric", 1, tags, additionalTags);

        verify(mockInfluxDB, times(1)).write(any(BatchPoints.class));
    }

    @Test
    void sendMetric_WithDoubleValue() {
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "value1");
        Map<String, String> additionalTags = new HashMap<>();
        additionalTags.put("tag2", "value2");

        influxDBMetricSender.sendMetric("gauge", "test_metric", 1.5, tags, additionalTags);

        verify(mockInfluxDB, times(1)).write(any(BatchPoints.class));
    }

    @Test
    void sendMetric_WithBooleanValue() {
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "value1");

        influxDBMetricSender.sendMetric("status", "test_metric", true, tags, null);

        verify(mockInfluxDB, times(1)).write(any(BatchPoints.class));
    }

    @Test
    void sendMetric_WithStringValue() {
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "value1");

        influxDBMetricSender.sendMetric("info", "test_metric", "test_value", tags, null);

        verify(mockInfluxDB, times(1)).write(any(BatchPoints.class));
    }

    @Test
    void sendMetric_WithNullTags() {
        influxDBMetricSender.sendMetric("counter", "test_metric", 1, null, null);

        verify(mockInfluxDB, times(1)).write(any(BatchPoints.class));
    }

    @Test
    void sendMetric_Exception() {
        doThrow(new RuntimeException("Test exception")).when(mockInfluxDB).write(any(BatchPoints.class));

        Map<String, String> tags = new HashMap<>();
        Map<String, String> additionalTags = new HashMap<>();

        assertDoesNotThrow(() -> {
            influxDBMetricSender.sendMetric("timer", "test_metric", 100, tags, additionalTags);
        });

        verify(mockInfluxDB, times(1)).write(any(BatchPoints.class));
    }

    @Test
    void close_ClosesInfluxDBClient() {
        influxDBMetricSender.close();

        verify(mockInfluxDB, times(1)).close();
    }

    @Test
    void close_WithException_DoesNotPropagate() {
        doThrow(new RuntimeException("Close exception")).when(mockInfluxDB).close();

        assertDoesNotThrow(() -> {
            influxDBMetricSender.close();
        });

        verify(mockInfluxDB, times(1)).close();
    }
}