package io.fleak.zephflow.lib.metric;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.write.Point;
import java.util.HashMap;
import java.util.Map;

import io.fleak.zephflow.api.metric.InfluxDBMetricSender;
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

        InfluxDBMetricSender.InfluxDBConfig config = new InfluxDBMetricSender.InfluxDBConfig();
        config.setUrl("http://localhost:8086");
        config.setToken("test-token");
        config.setOrg("test-org");
        config.setBucket("test-bucket");
        config.setMeasurement("test-measurement");

        when(mockInfluxDBClient.getWriteApiBlocking()).thenReturn(mockWriteApi);

        influxDBMetricSender = new InfluxDBMetricSender(config, mockInfluxDBClient);
    }

    @Test
    void sendMetric_Success() {
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "value1");
        Map<String, String> additionalTags = new HashMap<>();
        additionalTags.put("tag2", "value2");

        influxDBMetricSender.sendMetric("counter", "test_metric", 1, tags, additionalTags);

        verify(mockWriteApi, times(1)).writePoint(any(Point.class));
    }

    @Test
    void sendMetric_WithDoubleValue() {
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "value1");
        Map<String, String> additionalTags = new HashMap<>();
        additionalTags.put("tag2", "value2");

        influxDBMetricSender.sendMetric("gauge", "test_metric", 1.5, tags, additionalTags);

        verify(mockWriteApi, times(1)).writePoint(any(Point.class));
    }

    @Test
    void sendMetric_Exception() {
        doThrow(new RuntimeException("Test exception")).when(mockWriteApi).writePoint(any(Point.class));

        Map<String, String> tags = new HashMap<>();
        Map<String, String> additionalTags = new HashMap<>();

        influxDBMetricSender.sendMetric("timer", "test_metric", 100, tags, additionalTags);

        verify(mockWriteApi, times(1)).writePoint(any(Point.class));
    }

    @Test
    void constructor_WithEmptyToken_ThrowsException() {
        InfluxDBMetricSender.InfluxDBConfig config = new InfluxDBMetricSender.InfluxDBConfig();
        config.setUrl("http://localhost:8086");
        config.setToken("");
        config.setOrg("test-org");
        config.setBucket("test-bucket");
        config.setMeasurement("test-measurement");

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            new InfluxDBMetricSender(config, mockInfluxDBClient);
        });

        assertEquals("InfluxDB token is required. Use --influxdb-token parameter", exception.getMessage());
    }

    @Test
    void constructor_WithNullToken_ThrowsException() {
        InfluxDBMetricSender.InfluxDBConfig config = new InfluxDBMetricSender.InfluxDBConfig();
        config.setUrl("http://localhost:8086");
        config.setToken(null);
        config.setOrg("test-org");
        config.setBucket("test-bucket");
        config.setMeasurement("test-measurement");

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
            new InfluxDBMetricSender(config, mockInfluxDBClient);
        });

        assertEquals("InfluxDB token is required. Use --influxdb-token parameter", exception.getMessage());
    }
}