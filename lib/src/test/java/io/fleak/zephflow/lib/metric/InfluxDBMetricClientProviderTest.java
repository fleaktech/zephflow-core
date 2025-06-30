package io.fleak.zephflow.lib.metric;

import io.fleak.zephflow.api.metric.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class InfluxDBMetricClientProviderTest {

    @Mock
    private InfluxDBMetricSender mockInfluxDBMetricSender;

    private InfluxDBMetricClientProvider influxDBMetricClientProvider;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        influxDBMetricClientProvider = new InfluxDBMetricClientProvider(mockInfluxDBMetricSender);
    }

    @Test
    void counter_ReturnsInfluxDBFleakCounter() {
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "value1");

        FleakCounter counter = influxDBMetricClientProvider.counter("test_counter", tags);

        assertNotNull(counter);
        assertInstanceOf(InfluxDBFleakCounter.class, counter);
    }

    @Test
    void gauge_ReturnsNoopFleakGauge() {
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "value1");

        FleakGauge<Long> gauge = influxDBMetricClientProvider.gauge("test_gauge", tags, 0L);

        assertNotNull(gauge);
        // Since gauge is not implemented yet, it returns NoopFleakGauge
        assertInstanceOf(MetricClientProvider.NoopMetricClientProvider.NoopFleakGauge.class, gauge);
    }

    @Test
    void stopWatch_ReturnsNoopStopWatch() {
        Map<String, String> tags = new HashMap<>();
        tags.put("tag1", "value1");

        FleakStopWatch stopWatch = influxDBMetricClientProvider.stopWatch("test_stopwatch", tags);

        assertNotNull(stopWatch);
        // Since stopWatch is not implemented yet, it returns NoopStopWatch
        assertInstanceOf(MetricClientProvider.NoopMetricClientProvider.NoopStopWatch.class, stopWatch);
    }

    @Test
    void counter_WithNullTags_ReturnsInfluxDBFleakCounter() {
        FleakCounter counter = influxDBMetricClientProvider.counter("test_counter", null);

        assertNotNull(counter);
        assertInstanceOf(InfluxDBFleakCounter.class, counter);
    }

    @Test
    void counter_WithEmptyTags_ReturnsInfluxDBFleakCounter() {
        Map<String, String> emptyTags = new HashMap<>();

        FleakCounter counter = influxDBMetricClientProvider.counter("test_counter", emptyTags);

        assertNotNull(counter);
        assertInstanceOf(InfluxDBFleakCounter.class, counter);
    }
}
