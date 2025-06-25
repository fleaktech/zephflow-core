package io.fleak.zephflow.api.metric;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InfluxDBMetricClientProvider implements MetricClientProvider {

  private final InfluxDBMetricSender metricSender;

  public InfluxDBMetricClientProvider(InfluxDBMetricSender metricSender) {
    this.metricSender = metricSender;
  }

  @Override
  public FleakCounter counter(String name, Map<String, String> tags) {
    return new InfluxDBFleakCounter(name, tags, metricSender);
  }

  @Override
  public <T> FleakGauge<T> gauge(String name, Map<String, String> tags, T monitoredValue) {
    return new NoopMetricClientProvider.NoopFleakGauge<>();
  }

  @Override
  public FleakStopWatch stopWatch(String name, Map<String, String> tags) {
    return new NoopMetricClientProvider.NoopStopWatch();
  }
}
