package io.fleak.zephflow.api.metric;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InfluxDBFleakCounter implements FleakCounter {

  private final String name;
  private final Map<String, String> tags;
  private final InfluxDBMetricSender metricSender;
  private Long counter;

  public InfluxDBFleakCounter(
      String name, Map<String, String> tags, InfluxDBMetricSender metricSender) {
    this.name = name;
    this.tags = tags;
    this.metricSender = metricSender;
    this.counter = 0L;
  }

  @Override
  public void increase(Map<String, String> additionalTags) {
    increase(1L, additionalTags);
  }

  @Override
  public void increase(long n, Map<String, String> additionalTags) {
    counter += n;
    metricSender.sendMetric("counter", name, counter, tags, additionalTags);
  }
}
