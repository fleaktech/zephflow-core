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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class InfluxDBV2MetricClientProvider implements MetricClientProvider {

  private final InfluxDBV2MetricSender metricSender;
  private final List<InfluxDBV2FleakCounter> counters = new ArrayList<>();

  public InfluxDBV2MetricClientProvider(InfluxDBV2MetricSender metricSender) {
    this.metricSender = metricSender;
  }

  @Override
  public synchronized FleakCounter counter(String name, Map<String, String> tags) {
    InfluxDBV2FleakCounter counter = new InfluxDBV2FleakCounter(name, tags, metricSender);
    counters.add(counter);
    return counter;
  }

  @Override
  public <T> FleakGauge<T> gauge(String name, Map<String, String> tags, T monitoredValue) {
    return new NoopMetricClientProvider.NoopFleakGauge<>();
  }

  @Override
  public FleakStopWatch stopWatch(String name, Map<String, String> tags) {
    return new InfluxDBV2StopWatch(name, tags, metricSender);
  }

  @Override
  public void close() {
    try {
      for (InfluxDBV2FleakCounter counter : counters) {
        try {
          counter.flush();
        } catch (Exception e) {
          log.error("Error flushing counter", e);
        }
      }
      metricSender.close();
      log.info("InfluxDBV2MetricClientProvider closed successfully");
    } catch (Exception e) {
      log.error("Error closing InfluxDBV2MetricClientProvider", e);
    }
  }

  public InfluxDBV2MetricSender getSender() {
    return metricSender;
  }
}
