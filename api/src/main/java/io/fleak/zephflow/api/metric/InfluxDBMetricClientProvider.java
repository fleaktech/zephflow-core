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
    // TODO: Implement InfluxDB gauge functionality
    return new NoopMetricClientProvider.NoopFleakGauge<>();
  }

  @Override
  public FleakStopWatch stopWatch(String name, Map<String, String> tags) {
    return new InfluxDBStopWatch(name, tags, metricSender);
  }

  @Override
  public void close() {
    try {
      metricSender.close();
      log.info("InfluxDBMetricClientProvider closed successfully");
    } catch (Exception e) {
      log.error("Error closing InfluxDBMetricClientProvider", e);
    }
  }

  public InfluxDBMetricSender getSender() {
    return metricSender;
  }
}
