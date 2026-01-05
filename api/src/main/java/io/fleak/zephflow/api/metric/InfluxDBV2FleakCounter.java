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
public class InfluxDBV2FleakCounter implements FleakCounter {

  private final String name;
  private final Map<String, String> tags;
  private final InfluxDBV2MetricSender metricSender;

  public InfluxDBV2FleakCounter(
      String name, Map<String, String> tags, InfluxDBV2MetricSender metricSender) {
    this.name = name;
    this.tags = tags;
    this.metricSender = metricSender;
  }

  @Override
  public void increase(Map<String, String> additionalTags) {
    increase(1L, additionalTags);
  }

  @Override
  public void increase(long n, Map<String, String> additionalTags) {
    metricSender.sendMetric("counter", name, n, tags, additionalTags);
  }
}
