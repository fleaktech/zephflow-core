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
package io.fleak.zephflow.runner;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.FleakGauge;
import io.fleak.zephflow.api.metric.FleakStopWatch;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.LongAdder;

public final class RunSummaryMetricClientProvider implements MetricClientProvider {

  private final MetricClientProvider delegate;
  private final ConcurrentMap<String, LongAdder> counterTotals = new ConcurrentHashMap<>();

  public RunSummaryMetricClientProvider(MetricClientProvider delegate) {
    this.delegate = delegate;
  }

  @Override
  public FleakCounter counter(String name, Map<String, String> tags) {
    FleakCounter delegateCounter = delegate.counter(name, tags);
    LongAdder totalForName = counterTotals.computeIfAbsent(name, counterName -> new LongAdder());
    return new FleakCounter() {
      @Override
      public void increase(Map<String, String> additionalTags) {
        totalForName.increment();
        delegateCounter.increase(additionalTags);
      }

      @Override
      public void increase(long n, Map<String, String> additionalTags) {
        totalForName.add(n);
        delegateCounter.increase(n, additionalTags);
      }
    };
  }

  @Override
  public <T> FleakGauge<T> gauge(String name, Map<String, String> tags, T monitoredValue) {
    return delegate.gauge(name, tags, monitoredValue);
  }

  @Override
  public FleakStopWatch stopWatch(String name, Map<String, String> tags) {
    return delegate.stopWatch(name, tags);
  }

  @Override
  public void close() {
    delegate.close();
  }

  public RunSummary summarize() {
    Map<String, Long> totals = new HashMap<>();
    counterTotals.forEach((counterName, adder) -> totals.put(counterName, adder.sum()));
    return new RunSummary(totals);
  }
}
