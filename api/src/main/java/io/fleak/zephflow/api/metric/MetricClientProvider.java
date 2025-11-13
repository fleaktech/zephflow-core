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

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

/** Created by bolei on 4/8/24 */
public interface MetricClientProvider extends AutoCloseable {

  FleakCounter counter(String name, Map<String, String> tags);

  <T> FleakGauge<T> gauge(String name, Map<String, String> tags, T monitoredValue);

  FleakStopWatch stopWatch(String name, Map<String, String> tags);

  @Override
  void close();

  @Slf4j
  class CompositeMetricClientProvider implements MetricClientProvider {
    private final List<MetricClientProvider> providers;

    public CompositeMetricClientProvider(List<MetricClientProvider> providers) {
      this.providers = providers;
    }

    @Override
    public FleakCounter counter(String name, Map<String, String> tags) {
      return new CompositeFleakCounter(name, tags);
    }

    @Override
    public <T> FleakGauge<T> gauge(String name, Map<String, String> tags, T monitoredValue) {
      return new CompositeFleakGauge<>(name, tags);
    }

    @Override
    public FleakStopWatch stopWatch(String name, Map<String, String> tags) {
      return new CompositeFleakStopWatch(name, tags);
    }

    @Override
    public void close() {
      for (MetricClientProvider provider : providers) {
        try {
          provider.close();
        } catch (Exception e) {
          log.error("Failed to closed provider = {}", provider, e);
        }
      }
    }

    private class CompositeFleakCounter implements FleakCounter {
      private final String name;
      private final Map<String, String> tags;

      CompositeFleakCounter(String name, Map<String, String> tags) {
        this.name = name;
        this.tags = tags;
      }

      @Override
      public void increase(Map<String, String> additionalTags) {
        for (MetricClientProvider provider : providers) {
          provider.counter(name, tags).increase(additionalTags);
        }
      }

      @Override
      public void increase(long n, Map<String, String> additionalTags) {
        for (MetricClientProvider provider : providers) {
          provider.counter(name, tags).increase(n, additionalTags);
        }
      }
    }

    private class CompositeFleakGauge<T> implements FleakGauge<T> {
      private final String name;
      private final Map<String, String> tags;

      CompositeFleakGauge(String name, Map<String, String> tags) {
        this.name = name;
        this.tags = tags;
      }

      @Override
      public void setValue(T value, Map<String, String> additionalTags) {
        for (MetricClientProvider provider : providers) {
          provider.gauge(name, tags, value).setValue(value, additionalTags);
        }
      }
    }

    private class CompositeFleakStopWatch extends FleakStopWatch {
      private final String name;
      private final Map<String, String> tags;
      private long startTime;

      CompositeFleakStopWatch(String name, Map<String, String> tags) {
        this.name = name;
        this.tags = tags;
      }

      @Override
      public void start() {
        this.startTime = System.currentTimeMillis();
        for (MetricClientProvider provider : providers) {
          provider.stopWatch(name, tags).start();
        }
      }

      @Override
      public void stop(Map<String, String> additionalTags) {
        long duration = System.currentTimeMillis() - startTime;
        reportDuration(duration, additionalTags);
      }

      @Override
      protected void reportDuration(long duration, Map<String, String> additionalTags) {
        for (MetricClientProvider provider : providers) {
          provider.stopWatch(name, tags).stop(additionalTags);
        }
      }
    }
  }

  /** Created by bolei on 2/2/24 */
  class NoopMetricClientProvider implements MetricClientProvider {

    @Override
    public FleakCounter counter(String name, Map<String, String> tags) {
      return new NoopFleakCounter();
    }

    @Override
    public <T> FleakGauge<T> gauge(String name, Map<String, String> tags, T monitoredValue) {
      return new NoopFleakGauge<>();
    }

    @Override
    public FleakStopWatch stopWatch(String name, Map<String, String> tags) {
      return new NoopStopWatch();
    }

    @Override
    public void close() {
      // no-op
    }

    public static class NoopFleakCounter implements FleakCounter {
      @Override
      public void increase(Map<String, String> additionalTags) {
        // no-op
      }

      @Override
      public void increase(long n, Map<String, String> additionalTags) {
        // no-op
      }
    }

    public static class NoopFleakGauge<T> implements FleakGauge<T> {

      @Override
      public void setValue(T value, Map<String, String> additionalTags) {
        // no-op
      }
    }

    public static class NoopStopWatch extends FleakStopWatch {

      @Override
      protected void reportDuration(long duration, Map<String, String> additionalTags) {
        // no-op
      }
    }
  }
}
