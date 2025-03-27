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

import static org.mockito.Mockito.*;

import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.metric.FleakGauge;
import io.fleak.zephflow.api.metric.FleakStopWatch;
import io.fleak.zephflow.api.metric.MetricClientProvider;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class CompositeMetricClientProviderTest {

  private MetricClientProvider provider1;
  private MetricClientProvider provider2;
  private MetricClientProvider.CompositeMetricClientProvider compositeProvider;
  private Map<String, String> tags;

  @BeforeEach
  void setUp() {
    provider1 = Mockito.mock(MetricClientProvider.class);
    provider2 = Mockito.mock(MetricClientProvider.class);
    compositeProvider =
        new MetricClientProvider.CompositeMetricClientProvider(Arrays.asList(provider1, provider2));
    tags = new HashMap<>();
    tags.put("tag1", "value1");
  }

  @Test
  void testCounter() {
    FleakCounter counter1 = Mockito.mock(FleakCounter.class);
    FleakCounter counter2 = Mockito.mock(FleakCounter.class);

    when(provider1.counter("testCounter", tags)).thenReturn(counter1);
    when(provider2.counter("testCounter", tags)).thenReturn(counter2);

    FleakCounter compositeCounter = compositeProvider.counter("testCounter", tags);

    Map<String, String> additionalTags = new HashMap<>();
    additionalTags.put("additionalTag", "additionalValue");

    compositeCounter.increase(additionalTags);
    verify(counter1).increase(additionalTags);
    verify(counter2).increase(additionalTags);

    compositeCounter.increase(5L, additionalTags);
    verify(counter1).increase(5L, additionalTags);
    verify(counter2).increase(5L, additionalTags);
  }

  @SuppressWarnings("unchecked")
  @Test
  void testGauge() {
    FleakGauge<Integer> gauge1 = Mockito.mock(FleakGauge.class);
    FleakGauge<Integer> gauge2 = Mockito.mock(FleakGauge.class);

    when(provider1.gauge("testGauge", tags, 10)).thenReturn(gauge1);
    when(provider2.gauge("testGauge", tags, 10)).thenReturn(gauge2);

    FleakGauge<Integer> compositeGauge = compositeProvider.gauge("testGauge", tags, 0);

    Map<String, String> additionalTags = new HashMap<>();
    additionalTags.put("additionalTag", "additionalValue");

    compositeGauge.setValue(10, additionalTags);
    verify(gauge1).setValue(10, additionalTags);
    verify(gauge2).setValue(10, additionalTags);
  }

  @Test
  void testStopWatch() {
    FleakStopWatch stopWatch1 = Mockito.mock(FleakStopWatch.class);
    FleakStopWatch stopWatch2 = Mockito.mock(FleakStopWatch.class);

    when(provider1.stopWatch("testStopWatch", tags)).thenReturn(stopWatch1);
    when(provider2.stopWatch("testStopWatch", tags)).thenReturn(stopWatch2);

    FleakStopWatch compositeStopWatch = compositeProvider.stopWatch("testStopWatch", tags);

    Map<String, String> additionalTags = new HashMap<>();
    additionalTags.put("additionalTag", "additionalValue");

    compositeStopWatch.stop(additionalTags);
    verify(stopWatch1).stop(additionalTags);
    verify(stopWatch2).stop(additionalTags);
  }
}
